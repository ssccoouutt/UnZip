import os
import logging
import re
import tempfile
import zipfile
import shutil
import requests
import asyncio
import aiohttp
import signal  # Added this import
from datetime import datetime
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters
)
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from aiohttp import web

# Hardcoded configuration
SCOPES = ['https://www.googleapis.com/auth/drive']
TELEGRAM_BOT_TOKEN = '7829579947:AAFxoAbbfTako-XC1fmzPJQJSaQsS852V2g'
CLIENT_SECRET_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'
WEB_PORT = 8000
PING_INTERVAL = 25
HEALTH_CHECK_ENDPOINT = "/health"

# Initialize logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Global variables for web server
runner = None
site = None

async def health_check(request):
    """Health check endpoint for Koyeb"""
    return web.Response(
        text=f"🤖 Bot is operational | Last active: {datetime.now()}",
        headers={"Content-Type": "text/plain"},
        status=200
    )

async def root_handler(request):
    """Root endpoint handler for Koyeb health checks"""
    return web.Response(
        text="Bot is running",
        status=200
    )

async def run_webserver():
    """Run the web server for health checks"""
    app = web.Application()
    app.router.add_get(HEALTH_CHECK_ENDPOINT, health_check)
    app.router.add_get("/", root_handler)
    
    global runner, site
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, '0.0.0.0', WEB_PORT)
    await site.start()
    logger.info(f"Health check server running on port {WEB_PORT}")

async def self_ping():
    """Keep-alive mechanism for Koyeb"""
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f'http://localhost:{WEB_PORT}{HEALTH_CHECK_ENDPOINT}') as resp:
                    status = f"Status: {resp.status}" if resp.status != 200 else "Success"
                    logger.info(f"Keepalive ping {status}")
                    
            with open('/tmp/last_active.txt', 'w') as f:
                f.write(str(datetime.now()))
                
        except Exception as e:
            logger.error(f"Keepalive error: {str(e)}")
        
        await asyncio.sleep(PING_INTERVAL)

def authorize_google_drive():
    """Authorize Google Drive API using OAuth2."""
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            raise Exception("Google Drive authorization required.")
    return creds

async def create_drive_folder(folder_name):
    """Create a folder in Google Drive."""
    creds = authorize_google_drive()
    service = build('drive', 'v3', credentials=creds)
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    folder = service.files().create(body=file_metadata, fields='id').execute()
    return folder.get('id')

async def upload_to_google_drive(file_path, file_name, parent_folder_id=None):
    """Upload a file to Google Drive."""
    creds = authorize_google_drive()
    service = build('drive', 'v3', credentials=creds)
    file_metadata = {'name': file_name}
    if parent_folder_id:
        file_metadata['parents'] = [parent_folder_id]
    media = MediaFileUpload(file_path, resumable=True)
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return file.get('id')

async def download_file_from_link(url, destination):
    """Download a file from a direct download link or Google Drive link."""
    try:
        if "drive.google.com" in url:
            file_id = re.search(r"/file/d/([a-zA-Z0-9_-]+)", url)
            if not file_id:
                return False, "❌ Invalid Google Drive link."
            file_id = file_id.group(1)
            download_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
            creds = authorize_google_drive()
            headers = {"Authorization": f"Bearer {creds.token}"}
            response = requests.get(download_url, headers=headers, stream=True)
        else:
            response = requests.get(url, stream=True)

        response.raise_for_status()

        content_type = response.headers.get('Content-Type', '').lower()
        supported_types = ['zip', 'rar', 'x-7z-compressed', 'octet-stream']
        if not any(arch_type in content_type for arch_type in supported_types) and not any(url.lower().endswith(ext) for ext in ['.zip', '.rar', '.7z']):
            return False, "❌ The provided link does not point to a supported archive (zip/rar/7z)."

        with open(destination, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        return True, None
    except Exception as e:
        logger.error(f"Failed to download file from {url}: {e}")
        return False, f"❌ Failed to download the file: {e}"

async def extract_archive(archive_path, extract_dir):
    """Extract supported archive formats (zip/rar/7z)."""
    try:
        if zipfile.is_zipfile(archive_path):
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            return True, None
        else:
            return False, "❌ RAR and 7z extraction support coming soon. Currently only ZIP files are supported."
    except Exception as e:
        logger.error(f"Failed to extract archive: {e}")
        return False, f"❌ Failed to extract archive: {e}"

async def unzip_and_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process archive and upload to Google Drive."""
    try:
        creds = authorize_google_drive()
    except Exception as e:
        await start_authorization(update, context)
        return

    try:
        url = update.message.text.strip()
        file_name = os.path.basename(url.split('?')[0]) or "downloaded_archive"
        file_path = os.path.join(tempfile.gettempdir(), file_name)
        await update.message.reply_text("⬇️ Downloading file...")

        success, error_message = await download_file_from_link(url, file_path)
        if not success:
            await update.message.reply_text(error_message)
            return

        folder_name = os.path.splitext(file_name)[0]
        await update.message.reply_text(f"📁 Creating folder '{folder_name}' in Google Drive...")
        folder_id = await create_drive_folder(folder_name)

        extract_dir = os.path.join(tempfile.gettempdir(), 'extracted_files')
        os.makedirs(extract_dir, exist_ok=True)

        await update.message.reply_text("📦 Extracting files...")
        success, error_message = await extract_archive(file_path, extract_dir)
        if not success:
            await update.message.reply_text(error_message)
            return

        uploaded_files = []
        for root, _, files in os.walk(extract_dir):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                relative_path = os.path.relpath(file_path, extract_dir)
                await update.message.reply_text(f"⬆️ Uploading {relative_path} to Google Drive...")
                drive_file_id = await upload_to_google_drive(file_path, file_name, folder_id)
                uploaded_files.append((relative_path, drive_file_id))

        if uploaded_files:
            message = f"✅ Uploaded {len(uploaded_files)} files to Google Drive folder '{folder_name}':\n"
            for file_name, drive_file_id in uploaded_files[:10]:
                message += f"- {file_name}\n"
            if len(uploaded_files) > 10:
                message += f"... and {len(uploaded_files) - 10} more files\n"
            message += f"\n📁 Folder ID: {folder_id}"
            await update.message.reply_text(message)
        else:
            await update.message.reply_text("❌ No files found in the archive.")

        os.remove(file_path)
        shutil.rmtree(extract_dir, ignore_errors=True)

    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")
        logger.error(f"Unzip and upload error: {e}")
        if 'file_path' in locals() and os.path.exists(file_path):
            os.remove(file_path)
        if 'extract_dir' in locals() and os.path.exists(extract_dir):
            shutil.rmtree(extract_dir, ignore_errors=True)

async def start_authorization(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the OAuth2 authorization flow."""
    try:
        flow = Flow.from_client_secrets_file(
            CLIENT_SECRET_FILE,
            scopes=SCOPES,
            redirect_uri='urn:ietf:wg:oauth:2.0:oob'
        )
        auth_url, _ = flow.authorization_url(prompt='consent')
        context.user_data['flow'] = flow
        await update.message.reply_text(
            f"🔑 Authorization required!\n\n"
            f"Please visit this link to authorize:\n{auth_url}\n\n"
            "After authorization, send the code you receive back here."
        )
    except FileNotFoundError:
        await update.message.reply_text("❌ credentials.json file is missing. Please ensure it's in the root directory.")
        logger.error("credentials.json file is missing.")
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to start authorization. Error: {str(e)}")
        logger.error(f"Authorization error: {e}")

async def handle_authorization_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the authorization code from the user."""
    code = update.message.text.strip()
    flow = context.user_data.get('flow')
    if not flow:
        await update.message.reply_text("❌ No active authorization session. Please send a link first.")
        return

    try:
        flow.fetch_token(code=code)
        with open(TOKEN_FILE, 'w') as token_file:
            token_file.write(flow.credentials.to_json())
        await update.message.reply_text("✅ Authorization successful! You can now send links.")
    except Exception as e:
        await update.message.reply_text(f"❌ Authorization failed. Please try again. Error: {str(e)}")
        logger.error(f"Authorization error: {e}")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all incoming messages."""
    message_text = update.message.text.strip()

    if re.match(r'^[A-Za-z0-9_\-]+/[A-Za-z0-9_\-]+$', message_text):
        await handle_authorization_code(update, context)
    elif message_text.startswith(("http://", "https://")):
        await unzip_and_upload(update, context)
    else:
        await update.message.reply_text("⚠️ Please send a direct download link or an authorization code.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command."""
    await update.message.reply_text(
        "Send me a direct download link or Google Drive link to a .zip/.rar/.7z file, "
        "and I'll extract it and upload its contents to a new folder in your Google Drive!"
    )

async def run_bot():
    """Run the Telegram bot with web server."""
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    while True:
        await asyncio.sleep(3600)

async def shutdown(signal, loop):
    """Cleanup tasks tied to the service's shutdown."""
    logger.info(f"Received exit signal {signal.name}...")
    
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    
    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

async def main():
    """Main entry point with web server and bot"""
    webserver_task = asyncio.create_task(run_webserver())
    ping_task = asyncio.create_task(self_ping())
    bot_task = asyncio.create_task(run_bot())
    
    try:
        await asyncio.gather(webserver_task, ping_task, bot_task)
    except asyncio.CancelledError:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
    finally:
        logger.info("Starting cleanup process...")
        
        ping_task.cancel()
        try:
            await ping_task
        except asyncio.CancelledError:
            pass
            
        global runner, site
        if site:
            await site.stop()
        if runner:
            await runner.cleanup()
            
        application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        await application.stop()
        await application.shutdown()
        
        logger.info("Cleanup completed")

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(s, loop))
        )
    
    try:
        logger.info("Starting service...")
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception as e:
        logger.error(f"Critical failure: {str(e)}")
    finally:
        loop.close()
        logger.info("Event loop closed")
