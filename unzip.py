import os
import logging
import re
import tempfile
import zipfile
import shutil
import requests
import asyncio
import aiohttp
import signal
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    ConversationHandler,
    CallbackQueryHandler
)
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from aiohttp import web

# Configuration
SCOPES = ['https://www.googleapis.com/auth/drive']
TELEGRAM_BOT_TOKEN = '7539483784:AAE4MlT-IGXEb7md3v6pyhbkxe9VtCXZSe0'
CLIENT_SECRET_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'
WEB_PORT = 8000
PING_INTERVAL = 25
HEALTH_CHECK_ENDPOINT = "/health"
REDIRECT_URI = 'http://localhost:8080'

# Conversation states
AUTH_URL, PROCESS_LINK = range(2)

# Initialize logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Global variables
runner = None
site = None
app = None

async def health_check(request):
    return web.Response(text=f"Bot operational | Last active: {datetime.now()}", status=200)

async def run_webserver():
    global runner, site
    web_app = web.Application()
    web_app.router.add_get(HEALTH_CHECK_ENDPOINT, health_check)
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', WEB_PORT)
    await site.start()
    logger.info(f"Health check server running on port {WEB_PORT}")

async def self_ping():
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f'http://localhost:{WEB_PORT}{HEALTH_CHECK_ENDPOINT}') as resp:
                    logger.info(f"Keepalive ping - Status: {resp.status}")
        except Exception as e:
            logger.error(f"Keepalive error: {str(e)}")
        await asyncio.sleep(PING_INTERVAL)

def authorize_google_drive():
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
    creds = authorize_google_drive()
    service = build('drive', 'v3', credentials=creds)
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    folder = service.files().create(body=file_metadata, fields='id').execute()
    return folder.get('id')

async def upload_to_google_drive(file_path, file_name, parent_folder_id=None):
    creds = authorize_google_drive()
    service = build('drive', 'v3', credentials=creds)
    file_metadata = {'name': file_name}
    if parent_folder_id:
        file_metadata['parents'] = [parent_folder_id]
    media = MediaFileUpload(file_path, resumable=True)
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return file.get('id')

async def download_file_from_link(url, destination):
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
        with open(destination, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        return True, None
    except Exception as e:
        logger.error(f"Download error: {e}")
        return False, f"❌ Download failed: {e}"

async def extract_archive(archive_path, extract_dir):
    try:
        if zipfile.is_zipfile(archive_path):
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            return True, None
        return False, "❌ Only ZIP files currently supported."
    except Exception as e:
        logger.error(f"Extraction error: {e}")
        return False, f"❌ Extraction failed: {e}"

async def start_authorization(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        flow = Flow.from_client_secrets_file(
            CLIENT_SECRET_FILE,
            scopes=SCOPES,
            redirect_uri=REDIRECT_URI
        )
        auth_url, _ = flow.authorization_url(prompt='consent')
        context.user_data['flow'] = flow
        
        keyboard = [
            [InlineKeyboardButton("❌ Cancel", callback_data='cancel_auth')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "🔑 *Authorization Required*\n\n"
            "1. Click this link to authorize:\n"
            f"[Authorize Google Drive]({auth_url})\n\n"
            "2. After approving, you'll get a localhost URL\n"
            "3. Copy and send that URL back to me\n\n"
            "⚠️ Ignore browser errors, just copy the URL",
            parse_mode='Markdown',
            disable_web_page_preview=True,
            reply_markup=reply_markup
        )
        return AUTH_URL

    except Exception as e:
        await update.message.reply_text(f"❌ Authorization error: {str(e)}")
        logger.error(f"Authorization error: {e}")
        return ConversationHandler.END

async def handle_auth_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = update.message.text.strip()
    flow = context.user_data.get('flow')
    
    if not flow:
        await update.message.reply_text("❌ No active authorization session.")
        return ConversationHandler.END

    try:
        parsed = urlparse(url)
        if not (parsed.netloc == 'localhost:8080' and parsed.scheme == 'http'):
            await update.message.reply_text("❌ Invalid URL. Send the exact redirect URL.")
            return AUTH_URL
            
        query = parse_qs(parsed.query)
        code = query.get('code', [None])[0]
        
        if not code:
            await update.message.reply_text("❌ No authorization code found.")
            return AUTH_URL

        flow.fetch_token(code=code)
        with open(TOKEN_FILE, 'w') as token_file:
            token_file.write(flow.credentials.to_json())
        
        del context.user_data['flow']
        await update.message.reply_text("✅ *Authorization Successful!*", parse_mode='Markdown')
        return ConversationHandler.END

    except Exception as e:
        await update.message.reply_text(f"❌ Authorization failed: {str(e)}")
        logger.error(f"Token exchange error: {e}")
        return AUTH_URL

async def cancel_auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if 'flow' in context.user_data:
        del context.user_data['flow']
    
    await query.edit_message_text("❌ Authorization cancelled.")
    return ConversationHandler.END

async def process_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        url = update.message.text.strip()
        file_name = os.path.basename(url.split('?')[0]) or "archive"
        file_path = os.path.join(tempfile.gettempdir(), file_name)
        
        await update.message.reply_text("⬇️ Downloading file...")
        success, error = await download_file_from_link(url, file_path)
        if not success:
            return await update.message.reply_text(error)

        folder_name = os.path.splitext(file_name)[0]
        await update.message.reply_text(f"📁 Creating folder '{folder_name}'...")
        folder_id = await create_drive_folder(folder_name)

        extract_dir = os.path.join(tempfile.gettempdir(), 'extracted')
        os.makedirs(extract_dir, exist_ok=True)
        
        await update.message.reply_text("📦 Extracting files...")
        success, error = await extract_archive(file_path, extract_dir)
        if not success:
            return await update.message.reply_text(error)

        uploaded_files = []
        for root, _, files in os.walk(extract_dir):
            for file in files:
                file_path = os.path.join(root, file)
                await upload_to_google_drive(file_path, file, folder_id)
                uploaded_files.append(file)

        message = f"✅ Uploaded {len(uploaded_files)} files to '{folder_name}'"
        await update.message.reply_text(message)

    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")
        logger.error(f"Processing error: {e}")
    finally:
        if 'file_path' in locals() and os.path.exists(file_path):
            os.remove(file_path)
        if 'extract_dir' in locals() and os.path.exists(extract_dir):
            shutil.rmtree(extract_dir, ignore_errors=True)
    return ConversationHandler.END

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🔑 Authorize Google Drive", callback_data='start_auth')],
        [InlineKeyboardButton("🛠 Help", callback_data='help')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "Welcome to Google Drive Uploader Bot!\n\n"
        "1. First authorize Google Drive access\n"
        "2. Then send me file links to process",
        reply_markup=reply_markup
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    
    # Check if message is authorization redirect URL
    if text.startswith("http://localhost:8080") and 'code=' in text:
        if 'flow' in context.user_data:
            return await handle_auth_url(update, context)
        else:
            await update.message.reply_text("⚠️ Start authorization first using /auth")
            return
    
    # Process as regular link
    if text.startswith(("http://", "https://")):
        await process_link(update, context)
    else:
        await update.message.reply_text(
            "Please send either:\n"
            "- Google Drive/direct download link\n"
            "- Or authorization URL if you're in the middle of setup"
        )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📚 Help Guide:\n\n"
        "1. Click 'Authorize Google Drive' or use /auth\n"
        "2. Complete the Google authorization process\n"
        "3. Copy the localhost URL from your browser\n"
        "4. Send that URL back to the bot\n"
        "5. Now you can send download links for processing\n\n"
        "Cancel anytime with /cancel"
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == 'start_auth':
        await start_authorization(query, context)
    elif query.data == 'help':
        await help_command(query, context)
    elif query.data == 'cancel_auth':
        await cancel_auth(query, context)

async def shutdown(signal, loop):
    logger.info(f"Received exit signal {signal.name}...")
    
    global app
    if app:
        try:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    global runner, site
    if site:
        await site.stop()
    if runner:
        await runner.cleanup()
    
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    
    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

async def run_bot():
    global app
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('start', start),
            CommandHandler('auth', start_authorization),
            CallbackQueryHandler(button_handler)
        ],
        states={
            AUTH_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_auth_url)],
            PROCESS_LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_link)]
        },
        fallbacks=[
            CommandHandler('cancel', cancel_auth),
            CallbackQueryHandler(cancel_auth, pattern='^cancel_auth$')
        ],
        per_message=False
    )
    
    app.add_handler(conv_handler)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(button_handler))
    
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    
    logger.info("Bot started in polling mode")

async def main():
    webserver_task = asyncio.create_task(run_webserver())
    ping_task = asyncio.create_task(self_ping())
    bot_task = asyncio.create_task(run_bot())
    
    try:
        await asyncio.gather(webserver_task, ping_task, bot_task)
    except asyncio.CancelledError:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        logger.info("Cleanup completed")

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    for sig in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(sig, loop)))
    
    try:
        loop.run_until_complete(main())
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        loop.close()
