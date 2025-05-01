import os
import json
import logging
import re
import tempfile
import zipfile
import shutil
import requests
import asyncio
import aiohttp
import signal
import time
from datetime import datetime
from urllib.parse import urlparse
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters
)
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from aiohttp import web

# Configuration
SCOPES = ['https://www.googleapis.com/auth/drive']
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '7539483784:AAE4MlT-IGXEb7md3v6pyhbkxe9VtCXZSe0')
TOKEN_FILE = 'token.json'
WEB_PORT = int(os.getenv('WEB_PORT', '8000'))
PING_INTERVAL = int(os.getenv('PING_INTERVAL', '25'))
HEALTH_CHECK_ENDPOINT = "/health"
MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024  # 5GB

# Initialize logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class BotApplication:
    def __init__(self):
        self.runner = None
        self.site = None
        self.application = None
        self.keepalive_task = None
        self.shutting_down = False
        self.start_time = time.time()
        self.last_activity = time.time()

    def load_token(self):
        """Load token from token.json"""
        try:
            if not os.path.exists(TOKEN_FILE):
                raise FileNotFoundError(f"Token file {TOKEN_FILE} not found")
            
            with open(TOKEN_FILE, 'r') as token_file:
                return json.load(token_file)
        except Exception as e:
            logger.error(f"Failed to load token: {e}")
            raise

    async def health_check(self, request):
        """Simplified health check that always succeeds"""
        self.last_activity = time.time()
        return web.Response(
            text=f"🤖 Bot is operational | Last active: {datetime.now()}",
            headers={"Content-Type": "text/plain"},
            status=200
        )

    async def root_handler(self, request):
        """Simple root endpoint"""
        return web.Response(text="Google Drive Uploader Bot is running")

    async def run_webserver(self):
        """Run the web server for health checks"""
        app = web.Application()
        app.router.add_get(HEALTH_CHECK_ENDPOINT, self.health_check)
        app.router.add_get("/", self.root_handler)
        
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, '0.0.0.0', WEB_PORT)
        await self.site.start()
        logger.info(f"Health check server running on port {WEB_PORT}")

    async def self_ping(self):
        """Keep-alive mechanism"""
        while not self.shutting_down:
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

    def authorize_google_drive(self):
        """Authorize Google Drive"""
        token_data = self.load_token()
        creds = Credentials.from_authorized_user_info(token_data, SCOPES)
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        return creds

    async def download_file_from_link(self, url, destination):
        """Download file with retries"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if "drive.google.com" in url:
                    file_id = re.search(r"/file/d/([a-zA-Z0-9_-]+)", url)
                    if not file_id:
                        return False, "❌ Invalid Google Drive link."
                    file_id = file_id.group(1)
                    download_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
                    creds = self.authorize_google_drive()
                    headers = {"Authorization": f"Bearer {creds.token}"}
                    response = requests.get(download_url, headers=headers, stream=True)
                else:
                    response = requests.get(url, stream=True)

                response.raise_for_status()
                
                file_size = int(response.headers.get('content-length', 0))
                if file_size > MAX_FILE_SIZE:
                    return False, f"❌ File too large (max 5GB allowed)"

                with open(destination, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            file.write(chunk)
                return True, None
                
            except Exception as e:
                if attempt == max_retries - 1:
                    return False, f"❌ Download failed: {e}"
                await asyncio.sleep(5)

    async def extract_archive(self, archive_path, extract_dir):
        """Extract files from archive"""
        try:
            if not os.path.exists(archive_path):
                return False, "❌ Archive file not found"

            folder_name = os.path.splitext(os.path.basename(archive_path))[0]
            extract_dir = os.path.join(extract_dir, folder_name)
            os.makedirs(extract_dir, exist_ok=True)

            if zipfile.is_zipfile(archive_path):
                with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                return True, None
            return False, "❌ Only ZIP files supported"
        except Exception as e:
            return False, f"❌ Extraction failed: {e}"

    async def process_link(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process download link"""
        try:
            url = update.message.text.strip()
            
            if "drive.google.com" in url:
                file_id = re.search(r"/file/d/([a-zA-Z0-9_-]+)", url)
                if not file_id:
                    return await update.message.reply_text("❌ Invalid Google Drive link.")
                
                creds = self.authorize_google_drive()
                service = build('drive', 'v3', credentials=creds)
                file_info = service.files().get(fileId=file_id.group(1), fields='name').execute()
                file_name = file_info['name']
            else:
                file_name = os.path.basename(urlparse(url).path.split('/')[-1] or "archive")
                file_name = file_name.split('?')[0]
                
            file_path = os.path.join(tempfile.gettempdir(), file_name)
            
            await update.message.reply_text("⬇️ Downloading file...")
            success, error = await self.download_file_from_link(url, file_path)
            if not success:
                return await update.message.reply_text(error)

            temp_extract_dir = tempfile.mkdtemp()
            
            await update.message.reply_text("📦 Extracting files...")
            success, error = await self.extract_archive(file_path, temp_extract_dir)
            if not success:
                shutil.rmtree(temp_extract_dir, ignore_errors=True)
                return await update.message.reply_text(error)

            folder_name = os.path.splitext(os.path.basename(file_path))[0]
            extract_dir = os.path.join(temp_extract_dir, folder_name)
            
            if not os.path.exists(extract_dir):
                shutil.rmtree(temp_extract_dir, ignore_errors=True)
                return await update.message.reply_text("❌ No files found in archive")

            await update.message.reply_text(f"📁 Creating folder '{folder_name}'...")
            creds = self.authorize_google_drive()
            service = build('drive', 'v3', credentials=creds)
            
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            folder = service.files().create(body=folder_metadata, fields='id').execute()
            folder_id = folder.get('id')
            
            uploaded_files = []
            for root, _, files in os.walk(extract_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(root, extract_dir)
                    current_parent = folder_id
                    
                    if relative_path != '.':
                        path_parts = relative_path.split(os.sep)
                        for part in path_parts:
                            query = f"name='{part}' and '{current_parent}' in parents and mimeType='application/vnd.google-apps.folder'"
                            results = service.files().list(q=query, fields="files(id)").execute()
                            items = results.get('files', [])
                            
                            if items:
                                current_parent = items[0]['id']
                            else:
                                file_metadata = {
                                    'name': part,
                                    'mimeType': 'application/vnd.google-apps.folder',
                                    'parents': [current_parent]
                                }
                                new_folder = service.files().create(body=file_metadata, fields='id').execute()
                                current_parent = new_folder.get('id')
                    
                    file_metadata = {
                        'name': file,
                        'parents': [current_parent]
                    }
                    media = MediaFileUpload(file_path, resumable=True)
                    service.files().create(
                        body=file_metadata,
                        media_body=media,
                        fields='id'
                    ).execute()
                    uploaded_files.append(file)

            message = f"✅ Uploaded {len(uploaded_files)} files to Google Drive\n📁 Folder: {folder_name}"
            await update.message.reply_text(message)

        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")
            logger.error(f"Processing error: {e}", exc_info=True)
        finally:
            if 'file_path' in locals() and os.path.exists(file_path):
                os.remove(file_path)
            if 'temp_extract_dir' in locals() and os.path.exists(temp_extract_dir):
                shutil.rmtree(temp_extract_dir, ignore_errors=True)

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Send welcome message"""
        await update.message.reply_text(
            "Welcome to Google Drive Uploader Bot!\n\n"
            "Send me a direct download link or Google Drive link to a ZIP file\n"
            "I'll extract and upload its contents to Google Drive\n\n"
            "⚠️ Max file size: 5GB\n"
            "❌ Only ZIP files are supported"
        )

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Send help message"""
        await update.message.reply_text(
            "📚 How to use this bot:\n\n"
            "1. Send me a direct download link or Google Drive link to a ZIP file\n"
            "2. I'll download, extract and upload the contents to Google Drive\n"
            "3. Files will be organized in a folder with the same name as the ZIP file\n\n"
            "⚠️ Max file size: 5GB\n"
            "❌ Only ZIP files are supported"
        )

    async def shutdown(self):
        """Clean shutdown"""
        if self.shutting_down:
            return
        self.shutting_down = True
        
        logger.info("Shutting down...")
        
        if self.keepalive_task:
            self.keepalive_task.cancel()
        
        if self.application:
            try:
                await self.application.stop()
                await self.application.shutdown()
            except Exception as e:
                logger.error(f"Error during bot shutdown: {e}")

        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
        
        logger.info("Cleanup completed")

    async def run_bot(self):
        """Run the Telegram bot"""
        self.application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        
        self.application.add_handler(CommandHandler('start', self.start))
        self.application.add_handler(CommandHandler('help', self.help_command))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_link))
        
        await self.application.initialize()
        await self.application.start()
        
        # Start polling with clean state
        await self.application.updater.start_polling(
            drop_pending_updates=True,
            timeout=30,
            poll_interval=1.0
        )
        
        logger.info("Bot started")

    async def main(self):
        """Main application entry point"""
        try:
            self.load_token()
            
            await self.run_webserver()
            self.keepalive_task = asyncio.create_task(self.self_ping())
            await self.run_bot()
            
            while True:
                await asyncio.sleep(1)
                
        except asyncio.CancelledError:
            logger.info("Shutting down gracefully...")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
        finally:
            await self.shutdown()

async def shutdown(signal, loop):
    """Cleanup tasks tied to the service's shutdown."""
    logger.info(f"Received exit signal {signal.name}...")
    
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    
    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    bot_app = BotApplication()
    
    # Set up signal handlers
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(s, loop))
        )
    
    try:
        loop.run_until_complete(bot_app.main())
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        loop.close()
