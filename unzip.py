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
import warnings
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
from googleapiclient.discovery_cache.base import Cache
from aiohttp import web

# Suppress Google API cache warning
warnings.filterwarnings("ignore", "file_cache is only supported")

# Custom memory cache for Google API
class MemoryCache(Cache):
    _CACHE = {}

    def get(self, url):
        return MemoryCache._CACHE.get(url)

    def set(self, url, content):
        MemoryCache._CACHE[url] = content

# Configuration
SCOPES = ['https://www.googleapis.com/auth/drive']
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '7539483784:AAE4MlT-IGXEb7md3v6pyhbkxe9VtCXZSe0')
TOKEN_FILE = 'token.json'
WEB_PORT = int(os.getenv('WEB_PORT', '8000'))
PING_INTERVAL = int(os.getenv('PING_INTERVAL', '25'))
HEALTH_CHECK_ENDPOINT = "/health"
MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024  # 5GB
MAX_STARTUP_ATTEMPTS = 3

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
        self.startup_attempts = 0

    def load_token(self):
        """Load token from token.json with validation"""
        try:
            if not os.path.exists(TOKEN_FILE):
                raise FileNotFoundError(f"Token file {TOKEN_FILE} not found")
            
            with open(TOKEN_FILE, 'r') as token_file:
                token_data = json.load(token_file)
                
            if not all(key in token_data for key in ['token', 'refresh_token', 'client_id', 'client_secret']):
                raise ValueError("Invalid token file structure")
                
            return token_data
        except Exception as e:
            logger.error(f"Failed to load token: {e}")
            raise

    async def health_check(self, request):
        """Comprehensive health check endpoint"""
        try:
            status = {
                "status": "operational" if not self.shutting_down else "shutting_down",
                "last_active": str(datetime.now()),
                "bot_running": self.application is not None and self.application.running,
                "webserver_running": self.site is not None,
                "timestamp": time.time()
            }
            return web.json_response(status)
        except Exception as e:
            logger.error(f"Health check error: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def status_handler(self, request):
        """Detailed status endpoint"""
        status = {
            "status": "operational" if not self.shutting_down else "shutting_down",
            "version": "1.0.0",
            "services": {
                "bot_running": self.application is not None and self.application.running,
                "webserver_running": self.site is not None,
                "last_ping": str(datetime.now())
            }
        }
        return web.json_response(status)

    async def root_handler(self, request):
        """Root endpoint"""
        return web.Response(text="Google Drive Uploader Bot is running")

    async def run_webserver(self):
        """Run the web server for health checks and monitoring"""
        web_app = web.Application()
        web_app.router.add_get(HEALTH_CHECK_ENDPOINT, self.health_check)
        web_app.router.add_get("/status", self.status_handler)
        web_app.router.add_get("/", self.root_handler)
        
        self.runner = web.AppRunner(web_app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, '0.0.0.0', WEB_PORT)
        await self.site.start()
        logger.info(f"Web server running on port {WEB_PORT}")

    async def self_ping(self):
        """Keep-alive mechanism with error handling"""
        while not self.shutting_down:
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                    async with session.get(f'http://localhost:{WEB_PORT}{HEALTH_CHECK_ENDPOINT}') as resp:
                        if resp.status != 200:
                            logger.warning(f"Health check failed with status {resp.status}")
                with open('/tmp/last_active.txt', 'w') as f:
                    f.write(str(datetime.now()))
            except Exception as e:
                logger.error(f"Keepalive error: {str(e)}")
            await asyncio.sleep(PING_INTERVAL)

    def authorize_google_drive(self):
        """Authorize Google Drive with token.json"""
        token_data = self.load_token()
        creds = Credentials.from_authorized_user_info(token_data, SCOPES)
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        return creds

    async def download_file_from_link(self, url, destination):
        """Download a file from a URL with size check and retries"""
        max_retries = 3
        retry_delay = 5
        
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
                
                # Check file size
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
                    logger.error(f"Download failed after {max_retries} attempts: {e}")
                    return False, f"❌ Download failed: {e}"
                logger.warning(f"Download attempt {attempt + 1} failed, retrying...")
                await asyncio.sleep(retry_delay)

    async def extract_archive(self, archive_path, extract_dir):
        """Extract files from an archive with proper folder structure"""
        try:
            if not os.path.exists(archive_path):
                return False, "❌ Archive file not found"

            folder_name = os.path.splitext(os.path.basename(archive_path))[0]
            extract_dir = os.path.join(extract_dir, folder_name)
            os.makedirs(extract_dir, exist_ok=True)

            if zipfile.is_zipfile(archive_path):
                with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                    for member in zip_ref.infolist():
                        try:
                            zip_ref.extract(member, extract_dir)
                        except Exception as e:
                            logger.error(f"Failed to extract {member.filename}: {e}")
                            continue
                return True, None
            return False, "❌ Only ZIP files currently supported."
        except Exception as e:
            logger.error(f"Extraction error: {e}")
            return False, f"❌ Extraction failed: {e}"

    async def process_link(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process a download link"""
        try:
            url = update.message.text.strip()
            
            if "drive.google.com" in url:
                file_id = re.search(r"/file/d/([a-zA-Z0-9_-]+)", url)
                if not file_id:
                    return await update.message.reply_text("❌ Invalid Google Drive link.")
                
                creds = self.authorize_google_drive()
                service = build('drive', 'v3', credentials=creds, cache=MemoryCache())
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
            service = build('drive', 'v3', credentials=creds, cache=MemoryCache())
            
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
            "❌ Only ZIP files are supported\n\n"
            "Commands:\n"
            "/start - Show welcome message\n"
            "/help - Show this help message"
        )

    async def shutdown(self, signal=None):
        """Clean shutdown procedure"""
        if self.shutting_down:
            return
        self.shutting_down = True
        
        logger.info(f"Shutting down...")
        
        if self.keepalive_task:
            self.keepalive_task.cancel()
            try:
                await self.keepalive_task
            except asyncio.CancelledError:
                pass
        
        if self.application:
            try:
                if hasattr(self.application, 'updater') and self.application.updater.running:
                    await self.application.updater.stop()
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
        """Run the Telegram bot with error handling"""
        self.application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        
        self.application.add_handler(CommandHandler('start', self.start))
        self.application.add_handler(CommandHandler('help', self.help_command))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_link))
        
        await self.application.initialize()
        await self.application.start()
        
        # Use a single persistent polling instance
        if not hasattr(self.application, 'updater'):
            self.application.updater = self.application.bot
        await self.application.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
            timeout=60
        )
        
        logger.info("Bot started in polling mode")

    async def main(self):
        """Main application entry point with retry logic"""
        while self.startup_attempts < MAX_STARTUP_ATTEMPTS and not self.shutting_down:
            try:
                self.startup_attempts += 1
                logger.info(f"Starting application (attempt {self.startup_attempts}/{MAX_STARTUP_ATTEMPTS})")
                
                # Verify token exists
                self.load_token()
                
                await self.run_webserver()
                self.keepalive_task = asyncio.create_task(self.self_ping())
                await self.run_bot()
                
                # Reset startup attempts after successful start
                self.startup_attempts = 0
                
                # Keep the application running
                while True:
                    await asyncio.sleep(1)
                    
            except asyncio.CancelledError:
                logger.info("Shutting down gracefully...")
                break
            except Exception as e:
                logger.error(f"Startup attempt {self.startup_attempts} failed: {e}")
                if self.startup_attempts >= MAX_STARTUP_ATTEMPTS:
                    logger.error("Max startup attempts reached, giving up")
                    break
                await asyncio.sleep(5)  # Wait before retrying
            finally:
                await self.shutdown()

def handle_signal(bot_app, signal):
    """Handle shutdown signals"""
    asyncio.create_task(bot_app.shutdown(signal))

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    bot_app = BotApplication()
    
    # Register signal handlers
    for sig in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig,
            lambda sig=sig: handle_signal(bot_app, sig)
        )
    
    try:
        loop.run_until_complete(bot_app.main())
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        if not loop.is_closed():
            loop.close()
        logger.info("Application stopped")
