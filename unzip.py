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
from datetime import datetime, timedelta
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
PING_INTERVAL = int(os.getenv('PING_INTERVAL', '30'))
HEALTH_CHECK_ENDPOINT = "/health"
MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024  # 5GB
HEALTH_CHECK_TIMEOUT = 10  # seconds
MAX_UPLOAD_TIME = 1800  # 30 minutes max for upload operations
MAX_CONCURRENT_UPLOADS = 2  # Limit concurrent uploads

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
        self.active_operations = 0
        self.session = None
        self.loop = asyncio.get_event_loop()
        self.upload_semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)
        self.health_check_lock = asyncio.Lock()
        self._shutdown_lock = asyncio.Lock()

    async def root_handler(self, request):
        """Handle requests to the root endpoint"""
        return web.Response(
            text="Google Drive Uploader Bot\n\n"
                 f"Uptime: {timedelta(seconds=time.time() - self.start_time)}\n"
                 f"Last activity: {datetime.fromtimestamp(self.last_activity)}\n"
                 f"Active operations: {self.active_operations}\n\n"
                 f"Health check: http://{request.host}{HEALTH_CHECK_ENDPOINT}",
            headers={"Content-Type": "text/plain"}
        )

    async def health_check(self, request):
        """Health check endpoint"""
        async with self.health_check_lock:
            try:
                if self.shutting_down:
                    return web.json_response(
                        {"status": "shutting_down"},
                        status=503
                    )

                uptime = str(timedelta(seconds=time.time() - self.start_time))
                status = {
                    "status": "healthy",
                    "uptime": uptime,
                    "last_activity": datetime.fromtimestamp(self.last_activity).isoformat(),
                    "active_operations": self.active_operations,
                }
                
                return web.json_response(status)
            except Exception as e:
                logger.error(f"Health check error: {e}")
                return web.json_response(
                    {"status": "error", "error": str(e)},
                    status=500
                )

    async def init_session(self):
        """Initialize aiohttp session"""
        timeout = aiohttp.ClientTimeout(total=HEALTH_CHECK_TIMEOUT)
        connector = aiohttp.TCPConnector(force_close=True)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector
        )

    async def run_webserver(self):
        """Run the web server for health checks"""
        try:
            app = web.Application()
            app.router.add_get(HEALTH_CHECK_ENDPOINT, self.health_check)
            app.router.add_get("/", self.root_handler)
            
            self.runner = web.AppRunner(app)
            await self.runner.setup()
            
            # Try to find an available port
            port = WEB_PORT
            max_attempts = 5
            for attempt in range(max_attempts):
                try:
                    self.site = web.TCPSite(self.runner, '0.0.0.0', port)
                    await self.site.start()
                    logger.info(f"Health check server running on port {port}")
                    return True
                except OSError as e:
                    if "Address already in use" in str(e) and attempt < max_attempts - 1:
                        port += 1
                        logger.warning(f"Port {port-1} in use, trying {port}")
                        continue
                    raise
            
            return False
        except Exception as e:
            logger.error(f"Failed to start web server: {e}")
            return False

    async def self_ping(self):
        """Keep-alive mechanism"""
        while not self.shutting_down:
            try:
                async with self.session.get(
                    f'http://localhost:{WEB_PORT}{HEALTH_CHECK_ENDPOINT}',
                    timeout=HEALTH_CHECK_TIMEOUT
                ) as resp:
                    if resp.status != 200:
                        raise Exception(f"Health check failed with status {resp.status}")
                    data = await resp.json()
                    if data.get('status') != 'healthy':
                        raise Exception(f"Degraded status: {data.get('status')}")
                
                with open('/tmp/last_active.txt', 'w') as f:
                    f.write(str(datetime.now()))
                    
                await asyncio.sleep(PING_INTERVAL)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Keepalive error: {e}")
                if not self.shutting_down:
                    await self.shutdown()
                break

    def authorize_google_drive(self):
        """Authorize Google Drive"""
        try:
            if not os.path.exists(TOKEN_FILE):
                raise FileNotFoundError(f"Token file {TOKEN_FILE} not found")
            
            with open(TOKEN_FILE, 'r') as token_file:
                token_data = json.load(token_file)
                
            creds = Credentials.from_authorized_user_info(token_data, SCOPES)
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            return creds
        except Exception as e:
            logger.error(f"Failed to authorize Google Drive: {e}")
            raise

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
        self.active_operations += 1
        self.last_activity = time.time()
        
        try:
            async with self.upload_semaphore:
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
            self.active_operations -= 1
            if 'file_path' in locals() and os.path.exists(file_path):
                os.remove(file_path)
            if 'temp_extract_dir' in locals() and os.path.exists(temp_extract_dir):
                shutil.rmtree(temp_extract_dir, ignore_errors=True)

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Send welcome message"""
        self.last_activity = time.time()
        await update.message.reply_text(
            "Welcome to Google Drive Uploader Bot!\n\n"
            "Send me a direct download link or Google Drive link to a ZIP file\n"
            "I'll extract and upload its contents to Google Drive\n\n"
            "⚠️ Max file size: 5GB\n"
            "❌ Only ZIP files are supported"
        )

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Send help message"""
        self.last_activity = time.time()
        await update.message.reply_text(
            "📚 How to use this bot:\n\n"
            "1. Send me a direct download link or Google Drive link to a ZIP file\n"
            "2. I'll download, extract and upload the contents to Google Drive\n"
            "3. Files will be organized in a folder with the same name as the ZIP file\n\n"
            "⚠️ Max file size: 5GB\n"
            "❌ Only ZIP files are supported"
        )

    async def shutdown(self):
        """Graceful shutdown"""
        async with self._shutdown_lock:
            if self.shutting_down:
                return
                
            self.shutting_down = True
            logger.info("Starting graceful shutdown...")
            
            # Cancel keepalive task first
            if self.keepalive_task:
                self.keepalive_task.cancel()
                try:
                    await self.keepalive_task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"Error in keepalive task during shutdown: {e}")

            # Close aiohttp session
            if self.session and not self.session.closed:
                await self.session.close()

            # Shutdown Telegram bot
            if self.application:
                try:
                    await self.application.stop()
                    await self.application.shutdown()
                    if hasattr(self.application, 'updater') and self.application.updater:
                        await self.application.updater.stop()
                except Exception as e:
                    logger.error(f"Error during bot shutdown: {e}")

            # Shutdown web server
            if self.site:
                await self.site.stop()
            if self.runner:
                await self.runner.cleanup()
                
            # Wait for active operations to complete
            if self.active_operations > 0:
                logger.info(f"Waiting for {self.active_operations} active operations to complete...")
                start_wait = time.time()
                while self.active_operations > 0 and time.time() - start_wait < 30:
                    await asyncio.sleep(1)
                    
            logger.info("Shutdown completed")

    async def run_bot(self):
        """Run the Telegram bot"""
        try:
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
            
            logger.info("Bot started successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to start bot: {e}")
            return False

    async def main(self):
        """Main application entry point"""
        try:
            await self.init_session()
            
            if not await self.run_webserver():
                raise RuntimeError("Failed to start web server")
                
            if not await self.run_bot():
                raise RuntimeError("Failed to start Telegram bot")
            
            # Start keepalive after everything else is running
            self.keepalive_task = asyncio.create_task(self.self_ping())
            
            # Main loop
            while not self.shutting_down:
                await asyncio.sleep(1)
                
        except asyncio.CancelledError:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
        finally:
            await self.shutdown()

def handle_exception(loop, context):
    """Handle uncaught exceptions"""
    logger.error(f"Uncaught exception: {context}")
    if 'exception' in context:
        logger.error("Exception details:", exc_info=context['exception'])

if __name__ == '__main__':
    # Configure event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.set_exception_handler(handle_exception)
    
    # Create application
    bot_app = BotApplication()
    
    # Set up signal handlers
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(bot_app.shutdown())
        )
    
    try:
        # Run main application
        loop.run_until_complete(bot_app.main())
    except Exception as e:
        logger.error(f"Application crashed: {e}", exc_info=True)
    finally:
        # Cleanup
        if not loop.is_closed():
            loop.close()
        logger.info("Application shutdown complete")
