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
MAX_CONCURRENT_UPLOADS = 2  # Limit concurrent uploads to prevent resource exhaustion

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

    async def init_session(self):
        """Initialize aiohttp session with increased timeouts"""
        timeout = aiohttp.ClientTimeout(
            total=HEALTH_CHECK_TIMEOUT,
            connect=5,
            sock_connect=5,
            sock_read=5
        )
        connector = aiohttp.TCPConnector(
            limit=10,
            force_close=True,
            enable_cleanup_closed=True
        )
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            trust_env=True
        )

    def load_token(self):
        """Load token from token.json with validation"""
        try:
            if not os.path.exists(TOKEN_FILE):
                raise FileNotFoundError(f"Token file {TOKEN_FILE} not found")
            
            with open(TOKEN_FILE, 'r') as token_file:
                token_data = json.load(token_file)
                
            # Validate token structure
            required_fields = ['token', 'refresh_token', 'token_uri', 'client_id', 'client_secret', 'scopes']
            if not all(field in token_data for field in required_fields):
                raise ValueError("Token file is missing required fields")
                
            return token_data
        except Exception as e:
            logger.error(f"Failed to load token: {e}")
            raise

    async def health_check(self, request):
        """Health check with operation awareness"""
        async with self.health_check_lock:
            try:
                # Skip health check if we're in the middle of an upload
                if self.active_operations > 0:
                    return web.json_response(
                        {"status": "degraded", "reason": "active_operations", "count": self.active_operations},
                        status=202
                    )

                # Basic system health check
                uptime = str(timedelta(seconds=time.time() - self.start_time))
                status = {
                    "status": "healthy",
                    "uptime": uptime,
                    "last_activity": datetime.fromtimestamp(self.last_activity).isoformat(),
                    "active_operations": self.active_operations,
                    "telegram_connected": self.application is not None and self.application.updater is not None,
                    "google_auth_ready": self.test_google_auth(),
                }
                
                return web.json_response(status)
            except Exception as e:
                logger.error(f"Health check error: {e}")
                return web.json_response(
                    {"status": "error", "error": str(e)},
                    status=500
                )

    def test_google_auth(self):
        """Test Google auth connectivity with timeout"""
        try:
            creds = self.authorize_google_drive()
            return creds.valid
        except:
            return False

    async def run_webserver(self):
        """Run the web server with increased limits"""
        try:
            app = web.Application(client_max_size=1024*1024*10)  # 10MB max request size
            app.router.add_get(HEALTH_CHECK_ENDPOINT, self.health_check)
            app.router.add_get("/", self.root_handler)
            
            self.runner = web.AppRunner(
                app,
                access_log=logger,
                handle_signals=True
            )
            await self.runner.setup()
            
            self.site = web.TCPSite(
                self.runner,
                '0.0.0.0',
                WEB_PORT,
                reuse_port=True,
                shutdown_timeout=5
            )
            await self.site.start()
            logger.info(f"Health check server running on port {WEB_PORT}")
            return True
        except Exception as e:
            logger.error(f"Failed to start web server: {e}")
            return False

    async def self_ping(self):
        """Improved keep-alive with circuit breaker"""
        retry_count = 0
        max_retries = 3
        base_delay = 5
        
        while not self.shutting_down:
            try:
                async with self.session.get(
                    f'http://localhost:{WEB_PORT}{HEALTH_CHECK_ENDPOINT}',
                    timeout=HEALTH_CHECK_TIMEOUT
                ) as resp:
                    if resp.status == 200:
                        retry_count = 0
                        data = await resp.json()
                        if data.get('status') != 'healthy':
                            raise Exception(f"Degraded status: {data.get('reason', 'unknown')}")
                    else:
                        raise Exception(f"Status {resp.status}")
                
                # Write last active time
                with open('/tmp/last_active.txt', 'w') as f:
                    f.write(str(datetime.now()))
                    
                await asyncio.sleep(PING_INTERVAL)
                
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"Keepalive failed after {max_retries} attempts. Last error: {e}")
                    await self.shutdown()
                    return
                
                delay = min(base_delay * (2 ** retry_count), 60)  # Cap at 1 minute
                logger.warning(f"Keepalive error (attempt {retry_count}/{max_retries}): {e}. Retrying in {delay}s")
                await asyncio.sleep(delay)

    async def download_file_from_link(self, url, destination):
        """Download file with retries and proper cleanup"""
        max_retries = 3
        temp_path = f"{destination}.download"
        
        try:
            for attempt in range(max_retries):
                try:
                    if "drive.google.com" in url:
                        file_id = re.search(r"/file/d/([a-zA-Z0-9_-]+)", url)
                        if not file_id:
                            return False, "❌ Invalid Google Drive link format."
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
                        return False, f"❌ File too large (max {MAX_FILE_SIZE/1024/1024/1024}GB allowed)"

                    with open(temp_path, 'wb') as file:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                file.write(chunk)
                                
                    # Only rename if download completed successfully
                    os.rename(temp_path, destination)
                    return True, None
                    
                except requests.exceptions.RequestException as e:
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(5 * (attempt + 1))  # Linear backoff
                    
        except Exception as e:
            logger.error(f"Download failed: {e}")
            # Clean up any partial download
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return False, f"❌ Download failed: {str(e)}"
            
        finally:
            if 'response' in locals():
                response.close()

    async def process_link(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process download link with operation tracking"""
        self.active_operations += 1
        self.last_activity = time.time()
        
        try:
            async with self.upload_semaphore:
                url = update.message.text.strip()
                
                # Validate URL format
                if not re.match(r'^https?://', url, re.IGNORECASE):
                    return await update.message.reply_text("❌ Invalid URL format. Please provide a valid HTTP/HTTPS link.")

                # Start processing with timeout
                await asyncio.wait_for(
                    self._process_link_internal(update, url),
                    timeout=MAX_UPLOAD_TIME
                )
                
        except asyncio.TimeoutError:
            await update.message.reply_text("❌ Operation timed out (30 minutes max)")
            logger.error(f"Timeout processing link: {url}")
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")
            logger.error(f"Error processing link: {e}", exc_info=True)
        finally:
            self.active_operations -= 1

    async def _process_link_internal(self, update: Update, url: str):
        """Internal link processing with proper resource cleanup"""
        temp_dir = None
        file_path = None
        
        try:
            # Get file info
            if "drive.google.com" in url:
                file_id = re.search(r"/file/d/([a-zA-Z0-9_-]+)", url)
                if not file_id:
                    return await update.message.reply_text("❌ Invalid Google Drive link format.")
                
                creds = self.authorize_google_drive()
                service = build('drive', 'v3', credentials=creds)
                file_info = service.files().get(fileId=file_id.group(1), fields='name,size').execute()
                file_name = file_info['name']
                file_size = int(file_info.get('size', 0))
            else:
                parsed = urlparse(url)
                file_name = os.path.basename(parsed.path.split('/')[-1] or "archive")
                file_name = file_name.split('?')[0]
                file_size = 0  # Will be checked during download
                
            # Check size if we know it already
            if file_size > MAX_FILE_SIZE:
                return await update.message.reply_text(f"❌ File too large (max {MAX_FILE_SIZE/1024/1024/1024}GB allowed)")
                
            file_path = os.path.join(tempfile.gettempdir(), file_name)
            
            # Download with progress updates
            await update.message.reply_text("⬇️ Downloading file...")
            success, error = await self.download_file_from_link(url, file_path)
            if not success:
                return await update.message.reply_text(error)

            # Create temp directory for extraction
            temp_dir = tempfile.mkdtemp()
            
            await update.message.reply_text("📦 Extracting files...")
            success, error = await self.extract_archive(file_path, temp_dir)
            if not success:
                return await update.message.reply_text(error)

            folder_name = os.path.splitext(os.path.basename(file_path))[0]
            extract_dir = os.path.join(temp_dir, folder_name)
            
            if not os.path.exists(extract_dir):
                return await update.message.reply_text("❌ No files found in archive")

            # Count files for progress reporting
            file_count = sum(len(files) for _, _, files in os.walk(extract_dir))
            if file_count == 0:
                return await update.message.reply_text("❌ No files found in archive")

            await update.message.reply_text(f"📁 Creating folder '{folder_name}' with {file_count} files...")
            
            # Initialize Google Drive
            creds = self.authorize_google_drive()
            service = build('drive', 'v3', credentials=creds)
            
            # Create root folder
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            folder = service.files().create(body=folder_metadata, fields='id').execute()
            folder_id = folder.get('id')
            
            # Upload files with progress
            uploaded_files = 0
            last_progress_update = 0
            
            for root, _, files in os.walk(extract_dir):
                relative_path = os.path.relpath(root, extract_dir)
                current_parent = folder_id
                
                # Create subfolders if needed
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
                
                # Upload files in current directory
                for file in files:
                    file_path = os.path.join(root, file)
                    
                    # Skip if file is too large
                    if os.path.getsize(file_path) > MAX_FILE_SIZE:
                        logger.warning(f"Skipping large file: {file}")
                        continue
                        
                    file_metadata = {
                        'name': file,
                        'parents': [current_parent]
                    }
                    media = MediaFileUpload(file_path, resumable=True)
                    
                    try:
                        service.files().create(
                            body=file_metadata,
                            media_body=media,
                            fields='id'
                        ).execute()
                        uploaded_files += 1
                        
                        # Update progress every 10 files or 10%
                        now = time.time()
                        if (uploaded_files % 10 == 0 or 
                            (now - last_progress_update > 30) or 
                            (uploaded_files / file_count * 100) % 10 == 0):
                            progress = f"⏳ Uploaded {uploaded_files}/{file_count} files..."
                            await update.message.reply_text(progress)
                            last_progress_update = now
                            
                    except Exception as e:
                        logger.error(f"Failed to upload {file}: {e}")
                        continue

            message = (f"✅ Upload completed!\n"
                      f"📁 Folder: {folder_name}\n"
                      f"📄 Files uploaded: {uploaded_files}/{file_count}")
            
            if uploaded_files < file_count:
                message += "\n⚠️ Some files were skipped due to errors or size limits"
                
            await update.message.reply_text(message)

        finally:
            # Cleanup resources
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    logger.warning(f"Failed to remove temp file: {e}")
                    
            if temp_dir and os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                except Exception as e:
                    logger.warning(f"Failed to remove temp dir: {e}")

    async def shutdown(self):
        """Graceful shutdown with proper cleanup"""
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
            
        # Wait for active operations to complete (with timeout)
        if self.active_operations > 0:
            logger.info(f"Waiting for {self.active_operations} active operations to complete...")
            start_wait = time.time()
            while self.active_operations > 0 and time.time() - start_wait < 30:
                await asyncio.sleep(1)
                
        logger.info("Shutdown completed")

    async def main(self):
        """Main application entry point with proper initialization"""
        try:
            # Initialize components in order
            await self.init_session()
            
            if not await self.run_webserver():
                raise RuntimeError("Failed to start web server")
                
            self.application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
            self.application.add_handler(CommandHandler('start', self.start))
            self.application.add_handler(CommandHandler('help', self.help_command))
            self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_link))
            
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling(
                drop_pending_updates=True,
                timeout=30,
                poll_interval=1.0,
                allowed_updates=Update.ALL_TYPES
            )
            
            logger.info("Bot started successfully")
            
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

if __name__ == '__main__':
    # Configure event loop with debug
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.set_debug(True)
    
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
