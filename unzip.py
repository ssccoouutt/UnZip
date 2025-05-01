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
            "❌ Only ZIP files are supported\n\n"
            "For large files, please be patient as uploads may take time"
        )

    async def process_link(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process download link with operation tracking"""
        self.active_operations += 1
        self.last_activity = time.time()
        
        try:
            async with self.upload_semaphore:
                url = update.message.text.strip()
                
                if not re.match(r'^https?://', url, re.IGNORECASE):
                    return await update.message.reply_text("❌ Invalid URL format. Please provide a valid HTTP/HTTPS link.")

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
            # [Previous implementation of _process_link_internal]
            # ... (keep your existing implementation here)
            pass
            
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
                
            # Wait for active operations to complete (with timeout)
            if self.active_operations > 0:
                logger.info(f"Waiting for {self.active_operations} active operations to complete...")
                start_wait = time.time()
                while self.active_operations > 0 and time.time() - start_wait < 30:
                    await asyncio.sleep(1)
                    
            logger.info("Shutdown completed")

    async def run_bot(self):
        """Run the Telegram bot with proper initialization"""
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
            poll_interval=1.0,
            allowed_updates=Update.ALL_TYPES
        )
        
        logger.info("Bot started successfully")

    async def main(self):
        """Main application entry point"""
        try:
            await self.init_session()
            
            if not await self.run_webserver():
                raise RuntimeError("Failed to start web server")
                
            await self.run_bot()
            
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
