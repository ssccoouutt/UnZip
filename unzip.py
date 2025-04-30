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

class BotApplication:
    def __init__(self):
        self.runner = None
        self.site = None
        self.application = None  # Changed from 'app' to 'application' for clarity
        self.keepalive_task = None
        self.webserver_task = None
        self.bot_task = None
        self.loop = None
        self.shutting_down = False

    async def health_check(self, request):
        """Handle health check requests"""
        if self.shutting_down:
            return web.Response(text="Shutting down", status=503)
        return web.Response(
            text=f"🤖 Bot is operational | Last active: {datetime.now()}",
            headers={"Content-Type": "text/plain"},
            status=200
        )

    async def root_handler(self, request):
        """Handle root endpoint requests"""
        return web.Response(
            text="Bot is running",
            status=200
        )

    async def run_webserver(self):
        """Run the web server for health checks"""
        web_app = web.Application()
        web_app.router.add_get(HEALTH_CHECK_ENDPOINT, self.health_check)
        web_app.router.add_get("/", self.root_handler)  # Added root handler
        
        self.runner = web.AppRunner(web_app)
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
        """Authorize Google Drive access"""
        creds = None
        if os.path.exists(TOKEN_FILE):
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                raise Exception("Google Drive authorization required.")
        return creds

    async def create_drive_folder(self, folder_name):
        """Create a folder in Google Drive"""
        creds = self.authorize_google_drive()
        service = build('drive', 'v3', credentials=creds)
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        folder = service.files().create(body=file_metadata, fields='id').execute()
        return folder.get('id')

    async def upload_to_google_drive(self, file_path, file_name, parent_folder_id=None):
        """Upload a file to Google Drive"""
        creds = self.authorize_google_drive()
        service = build('drive', 'v3', credentials=creds)
        file_metadata = {'name': file_name}
        if parent_folder_id:
            file_metadata['parents'] = [parent_folder_id]
        media = MediaFileUpload(file_path, resumable=True)
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file.get('id')

    async def download_file_from_link(self, url, destination):
        """Download a file from a URL"""
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
            with open(destination, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            return True, None
        except Exception as e:
            logger.error(f"Download error: {e}")
            return False, f"❌ Download failed: {e}"

    async def extract_archive(self, archive_path, extract_dir):
        """Extract files from an archive"""
        try:
            if zipfile.is_zipfile(archive_path):
                with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                return True, None
            return False, "❌ Only ZIP files currently supported."
        except Exception as e:
            logger.error(f"Extraction error: {e}")
            return False, f"❌ Extraction failed: {e}"

    async def start_authorization(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start Google Drive authorization process"""
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
            
            target = update.callback_query.message if update.callback_query else update.message
            
            await target.reply_text(
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
            error_msg = f"❌ Authorization error: {str(e)}"
            if update.callback_query:
                await update.callback_query.message.reply_text(error_msg)
            else:
                await update.message.reply_text(error_msg)
            logger.error(f"Authorization error: {e}")
            return ConversationHandler.END

    async def handle_auth_url(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle authorization callback URL"""
        url = update.message.text.strip()
        flow = context.user_data.get('flow')
        
        if not flow:
            await update.message.reply_text("❌ No active authorization session. Please start authorization first using /auth")
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
            await update.message.reply_text("✅ *Authorization Successful!* You can now send me download links.", parse_mode='Markdown')
            return ConversationHandler.END

        except Exception as e:
            await update.message.reply_text(f"❌ Authorization failed: {str(e)}")
            logger.error(f"Token exchange error: {e}")
            return AUTH_URL

    async def cancel_auth(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel authorization process"""
        query = update.callback_query
        await query.answer()
        
        if 'flow' in context.user_data:
            del context.user_data['flow']
        
        await query.edit_message_text("❌ Authorization cancelled.")
        return ConversationHandler.END

    async def process_link(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process a download link"""
        try:
            if not os.path.exists(TOKEN_FILE):
                await update.message.reply_text("❌ Google Drive authorization required. Please use /auth first.")
                return ConversationHandler.END

            url = update.message.text.strip()
            file_name = os.path.basename(url.split('?')[0]) or "archive"
            file_path = os.path.join(tempfile.gettempdir(), file_name)
            
            await update.message.reply_text("⬇️ Downloading file...")
            success, error = await self.download_file_from_link(url, file_path)
            if not success:
                return await update.message.reply_text(error)

            folder_name = os.path.splitext(file_name)[0]
            await update.message.reply_text(f"📁 Creating folder '{folder_name}'...")
            folder_id = await self.create_drive_folder(folder_name)

            extract_dir = os.path.join(tempfile.gettempdir(), 'extracted')
            os.makedirs(extract_dir, exist_ok=True)
            
            await update.message.reply_text("📦 Extracting files...")
            success, error = await self.extract_archive(file_path, extract_dir)
            if not success:
                return await update.message.reply_text(error)

            uploaded_files = []
            for root, _, files in os.walk(extract_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    await self.upload_to_google_drive(file_path, file, folder_id)
                    uploaded_files.append(file)

            message = f"✅ Uploaded {len(uploaded_files)} files to Google Drive folder '{folder_name}'"
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

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Send welcome message"""
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

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Send help message"""
        await update.message.reply_text(
            "📚 Help Guide:\n\n"
            "1. Click 'Authorize Google Drive' or use /auth\n"
            "2. Complete the Google authorization process\n"
            "3. Copy the localhost URL from your browser\n"
            "4. Send that URL back to the bot\n"
            "5. Now you can send download links for processing\n\n"
            "Cancel anytime with /cancel"
        )

    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle inline button presses"""
        query = update.callback_query
        await query.answer()
        
        if query.data == 'start_auth':
            await self.start_authorization(update, context)
        elif query.data == 'help':
            await self.help_command(update, context)
        elif query.data == 'cancel_auth':
            await self.cancel_auth(update, context)

    async def shutdown(self, signal=None):
        """Clean shutdown procedure"""
        if self.shutting_down:
            return
        self.shutting_down = True
        
        logger.info(f"Received exit signal {signal.name if signal else 'manual'}...")
        
        # Cancel tasks in proper order
        tasks = []
        if self.keepalive_task:
            self.keepalive_task.cancel()
            tasks.append(self.keepalive_task)
        
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

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info("Cleanup completed")

    async def run_bot(self):
        """Run the Telegram bot"""
        self.application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        
        # Fixed ConversationHandler configuration to avoid PTBUserWarning
        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler('start', self.start),
                CommandHandler('auth', self.start_authorization),
                CallbackQueryHandler(self.button_handler, pattern='^start_auth$'),
                CallbackQueryHandler(self.help_command, pattern='^help$'),
                CallbackQueryHandler(self.cancel_auth, pattern='^cancel_auth$')
            ],
            states={
                AUTH_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_auth_url)],
                PROCESS_LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_link)]
            },
            fallbacks=[
                CommandHandler('cancel', self.cancel_auth),
                CallbackQueryHandler(self.cancel_auth, pattern='^cancel_auth$')
            ]
        )
        
        self.application.add_handler(conv_handler)
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_link))
        
        await self.application.initialize()
        await self.application.start()
        
        if not hasattr(self.application, 'updater') or not self.application.updater:
            self.application.updater = self.application.bot
        await self.application.updater.start_polling(drop_pending_updates=True)
        
        logger.info("Bot started in polling mode")

    async def main(self):
        """Main application entry point"""
        try:
            self.webserver_task = asyncio.create_task(self.run_webserver())
            self.keepalive_task = asyncio.create_task(self.self_ping())
            self.bot_task = asyncio.create_task(self.run_bot())
            
            await asyncio.gather(
                self.webserver_task,
                self.keepalive_task,
                self.bot_task,
                return_exceptions=True
            )
        except asyncio.CancelledError:
            logger.info("Shutting down gracefully...")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
        finally:
            await self.shutdown()

def handle_signal(bot_app, signal):
    """Handle shutdown signals"""
    asyncio.create_task(bot_app.shutdown(signal))

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    bot_app = BotApplication()
    bot_app.loop = loop
    
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
        loop.close()
