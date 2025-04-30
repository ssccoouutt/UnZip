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
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '7539483784:AAE4MlT-IGXEb7md3v6pyhbkxe9VtCXZSe0')
CLIENT_SECRET_FILE = os.getenv('CLIENT_SECRET_FILE', 'credentials.json')
TOKEN_FILE = os.getenv('TOKEN_FILE', 'token.json')
WEB_PORT = int(os.getenv('WEB_PORT', '8000'))
PING_INTERVAL = int(os.getenv('PING_INTERVAL', '25'))
HEALTH_CHECK_ENDPOINT = "/health"
REDIRECT_URI = os.getenv('REDIRECT_URI', 'http://localhost:8080')
MAX_FILE_SIZE = int(os.getenv('MAX_FILE_SIZE', '2500288000'))  # 500MB

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
        self.application = None
        self.keepalive_task = None
        self.webserver_task = None
        self.bot_task = None
        self.loop = None
        self.shutting_down = False

    async def health_check(self, request):
        """Enhanced health check with system status"""
        status = {
            "status": "operational" if not self.shutting_down else "shutting_down",
            "last_active": str(datetime.now()),
            "bot_running": self.application is not None and self.application.running,
            "webserver_running": self.site is not None,
            "google_auth": os.path.exists(TOKEN_FILE),
            "version": "1.1.0"
        }
        status_code = 200 if status["status"] == "operational" else 503
        
        return web.json_response(
            status,
            status=status_code,
            headers={"Cache-Control": "no-cache"}
        )

    async def root_handler(self, request):
        """Handle root endpoint requests"""
        return web.Response(
            text="Google Drive Uploader Bot is running",
            status=200
        )

    async def run_webserver(self):
        """Run the web server for health checks"""
        web_app = web.Application()
        web_app.router.add_get(HEALTH_CHECK_ENDPOINT, self.health_check)
        web_app.router.add_get("/", self.root_handler)
        
        self.runner = web.AppRunner(web_app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, '0.0.0.0', WEB_PORT)
        await self.site.start()
        logger.info(f"Health check server running on port {WEB_PORT}")

    async def self_ping(self):
        """Enhanced keep-alive mechanism"""
        while not self.shutting_down:
            try:
                # Double check with local health check
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f'http://localhost:{WEB_PORT}{HEALTH_CHECK_ENDPOINT}',
                        timeout=10
                    ) as resp:
                        if resp.status != 200:
                            logger.warning(f"Health check failed with status {resp.status}")
                            # Attempt to restart critical components
                            if not self.application.running:
                                logger.info("Restarting bot application...")
                                await self.run_bot()
                        
                # Write last active timestamp
                with open('/tmp/last_active.txt', 'w') as f:
                    f.write(str(datetime.now()))
                    
            except Exception as e:
                logger.error(f"Keepalive error: {str(e)}")
                # Attempt recovery
                if not self.site:
                    await self.run_webserver()
            
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

    async def download_file_from_link(self, url, destination):
        """Download a file from a URL with size check"""
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
                return False, f"❌ File too large (max {MAX_FILE_SIZE/1024/1024}MB allowed)"

            with open(destination, 'wb') as file:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        file.write(chunk)
                        downloaded += len(chunk)
                        # You could add progress reporting here if needed
            return True, None
        except Exception as e:
            logger.error(f"Download error: {e}")
            return False, f"❌ Download failed: {e}"

    async def extract_archive(self, archive_path, extract_dir):
        """Improved archive extraction with proper folder structure"""
        try:
            if not os.path.exists(archive_path):
                return False, "❌ Archive file not found"

            # Get clean folder name from archive filename
            folder_name = os.path.splitext(os.path.basename(archive_path))[0]
            extract_dir = os.path.join(extract_dir, folder_name)
            os.makedirs(extract_dir, exist_ok=True)

            if zipfile.is_zipfile(archive_path):
                with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                    # Preserve all file and folder structure
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

    async def create_drive_folder(self, folder_name, parent_id=None):
        """Create a folder in Google Drive"""
        creds = self.authorize_google_drive()
        service = build('drive', 'v3', credentials=creds)
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_id:
            file_metadata['parents'] = [parent_id]
        folder = service.files().create(body=file_metadata, fields='id').execute()
        return folder.get('id')

    async def upload_to_google_drive(self, file_path, file_name, parent_folder_id=None):
        """Upload a file to Google Drive with retry logic"""
        creds = self.authorize_google_drive()
        service = build('drive', 'v3', credentials=creds)
        
        file_metadata = {'name': file_name}
        if parent_folder_id:
            file_metadata['parents'] = [parent_folder_id]
        
        media = MediaFileUpload(file_path, resumable=True)
        
        # Simple retry mechanism
        max_retries = 3
        for attempt in range(max_retries):
            try:
                file = service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                return file.get('id')
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Upload attempt {attempt + 1} failed, retrying...")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff

    async def start_authorization(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start Google Drive authorization process"""
        try:
            # Clear any previous flow
            if 'flow' in context.user_data:
                del context.user_data['flow']

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
            
            if update.callback_query:
                await update.callback_query.message.reply_text(
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
            else:
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
        
        if 'flow' not in context.user_data:
            await update.message.reply_text(
                "❌ No active authorization session. Please start authorization first using /auth",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔑 Authorize Google Drive", callback_data='start_auth')]
                ])
            )
            return ConversationHandler.END

        flow = context.user_data['flow']
        
        try:
            parsed = urlparse(url)
            if not (parsed.netloc == 'localhost:8080' and parsed.scheme == 'http'):
                await update.message.reply_text(
                    "❌ Invalid URL format. Please send the exact redirect URL you received after authorization.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("❌ Cancel", callback_data='cancel_auth')]
                    ])
                )
                return AUTH_URL
                
            query = parse_qs(parsed.query)
            code = query.get('code', [None])[0]
            
            if not code:
                await update.message.reply_text(
                    "❌ No authorization code found in the URL.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("❌ Cancel", callback_data='cancel_auth')]
                    ])
                )
                return AUTH_URL

            # Fetch the token using the authorization code
            flow.fetch_token(code=code)
            
            # Save the credentials
            with open(TOKEN_FILE, 'w') as token_file:
                token_file.write(flow.credentials.to_json())
            
            # Clean up
            del context.user_data['flow']
            
            await update.message.reply_text(
                "✅ *Authorization Successful!*\n\nYou can now send me download links to process.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🛠 Help", callback_data='help')]
                ])
            )
            return ConversationHandler.END

        except Exception as e:
            logger.error(f"Token exchange error: {e}")
            await update.message.reply_text(
                f"❌ Authorization failed: {str(e)}\n\nPlease try again.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Retry", callback_data='start_auth')],
                    [InlineKeyboardButton("❌ Cancel", callback_data='cancel_auth')]
                ])
            )
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
        """Process a download link with improved folder handling"""
        try:
            if not os.path.exists(TOKEN_FILE):
                await update.message.reply_text(
                    "❌ Google Drive authorization required. Please use /auth first.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔑 Authorize", callback_data='start_auth')]
                    ])
                )
                return

            url = update.message.text.strip()
            # Get filename from URL or use default
            file_name = os.path.basename(urlparse(url).path.split('/')[-1] or "archive")
            file_name = file_name.split('?')[0]  # Remove query params
            file_path = os.path.join(tempfile.gettempdir(), file_name)
            
            await update.message.reply_text("⬇️ Downloading file...")
            success, error = await self.download_file_from_link(url, file_path)
            if not success:
                return await update.message.reply_text(error)

            # Create temp directory for extraction
            temp_extract_dir = tempfile.mkdtemp()
            
            await update.message.reply_text("📦 Extracting files...")
            success, error = await self.extract_archive(file_path, temp_extract_dir)
            if not success:
                shutil.rmtree(temp_extract_dir, ignore_errors=True)
                return await update.message.reply_text(error)

            # Get the actual extracted folder (our extract_archive creates a subfolder)
            extracted_folders = [f for f in os.listdir(temp_extract_dir) 
                               if os.path.isdir(os.path.join(temp_extract_dir, f))]
            if not extracted_folders:
                shutil.rmtree(temp_extract_dir, ignore_errors=True)
                return await update.message.reply_text("❌ No folders found in archive")

            main_extracted_folder = os.path.join(temp_extract_dir, extracted_folders[0])
            
            # Upload to Google Drive
            await update.message.reply_text(f"☁️ Uploading to Google Drive...")
            
            creds = self.authorize_google_drive()
            service = build('drive', 'v3', credentials=creds)
            
            # Create main folder
            folder_metadata = {
                'name': os.path.basename(main_extracted_folder),
                'mimeType': 'application/vnd.google-apps.folder'
            }
            folder = service.files().create(body=folder_metadata, fields='id').execute()
            folder_id = folder.get('id')
            
            # Upload all files recursively
            uploaded_files = []
            for root, _, files in os.walk(main_extracted_folder):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Create subfolder structure if needed
                    relative_path = os.path.relpath(root, main_extracted_folder)
                    current_parent = folder_id
                    
                    if relative_path != '.':
                        path_parts = relative_path.split(os.sep)
                        for part in path_parts:
                            # Check if folder exists
                            query = f"name='{part}' and '{current_parent}' in parents and mimeType='application/vnd.google-apps.folder'"
                            results = service.files().list(q=query, fields="files(id)").execute()
                            items = results.get('files', [])
                            
                            if items:
                                current_parent = items[0]['id']
                            else:
                                # Create new folder
                                file_metadata = {
                                    'name': part,
                                    'mimeType': 'application/vnd.google-apps.folder',
                                    'parents': [current_parent]
                                }
                                new_folder = service.files().create(body=file_metadata, fields='id').execute()
                                current_parent = new_folder.get('id')
                    
                    # Upload file
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

            message = (f"✅ Uploaded {len(uploaded_files)} files to Google Drive\n"
                      f"📁 Folder: {os.path.basename(main_extracted_folder)}")
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
        keyboard = [
            [InlineKeyboardButton("🔑 Authorize Google Drive", callback_data='start_auth')],
            [InlineKeyboardButton("🛠 Help", callback_data='help')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "Welcome to Google Drive Uploader Bot!\n\n"
            "1. First authorize Google Drive access\n"
            "2. Then send me file links to process\n"
            f"⚠️ Max file size: {MAX_FILE_SIZE/1024/1024}MB",
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
            f"⚠️ Max file size: {MAX_FILE_SIZE/1024/1024}MB\n"
            "❌ Cancel anytime with /cancel"
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
        
        # Auth conversation handler
        auth_conv = ConversationHandler(
            entry_points=[
                CommandHandler('auth', self.start_authorization),
                CallbackQueryHandler(self.start_authorization, pattern='^start_auth$')
            ],
            states={
                AUTH_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_auth_url)]
            },
            fallbacks=[
                CommandHandler('cancel', self.cancel_auth),
                CallbackQueryHandler(self.cancel_auth, pattern='^cancel_auth$')
            ],
            allow_reentry=True
        )
        
        # Regular command handlers
        self.application.add_handler(CommandHandler('start', self.start))
        self.application.add_handler(CommandHandler('help', self.help_command))
        
        # Add conversation handlers
        self.application.add_handler(auth_conv)
        
        # Add message handler for processing links
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_link))
        
        # Add callback query handler
        self.application.add_handler(CallbackQueryHandler(self.button_handler))
        
        await self.application.initialize()
        await self.application.start()
        
        if not hasattr(self.application, 'updater') or not self.application.updater:
            self.application.updater = self.application.bot
        await self.application.updater.start_polling(drop_pending_updates=True)
        
        logger.info("Bot started in polling mode")

    async def main(self):
        """Enhanced main application loop"""
        try:
            # Start components with error handling
            try:
                await self.run_webserver()
                await self.run_bot()
            except Exception as e:
                logger.critical(f"Failed to start component: {e}")
                raise

            # Start keepalive after everything is running
            self.keepalive_task = asyncio.create_task(self.self_ping())
            
            # Monitor tasks
            while True:
                await asyncio.sleep(5)
                if self.shutting_down:
                    break
                    
        except asyncio.CancelledError:
            logger.info("Shutting down gracefully...")
        except Exception as e:
            logger.critical(f"Fatal error: {e}", exc_info=True)
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
