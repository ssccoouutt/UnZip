import os
import logging
import re
import tempfile
import base64
import zipfile
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
SCOPES = ['https://www.googleapis.com/auth/drive']
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')  # From environment variable
GOOGLE_CREDENTIALS = os.getenv('GOOGLE_CREDENTIALS')  # Base64 encoded credentials
CLIENT_SECRET_FILE = 'credentials.json'  # Created from environment variable
TOKEN_FILE = 'token.json'  # Stored in ephemeral storage

# Initialize logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Create credentials.json from environment variable
if GOOGLE_CREDENTIALS and not os.path.exists(CLIENT_SECRET_FILE):
    try:
        with open(CLIENT_SECRET_FILE, 'w') as f:
            f.write(base64.b64decode(GOOGLE_CREDENTIALS).decode())
    except Exception as e:
        logger.error(f"Failed to create credentials.json: {e}")
        raise

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

async def upload_to_google_drive(file_path, file_name):
    """Upload a file to Google Drive."""
    creds = authorize_google_drive()
    service = build('drive', 'v3', credentials=creds)
    file_metadata = {'name': file_name}
    media = MediaFileUpload(file_path, resumable=True)
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return file.get('id')

async def unzip_and_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Unzip a file and upload its contents to Google Drive."""
    try:
        # Check if Google Drive is authorized
        creds = authorize_google_drive()
    except Exception as e:
        # If not authorized, start the OAuth2 flow
        await start_authorization(update, context)
        return

    try:
        # Get the file from the Telegram message
        file = await context.bot.get_file(update.message.document.file_id)
        file_path = os.path.join(tempfile.gettempdir(), update.message.document.file_name)
        await file.download_to_drive(file_path)

        # Check if the file is a zip file
        if not file_path.endswith('.zip'):
            await update.message.reply_text("❌ Please send a .zip file.")
            os.remove(file_path)
            return

        # Create a temporary directory to extract files
        extract_dir = os.path.join(tempfile.gettempdir(), 'extracted_files')
        os.makedirs(extract_dir, exist_ok=True)

        # Unzip the file
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        # Upload each extracted file to Google Drive
        uploaded_files = []
        for root, _, files in os.walk(extract_dir):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                drive_file_id = await upload_to_google_drive(file_path, file_name)
                uploaded_files.append((file_name, drive_file_id))

        # Send a confirmation message
        if uploaded_files:
            message = "✅ Uploaded files to Google Drive:\n"
            for file_name, drive_file_id in uploaded_files:
                message += f"- {file_name} (ID: {drive_file_id})\n"
            await update.message.reply_text(message)
        else:
            await update.message.reply_text("❌ No files found in the zip archive.")

        # Clean up temporary files
        os.remove(file_path)
        for root, _, files in os.walk(extract_dir):
            for file_name in files:
                os.remove(os.path.join(root, file_name))
        os.rmdir(extract_dir)

    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")
        logger.error(f"Unzip and upload error: {e}")

async def start_authorization(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the OAuth2 authorization flow."""
    flow = Flow.from_client_secrets_file(
        CLIENT_SECRET_FILE,
        scopes=SCOPES,
        redirect_uri='urn:ietf:wg:oauth:2.0:oob'
    )
    auth_url, _ = flow.authorization_url(prompt='consent')
    context.user_data['flow'] = flow  # Store the flow object in user_data
    await update.message.reply_text(
        f"🔑 Authorization required!\n\n"
        f"Please visit this link to authorize:\n{auth_url}\n\n"
        "After authorization, send the code you receive back here."
    )

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
        await update.message.reply_text("✅ Authorization successful! You can now send files.")
    except Exception as e:
        await update.message.reply_text(f"❌ Authorization failed. Please try again. Error: {str(e)}")
        logger.error(f"Authorization error: {e}")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all incoming messages."""
    message_text = update.message.text.strip()

    # Check if the message is an authorization code
    if re.match(r'^[A-Za-z0-9_\-]+/[A-Za-z0-9_\-]+$', message_text):
        await handle_authorization_code(update, context)
    else:
        await update.message.reply_text("⚠️ Please send a .zip file or an authorization code.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command."""
    await update.message.reply_text(
        "Send me a .zip file, and I'll unzip it and upload its contents to your Google Drive!"
    )

def main():
    """Start the bot."""
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, unzip_and_upload))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.run_polling()

if __name__ == '__main__':
    main()
