import os
import logging
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from pytube import YouTube

# Set up logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Define a command handler for the /start command
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text('Send me a YouTube video link and I will download it for you!')

# Define a message handler for video links
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    url = update.message.text
    if 'youtube.com' in url or 'youtu.be' in url:
        try:
            await update.message.reply_text('Downloading video...')
            yt = YouTube(url)
            video = yt.streams.get_highest_resolution()
            video_file = video.download()
            await update.message.reply_text('Uploading video...')
            with open(video_file, 'rb') as f:
                await context.bot.send_video(chat_id=update.message.chat_id, video=f)
            os.remove(video_file)  # Clean up the downloaded file
        except Exception as e:
            logger.error(f"Error: {e}")
            await update.message.reply_text('An error occurred while downloading the video.')
    else:
        await update.message.reply_text('Please send a valid YouTube link.')

async def main() -> None:
    # Get the bot token from the environment variable
    token = os.getenv("TELEGRAM_TOKEN")
    if not token:
        raise ValueError("No TELEGRAM_TOKEN environment variable set.")

    # Create the Application
    application = ApplicationBuilder().token(token).build()

    # Register command and message handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Start the Bot
    await application.run_polling()

if __name__ == '__main__':
    import asyncio

    # Check if the event loop is already running
    try:
        asyncio.get_event_loop().run_until_complete(main())
    except RuntimeError:  # If the event loop is already running
        asyncio.run(main())
