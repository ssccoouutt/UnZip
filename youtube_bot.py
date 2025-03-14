import os
import logging
from telegram import Update
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
from pytube import YouTube

# Set up logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Define a command handler for the /start command
def start(update: Update, context: CallbackContext) -> None:
    update.message.reply_text('Send me a YouTube video link and I will download it for you!')

# Define a message handler for video links
def handle_message(update: Update, context: CallbackContext) -> None:
    url = update.message.text
    if 'youtube.com' in url or 'youtu.be' in url:
        try:
            update.message.reply_text('Downloading video...')
            yt = YouTube(url)
            video = yt.streams.get_highest_resolution()
            video_file = video.download()
            update.message.reply_text('Uploading video...')
            with open(video_file, 'rb') as f:
                context.bot.send_video(chat_id=update.message.chat_id, video=f)
            os.remove(video_file)  # Clean up the downloaded file
        except Exception as e:
            logger.error(f"Error: {e}")
            update.message.reply_text('An error occurred while downloading the video.')
    else:
        update.message.reply_text('Please send a valid YouTube link.')

def main() -> None:
    # Get the bot token from the environment variable
    token = os.getenv("TELEGRAM_TOKEN")
    if not token:
        raise ValueError("No TELEGRAM_TOKEN environment variable set.")

    updater = Updater(token)

    # Get the dispatcher to register handlers
    dispatcher = updater.dispatcher

    # Register command and message handlers
    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

    # Start the Bot
    updater.start_polling()

    # Run the bot until you send a signal to stop
    updater.idle()

if __name__ == '__main__':
    main()
