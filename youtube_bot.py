import os
import logging
from pytube import YouTube
from http.cookiejar import MozillaCookieJar
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters

# Set up logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Load cookies from cookies.txt
def load_cookies():
    cookie_jar = MozillaCookieJar('cookies.txt')
    cookie_jar.load()
    return cookie_jar

async def start(update: Update, context):
    await update.message.reply_text('Send me a YouTube link and I will download the video for you!')

async def handle_message(update: Update, context):
    url = update.message.text
    if 'youtube.com' in url or 'youtu.be' in url:
        try:
            # Load cookies
            cookies = load_cookies()
            
            # Download the video using pytube
            yt = YouTube(url, cookies=cookies)
            stream = yt.streams.get_highest_resolution()
            file_path = stream.download(output_path='downloads')
            
            # Send the video back to the user
            await update.message.reply_video(video=open(file_path, 'rb'))
            
            # Clean up the downloaded file
            os.remove(file_path)
        except Exception as e:
            logger.error(f"Error downloading video: {e}")
            await update.message.reply_text('Failed to download the video. Please check the link and try again.')
    else:
        await update.message.reply_text('Please send a valid YouTube link.')

def main():
    # Replace 'YOUR_TELEGRAM_BOT_TOKEN' with your actual bot token
    application = ApplicationBuilder().token('YOUR_TELEGRAM_BOT_TOKEN').build()

    start_handler = CommandHandler('start', start)
    message_handler = MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)

    application.add_handler(start_handler)
    application.add_handler(message_handler)

    application.run_polling()

if __name__ == '__main__':
    main()
