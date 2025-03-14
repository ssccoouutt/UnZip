import os
import logging
from pytube import YouTube
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

async def start(update: Update, context):
    await update.message.reply_text('📤 Send me a YouTube link to download the video!')

async def handle_message(update: Update, context):
    url = update.message.text
    try:
        # Initialize YouTube object with OAuth for age-restricted videos
        yt = YouTube(
            url,
            use_oauth=True,       # Enable OAuth authentication
            allow_oauth_cache=True  # Cache OAuth tokens
        )
        
        # Get the best progressive stream (video+audio)
        stream = yt.streams.filter(
            progressive=True,
            file_extension='mp4'
        ).order_by('resolution').desc().first()

        if not stream:
            raise Exception("No suitable video stream found")

        # Download the video
        file_path = stream.download(output_path='downloads')
        
        # Send video to user
        await update.message.reply_video(
            video=open(file_path, 'rb'),
            caption=f"🎥 {yt.title}"
        )
        
        # Cleanup
        os.remove(file_path)

    except Exception as e:
        logger.error(f"Download failed: {str(e)}", exc_info=True)
        await update.message.reply_text(
            "❌ Failed to download. Possible reasons:\n"
            "1. Age-restricted video\n"
            "2. Invalid/unavailable URL\n"
            "3. Server network issues\n\n"
            "Try another video or check the link!"
        )

def main():
    # Get bot token from environment
    TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN not set!")

    # Create application
    app = ApplicationBuilder().token(TOKEN).build()

    # Add handlers
    app.add_handler(CommandHandler('start', start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Start polling
    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES
    )

if __name__ == '__main__':
    main()
