import yt_dlp
import hashlib
import os
import importlib
import subprocess
import sys
import logging

logger = logging.getLogger("MediaPirate")


def update_and_reimport_yt_dlp():
    try:
        # Install or update yt-dlp using pip
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--upgrade", "yt-dlp"])

        # Import or reload yt-dlp to get the latest version
        import yt_dlp
        importlib.reload(yt_dlp)
        logger.info(
            "yt-dlp has been updated and re-imported successfully.")

        return yt_dlp  # Return the module if needed
    except Exception as e:
        logger.fatal(f"An error occurred: {e}")
        return None


def download_video(url: str, output_path='./downloads', reloadlib: bool = False) -> str:
    if reloadlib:
        importlib.reload(yt_dlp)

    # # folder where the script lives
    # base_dir = os.path.dirname(os.path.abspath(__file__))
    # output_path = os.path.join(base_dir, output_path)
    os.makedirs(output_path, exist_ok=True)  # ✅ ensure directory exists

    md5_hash = hashlib.md5()
    md5_hash.update(url.encode('utf-8'))
    filename = md5_hash.hexdigest()

    if os.path.exists(f'{output_path}/{filename}.mp4'):
        return f'{output_path}/{filename}.mp4'

    ydl_opts = {
        'outtmpl': f'{output_path}/{filename}.%(ext)s',
        'format': 'best',
        'postprocessors': [{
            'key': 'FFmpegVideoConvertor',
            'preferedformat': 'mp4',  # Convert to mp4 if needed
        }]
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])
    return f'{output_path}/{filename}.mp4'


def download_audio(url: str, output_path='./downloads', reloadlib: bool = False, audio_format='mp3') -> str:
    if reloadlib:
        importlib.reload(yt_dlp)

    os.makedirs(output_path, exist_ok=True)  # Ensure directory exists

    md5_hash = hashlib.md5()
    md5_hash.update(url.encode('utf-8'))
    filename = md5_hash.hexdigest()

    audio_file = f'{output_path}/{filename}.{audio_format}'
    if os.path.exists(audio_file):
        return audio_file

    ydl_opts = {
        'outtmpl': f'{output_path}/{filename}.%(ext)s',
        'format': 'bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': audio_format,
            'preferredquality': '192',  # bitrate for mp3
        }],
        'quiet': True,
        'no_warnings': True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])

    return audio_file
