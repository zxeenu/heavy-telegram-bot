from yt_dlp_client import download_tiktok_video
import logging

logger = logging.getLogger("TestRunner")

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

logger.setLevel(logging.INFO)


def test_youtube_download():
    result = download_tiktok_video(
        url="https://www.youtube.com/shorts/0OmrmUDFprI", reloadlib=False)
    logger.info(result)
    assert result.lower().endswith(
        ".mp4"), f"Expected file path to end with .mp4, got {result}"


def run_tests():
    tests = [
        ("test_youtube_download", test_youtube_download),
    ]

    passed = 0
    for name, fn in tests:
        try:
            fn()
            logger.info(f"✅ {name} passed")
            passed += 1
        except AssertionError as e:
            logger.error(f"❌ {name} failed: {e}")
        except Exception as e:
            logger.error(f"💥 {name} crashed: {e}")

    print(f"\n🏁 {passed}/{len(tests)} passed")


if __name__ == "__main__":
    run_tests()
