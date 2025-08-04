import logging
import os
from pathlib import Path
from typing import List
from src.core.service_container import ServiceContainer

logger = logging.getLogger()


async def delete_oldest_files(folder: str, max_delete: int = 1000) -> List[str]:
    path = Path(folder)
    if not path.exists():
        logger.debug(f"Path does not exist: {folder}")
        return []

    logger.debug(f"Listing contents of: {folder}")
    try:
        logger.debug("\n".join(os.listdir(folder)))
    except Exception as e:
        logger.debug(f"Failed to list files in {folder}: {e}")
        return []

    files = [f for f in path.iterdir() if f.is_file()]
    logger.debug(f"Total files found: {len(files)}")

    if not files:
        logger.debug("No files to delete.")
        return []

    # Sort files by modification time (oldest first)
    files.sort(key=lambda f: f.stat().st_mtime)

    # Take up to `max_delete` oldest files
    to_delete = files[:max_delete]
    logger.debug(
        f"Preparing to delete {len(to_delete)} file(s): {[str(f) for f in to_delete]}")

    deleted_paths = []

    for f in to_delete:
        try:
            f.unlink()
            deleted_paths.append(str(f))
            logger.debug(f"Deleted: {f}")
        except Exception as e:
            logger.debug(f"Failed to delete {f}: {e}")

    return deleted_paths


async def download_cleanup_command_handler(ctx: ServiceContainer, payload: object):
    max_delete = payload.get("max_delete", 1000)
    total_deleted = 0
    deleted_details = await delete_oldest_files("./downloads", max_delete=max_delete)
    total_deleted = len(deleted_details)

    ctx.logger.info("Download cleanup executed", extra={
        'payload': payload,
        'max_delete': max_delete,
        "deleted_files": deleted_details,
        "total_deleted": total_deleted,
    })
