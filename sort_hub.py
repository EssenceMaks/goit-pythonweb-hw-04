import asyncio
import logging
import shutil
from argparse import ArgumentParser
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def copy_file(source_file: Path, dest_folder: Path):
    """
    Asynchronously copy a file to its corresponding extension folder.
    """
    try:
        # Get file extension (lowercase) or 'no_extension' if none exists
        extension = source_file.suffix.lower()[1:] or 'no_extension'
        
        # Create extension folder if it doesn't exist
        ext_folder = dest_folder / extension
        ext_folder.mkdir(exist_ok=True)
        
        # Generate destination path
        dest_path = ext_folder / source_file.name
        
        # Copy the file using asyncio.to_thread for async I/O
        await asyncio.to_thread(shutil.copy2, source_file, dest_path)
        logger.info(f"Copied {source_file.name} to {ext_folder}")
        
    except Exception as e:
        logger.error(f"Error copying {source_file}: {str(e)}")

async def read_folder(source_folder: Path):
    """
    Recursively read all files in the source folder and its subfolders.
    """
    files = []
    try:
        # Use asyncio.to_thread to make the recursive glob async
        entries = await asyncio.to_thread(list, source_folder.rglob('*'))
        for entry in entries:
            if entry.is_file():
                files.append(entry)
    except Exception as e:
        logger.error(f"Error reading folder {source_folder}: {str(e)}")
    return files

async def process_files(source_folder: Path, dest_folder: Path):
    """
    Process all files in the source folder and its subfolders asynchronously.
    """
    try:
        # Get list of files using read_folder
        files = await read_folder(source_folder)
        
        if files:
            # Create tasks for copying each file
            tasks = [copy_file(file, dest_folder) for file in files]
            await asyncio.gather(*tasks)
            logger.info(f"Processed {len(tasks)} files")
        else:
            logger.warning(f"No files found in {source_folder}")
                
    except Exception as e:
        logger.error(f"Error processing folder {source_folder}: {str(e)}")

async def main():
    # Parse command line arguments
    parser = ArgumentParser(description='Sort files by extension asynchronously')
    parser.add_argument('source', type=str, help='Source folder path')
    parser.add_argument('destination', type=str, help='Destination folder path')
    args = parser.parse_args()

    # Convert to Path objects
    source_path = Path(args.source)
    dest_path = Path(args.destination)

    # Validate source folder exists
    if not source_path.exists():
        logger.error(f"Source folder {args.source} does not exist")
        return

    # Create destination folder if it doesn't exist
    dest_path.mkdir(exist_ok=True)

    # Start processing
    logger.info(f"Starting to process files from {args.source}")
    await process_files(source_path, dest_path)
    logger.info("File processing completed")

if __name__ == "__main__":
    asyncio.run(main())