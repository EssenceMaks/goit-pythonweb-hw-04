import asyncio
import logging
import shutil
from pathlib import Path
import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def move_file(source_file: Path, dest_folder: Path):
    """
    Asynchronously move a file to its corresponding extension folder.
    """
    try:
        # Get file extension (lowercase) or 'no_extension' if none exists
        extension = source_file.suffix.lower()[1:] or 'no_extension'
        
        # Create extension folder if it doesn't exist
        ext_folder = dest_folder / extension
        ext_folder.mkdir(exist_ok=True)
        
        # Generate destination path
        dest_path = ext_folder / source_file.name
        
        # Move the file using asyncio.to_thread for async I/O
        await asyncio.to_thread(shutil.move, source_file, dest_path)
        logger.info(f"Moved {source_file.name} to {ext_folder}")
        
    except Exception as e:
        logger.error(f"Error moving {source_file}: {str(e)}")

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

async def move_folder(source_folder: Path, dest_root_folder: Path):
    """
    Asynchronously move a folder to the destination folder.
    """
    try:
        # Create "folders" directory if it doesn't exist
        folders_dir = dest_root_folder / "folders"
        folders_dir.mkdir(exist_ok=True)
        
        # Generate destination path
        dest_path = folders_dir / source_folder.name
        
        # Move the folder using asyncio.to_thread for async I/O
        await asyncio.to_thread(shutil.move, source_folder, dest_path)
        logger.info(f"Moved folder {source_folder.name} to {folders_dir}")
        
    except Exception as e:
        logger.error(f"Error moving folder {source_folder}: {str(e)}")

async def copy_folder(source_folder: Path, dest_root_folder: Path):
    """
    Asynchronously copy a folder to the destination folder.
    """
    try:
        # Create "folders" directory if it doesn't exist
        folders_dir = dest_root_folder / "folders"
        folders_dir.mkdir(exist_ok=True)
        
        # Generate destination path
        dest_path = folders_dir / source_folder.name
        
        # Copy the folder using asyncio.to_thread for async I/O
        await asyncio.to_thread(shutil.copytree, source_folder, dest_path)
        logger.info(f"Copied folder {source_folder.name} to {folders_dir}")
        
    except Exception as e:
        logger.error(f"Error copying folder {source_folder}: {str(e)}")

async def read_folder(source_folder: Path, recursive=False):
    """
    Read files and folders in the source folder, with option for recursive search.
    """
    files = []
    folders = []
    
    try:
        if recursive:
            # Use rglob for recursive search
            entries = await asyncio.to_thread(list, source_folder.rglob('*'))
            for entry in entries:
                # Skip the source folder itself
                if entry == source_folder:
                    continue
                if entry.is_file():
                    files.append(entry)
                elif entry.is_dir():
                    folders.append(entry)
        else:
            # Use glob for non-recursive search (only current directory)
            entries = await asyncio.to_thread(list, source_folder.glob('*'))
            for entry in entries:
                if entry.is_file():
                    files.append(entry)
                elif entry.is_dir():
                    folders.append(entry)
    except Exception as e:
        logger.error(f"Error reading folder {source_folder}: {str(e)}")
        
    return files, folders

async def analyze_folder(source_folder: Path, recursive=False):
    """
    Analyze the source folder and return dictionaries with file extensions and folders.
    """
    files, folders = await read_folder(source_folder, recursive)
    
    extensions = defaultdict(int)
    
    for file in files:
        extension = file.suffix.lower()[1:] or 'no_extension'
        extensions[extension] += 1
        
    # Count folders if there are any
    if folders:
        extensions['folders'] = len(folders)
        
    return extensions, files, folders

async def process_files(source_folder: Path, dest_folder: Path, selected_extensions=None, selected_folders=False, 
                        recursive=False, operation='move'):
    """
    Process files and folders in the source folder based on selected extensions.
    Operation can be 'move' or 'copy'.
    """
    try:
        # Get list of files and folders
        files, folders = await read_folder(source_folder, recursive)
        
        if not files and not folders:
            logger.warning(f"No files or folders found in {source_folder}")
            return
            
        tasks = []
        
        # Process files
        for file in files:
            ext = file.suffix.lower()[1:] or 'no_extension'
            # Only process files with selected extensions
            if selected_extensions is None or ext in selected_extensions:
                if operation == 'move':
                    tasks.append(move_file(file, dest_folder))
                else:
                    tasks.append(copy_file(file, dest_folder))
        
        # Process folders if selected
        if selected_folders and 'folders' in selected_extensions:
            for folder in folders:
                if operation == 'move':
                    tasks.append(move_folder(folder, dest_folder))
                else:
                    tasks.append(copy_folder(folder, dest_folder))
                
        if tasks:
            await asyncio.gather(*tasks)
            logger.info(f"Processed {len(tasks)} items")
        else:
            logger.warning("No items selected for processing")
                
    except Exception as e:
        logger.error(f"Error processing folder {source_folder}: {str(e)}")

class SortHubApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Sort Hub - Сортувальник файлів")
        self.geometry("650x500")
        self.minsize(650, 500)
        
        self.source_folder = None
        self.dest_folder = None
        self.files = []
        self.folders = []
        self.extension_vars = {}
        self.recursive_var = tk.BooleanVar(value=False)
        
        self.create_widgets()
        
    def create_widgets(self):
        # Main frame
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Source directory selection
        ttk.Label(main_frame, text="Яку директорію ви бажаєте відсортувати?", font=("", 12)).pack(pady=(10, 5), anchor=tk.W)
        
        dir_frame = ttk.Frame(main_frame)
        dir_frame.pack(fill=tk.X, pady=5)
        
        self.source_entry = ttk.Entry(dir_frame)
        self.source_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        ttk.Button(dir_frame, text="Обрати...", command=self.select_source_dir).pack(side=tk.RIGHT, padx=5)
        
        # Destination directory selection
        ttk.Label(main_frame, text="Куди зберегти відсортовані файли?", font=("", 12)).pack(pady=(10, 5), anchor=tk.W)
        
        dest_frame = ttk.Frame(main_frame)
        dest_frame.pack(fill=tk.X, pady=5)
        
        self.dest_entry = ttk.Entry(dest_frame)
        self.dest_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        ttk.Button(dest_frame, text="Обрати...", command=self.select_dest_dir).pack(side=tk.RIGHT, padx=5)
        
        # Recursive checkbox and analyze button row
        options_frame = ttk.Frame(main_frame)
        options_frame.pack(fill=tk.X, pady=5)
        
        ttk.Checkbutton(
            options_frame, 
            text="Проаналізувати по всім директоріям в цій директорії",
            variable=self.recursive_var
        ).pack(side=tk.LEFT)
        
        ttk.Button(options_frame, text="Проаналізувати директорію", command=self.analyze_directory).pack(side=tk.RIGHT, padx=5)
        
        # Extensions frame (will be populated after directory analysis)
        self.extensions_frame = ttk.LabelFrame(main_frame, text="Знайдені формати файлів", padding="10")
        self.extensions_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        # Scrollable frame for extensions - уменьшаем высоту на треть
        self.canvas = tk.Canvas(self.extensions_frame, height=120)  # Уменьшенная высота
        scrollbar = ttk.Scrollbar(self.extensions_frame, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = ttk.Frame(self.canvas)
        
        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(
                scrollregion=self.canvas.bbox("all")
            )
        )
        
        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=scrollbar.set)
        
        # Добавляем обработку событий колесика мыши для скроллинга
        def _on_mousewheel(event):
            self.canvas.yview_scroll(int(-1*(event.delta/120)), "units")
            
        self.canvas.bind_all("<MouseWheel>", _on_mousewheel)  # Windows и MacOS
        self.canvas.bind_all("<Button-4>", lambda e: self.canvas.yview_scroll(-1, "units"))  # Linux
        self.canvas.bind_all("<Button-5>", lambda e: self.canvas.yview_scroll(1, "units"))   # Linux
        
        self.canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Status label
        self.status_label = ttk.Label(main_frame, text="Оберіть директорію для початку", font=("", 10))
        self.status_label.pack(pady=5, anchor=tk.W)
        
        # Control buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)
        
        # Select all/none buttons
        ttk.Button(button_frame, text="Обрати всі", command=self.select_all).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Скасувати всі", command=self.select_none).pack(side=tk.LEFT, padx=5)
        
        # Sorting buttons
        self.move_button = ttk.Button(
            button_frame, 
            text="Почати сортування переносом", 
            command=lambda: self.start_sorting(operation='move'), 
            state=tk.DISABLED
        )
        self.move_button.pack(side=tk.RIGHT, padx=5)
        
        self.copy_button = ttk.Button(
            button_frame, 
            text="Почати сортування копіюванням", 
            command=lambda: self.start_sorting(operation='copy'), 
            state=tk.DISABLED
        )
        self.copy_button.pack(side=tk.RIGHT, padx=5)
    
    def select_source_dir(self):
        folder = filedialog.askdirectory(title="Оберіть директорію для сортування")
        if folder:
            self.source_folder = Path(folder)
            self.source_entry.delete(0, tk.END)
            self.source_entry.insert(0, str(self.source_folder))
            
            # Сразу анализируем директорию после выбора
            self.analyze_directory()
    
    def select_dest_dir(self):
        folder = filedialog.askdirectory(title="Оберіть директорію для збереження")
        if folder:
            self.dest_folder = Path(folder)
            self.dest_entry.delete(0, tk.END)
            self.dest_entry.insert(0, str(self.dest_folder))
    
    def analyze_directory(self):
        source_path = self.source_entry.get()
        if not source_path:
            messagebox.showerror("Помилка", "Будь ласка, оберіть директорію для сортування")
            return
            
        self.source_folder = Path(source_path)
        
        if not self.source_folder.exists():
            messagebox.showerror("Помилка", f"Директорія {source_path} не існує")
            return
            
        self.status_label.config(text="Аналіз директорії...")
        self.update()
        
        # Run the analysis with recursive option
        recursive = self.recursive_var.get()
        asyncio.run(self.run_analysis(recursive))
    
    async def run_analysis(self, recursive=False):
        # Clear previous extensions
        for widget in self.scrollable_frame.winfo_children():
            widget.destroy()
        self.extension_vars.clear()
        
        extensions, self.files, self.folders = await analyze_folder(self.source_folder, recursive)
        
        if not extensions:
            self.status_label.config(text=f"Файли не знайдено у {self.source_folder}")
            return
        
        # Add extensions checkboxes
        files_count = len(self.files)
        folders_count = len(self.folders)
        total_items = files_count + folders_count
        
        text = f"Знайдено {files_count} файлів"
        if folders_count > 0:
            text += f", {folders_count} папок"
        text += f" (всього {len(extensions)} форматів)"
        
        self.status_label.config(text=text)
        
        # Create checkboxes for each extension
        for i, (ext, count) in enumerate(sorted(extensions.items())):
            var = tk.BooleanVar(value=True)
            self.extension_vars[ext] = var
            
            frame = ttk.Frame(self.scrollable_frame)
            frame.pack(fill=tk.X, expand=True, pady=2)
            
            ttk.Checkbutton(frame, variable=var).pack(side=tk.LEFT)
            
            # Special label for folders
            if ext == 'folders':
                ttk.Label(frame, text=f"Папки ({count} шт.)").pack(side=tk.LEFT, padx=5)
            else:
                ttk.Label(frame, text=f"{ext or 'Без розширення'} ({count} файлів)").pack(side=tk.LEFT, padx=5)
        
        # Enable sort buttons
        self.move_button.config(state=tk.NORMAL)
        self.copy_button.config(state=tk.NORMAL)
    
    def select_all(self):
        for var in self.extension_vars.values():
            var.set(True)
    
    def select_none(self):
        for var in self.extension_vars.values():
            var.set(False)
    
    def start_sorting(self, operation='move'):
        if not self.extension_vars:
            messagebox.showinfo("Інформація", "Спочатку проаналізуйте директорію")
            return
            
        dest_path = self.dest_entry.get()
        if not dest_path:
            messagebox.showerror("Помилка", "Будь ласка, оберіть директорію для збереження")
            return
            
        self.dest_folder = Path(dest_path)
            
        # Get selected extensions
        selected_extensions = [ext for ext, var in self.extension_vars.items() if var.get()]
        
        if not selected_extensions:
            messagebox.showinfo("Інформація", "Виберіть хоча б один формат файлів для сортування")
            return

        # Check for folders option
        has_folders = 'folders' in selected_extensions
        
        recursive = self.recursive_var.get()
        
        action_name = "перенесення" if operation == 'move' else "копіювання"
        self.status_label.config(text=f"Сортування файлів ({action_name})...")
        self.move_button.config(state=tk.DISABLED)
        self.copy_button.config(state=tk.DISABLED)
        self.update()
        
        # Create destination folder if it doesn't exist
        os.makedirs(self.dest_folder, exist_ok=True)
        
        # Run the sorting
        asyncio.run(self.run_sorting(selected_extensions, has_folders, recursive, operation))
        
    async def run_sorting(self, selected_extensions, selected_folders, recursive, operation):
        await process_files(
            self.source_folder, 
            self.dest_folder, 
            selected_extensions, 
            selected_folders,
            recursive,
            operation
        )
        
        action_name = "переміщено" if operation == 'move' else "скопійовано"
        # Show completion message
        self.status_label.config(text=f"Сортування завершено. Файли {action_name}.")
        messagebox.showinfo("Завершено", f"Сортування файлів успішно завершено! Файли {action_name}.")
        self.move_button.config(state=tk.NORMAL)
        self.copy_button.config(state=tk.NORMAL)

def main():
    app = SortHubApp()
    app.mainloop()

if __name__ == "__main__":
    main()