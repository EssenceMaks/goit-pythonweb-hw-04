import asyncio
import logging
import shutil
from pathlib import Path
import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from collections import defaultdict
import subprocess
import functools
import queue
import threading
import signal
import sys
import concurrent.futures

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global settings for file conflict resolution
FILE_CONFLICT_ACTION = None
FOLDER_CONFLICT_ACTION = None
APPLY_TO_ALL_FILES = False
APPLY_TO_ALL_FOLDERS = False

# Черга для обміну повідомленнями між асинхронним та GUI потоками
async_gui_queue = queue.Queue()

# Список активних потоків для коректного завершення
active_threads = []

# Створюємо глобальний ThreadPoolExecutor для запуску блокуючих операцій
thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)

def run_in_background(func):
    """
    Декоратор для запуску асинхронних функцій у фоновому режимі
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        
        def run_async():
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(func(*args, **kwargs))
            except asyncio.CancelledError:
                logger.info(f"Task {func.__name__} was cancelled")
            except Exception as e:
                logger.error(f"Error in {func.__name__}: {str(e)}")
            finally:
                loop.close()
                # Видаляємо потік зі списку активних при завершенні
                global active_threads
                current_thread = threading.current_thread()
                if current_thread in active_threads:
                    active_threads.remove(current_thread)
                
        # Запускаємо в окремому потоці
        thread = threading.Thread(target=run_async)
        thread.daemon = True
        
        # Додаємо потік до списку активних
        active_threads.append(thread)
        
        thread.start()
        return thread
    return wrapper

async def read_folder(source_folder: Path, recursive=False):
    """
    Асинхронно читає файли та папки в початковій директорії
    """
    files = []
    folders = []
    
    try:
        if recursive:
            # Використовуємо rglob для рекурсивного пошуку
            entries = await asyncio.to_thread(list, source_folder.rglob('*'))
            for entry in entries:
                # Пропускаємо саму початкову папку
                if entry == source_folder:
                    continue
                if entry.is_file():
                    files.append(entry)
                elif entry.is_dir():
                    folders.append(entry)
        else:
            # Використовуємо glob для нерекурсивного пошуку (тільки поточна директорія)
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
    Аналізує початкову папку та повертає словники з розширеннями файлів і папками
    """
    files, folders = await read_folder(source_folder, recursive)
    
    extensions = defaultdict(int)
    
    for file in files:
        extension = file.suffix.lower()[1:] or 'no_extension'
        extensions[extension] += 1
        
    # Рахуємо папки, якщо вони є
    if folders:
        extensions['folders'] = len(folders)
        
    return extensions, files, folders

class SortHubApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Sort Hub - Сортувальник файлів")
        self.geometry("650x650")  # Увеличиваем высоту окна с 500 до 650
        self.minsize(650, 650)  # Также увеличиваем минимальный размер
        
        self.source_folder = None
        self.dest_folder = None
        self.files = []
        self.folders = []
        self.extension_vars = {}
        self.recursive_var = tk.BooleanVar(value=False)
        
        # Для управления асинхронными задачами
        self.sorting_in_progress = False
        self.should_cancel = False
        self.is_sorting = False
        
        # События для разрешения конфликтов
        self.conflict_result = None
        self.conflict_resolved = threading.Event()
        
        # Накопленные конфликты
        self.pending_conflicts = []  # Список файлов, ожидающих разрешения конфликта
        self.pending_operations = []  # Список операций для конфликтующих файлов (move/copy)
        self.current_operation = None  # Текущая операция (move/copy)
        
        self.create_widgets()
        
        # Запускаем проверку очереди сообщений
        self.check_async_messages()
        
        # Обработка закрытия окна
        self.protocol("WM_DELETE_WINDOW", self.on_closing)

    def create_widgets(self):
        # Main frame с отступами
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # ________________________________________________
        # 1. Заголовок выбора исходной директории
        ttk.Label(main_frame, text="Яку директорію ви бажаєте відсортувати?", font=("", 12)).pack(pady=(5, 5), anchor=tk.W)
        
        # 2. Выбор исходной директории
        dir_frame = ttk.Frame(main_frame)
        dir_frame.pack(fill=tk.X, pady=5)
        
        self.source_entry = ttk.Entry(dir_frame)
        self.source_entry.pack(side=tk.RIGHT, fill=tk.X, expand=True)
        
        ttk.Button(dir_frame, text="Обрати...", command=self.select_source_dir).pack(side=tk.LEFT, padx=5)
        
        # ________________________________________________
        # 3. Галочка рекурсивного анализа и кнопка анализа
        options_frame = ttk.Frame(main_frame)
        options_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(options_frame, text="Аналіз", command=self.analyze_directory).pack(side=tk.LEFT, padx=5)
        
        ttk.Checkbutton(
            options_frame, 
            text="Проаналізувати по всім директоріям з цього рівня",
            variable=self.recursive_var
        ).pack(side=tk.LEFT)
        
        
        # ________________________________________________
        # 4. Кнопки выбора всех/отмены всех форматов
        select_buttons_frame = ttk.Frame(main_frame)
        select_buttons_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(select_buttons_frame, text="Вибрати всі формати", command=self.select_all).pack(side=tk.LEFT, padx=5)
        ttk.Button(select_buttons_frame, text="Зняти всі формати", command=self.select_none).pack(side=tk.LEFT, padx=5)
        
        # ________________________________________________
        # 5. Окно с найденными форматами
        # Рамка с форматами файлов
        self.extensions_frame = ttk.LabelFrame(main_frame, text="Знайдені формати файлів", padding="10")
        self.extensions_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        # Прокручиваемое окно для форматов
        self.canvas = tk.Canvas(self.extensions_frame, height=180)  # Увеличиваем высоту с 120 до 180
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
        
        def _on_mousewheel(event):
            self.canvas.yview_scroll(int(-1*(event.delta/120)), "units")
            
        # Привязываем обработчики только к canvas
        self.canvas.bind("<MouseWheel>", _on_mousewheel)
        self.canvas.bind("<Button-4>", lambda e: self.canvas.yview_scroll(-1, "units"))
        self.canvas.bind("<Button-5>", lambda e: self.canvas.yview_scroll(1, "units"))
        
        self.canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # ________________________________________________
        # 6. Информация о количестве файлов и их объеме
        self.files_info_frame = ttk.Frame(main_frame)
        self.files_info_frame.pack(fill=tk.X, pady=5)
        
        self.files_count_label = ttk.Label(self.files_info_frame, text="Кількість файлів: 0", font=("", 10))
        self.files_count_label.pack(side=tk.LEFT, padx=5)
        
        self.files_size_label = ttk.Label(self.files_info_frame, text="Загальний об'єм: 0 байт", font=("", 10))
        self.files_size_label.pack(side=tk.RIGHT, padx=5)
        
        # ________________________________________________
        # 7. Заголовок выбора директории назначения
        ttk.Label(main_frame, text="Куди зберегти відсортовані файли?", font=("", 12)).pack(pady=(5, 5), anchor=tk.W)
        
        # 8. Выбор директории назначения
        dest_frame = ttk.Frame(main_frame)
        dest_frame.pack(fill=tk.X, pady=5)
        
        self.dest_entry = ttk.Entry(dest_frame)
        self.dest_entry.pack(side=tk.RIGHT, fill=tk.X, expand=True)
        
        ttk.Button(dest_frame, text="Обрати...", command=self.select_dest_dir).pack(side=tk.LEFT, padx=5)
        
        # ________________________________________________
        # 9. Лог состояния и прогресс-бар
        self.status_label = ttk.Label(main_frame, text="Оберіть директорію для початку", font=("", 10))
        self.status_label.pack(pady=5, anchor=tk.W)
        
        # Добавляем прогресс-бар
        self.progress_frame = ttk.Frame(main_frame)
        self.progress_frame.pack(fill=tk.X, pady=5)
        
        self.progress_bar = ttk.Progressbar(self.progress_frame, orient="horizontal", length=100, mode="determinate")
        self.progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        
        self.progress_label = ttk.Label(self.progress_frame, text="0%")
        self.progress_label.pack(side=tk.RIGHT)
        
        # ________________________________________________
        # 10. Кнопки управления сортировкой
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)
        
        # Кнопка отмены
        self.cancel_button = ttk.Button(
            button_frame, 
            text="Зупинити процес", 
            command=self.cancel_sorting,
            state=tk.DISABLED
        )
        self.cancel_button.pack(side=tk.RIGHT, padx=5)
        
        # Кнопки сортировки
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

    def check_async_messages(self):
        """
        Перевіряє повідомлення від асинхронних функцій
        """
        try:
            while True:
                message = async_gui_queue.get_nowait()
                action = message.get('action')
                
                if action == 'update_status':
                    self.status_label.config(text=message.get('text', ''))
                    
                elif action == 'show_completion':
                    self._show_completion_dialog(message.get('operation', ''))
                    
                elif action == 'show_error':
                    messagebox.showerror("Помилка", message.get('error', 'Невідома помилка'))
                    self._enable_buttons()
                    
                elif action == 'update_analysis':
                    extensions = message.get('extensions', {})
                    files = message.get('files', [])
                    folders = message.get('folders', [])
                    self._show_analysis_results(extensions, files, folders)
                
        except queue.Empty:
            pass
            
        # Перевіряємо чергу кожні 100 мс
        self.after(100, self.check_async_messages)
    
    def select_source_dir(self):
        """
        Вибирає вихідну директорію через діалог
        """
        folder = filedialog.askdirectory(title="Оберіть директорію для сортування")
        if folder:
            self.source_folder = Path(folder)
            self.source_entry.delete(0, tk.END)
            self.source_entry.insert(0, str(self.source_folder))
            self.analyze_directory()
    
    def select_dest_dir(self):
        """
        Вибирає цільову директорію через діалог
        """
        folder = filedialog.askdirectory(title="Оберіть директорію для збереження")
        if folder:
            self.dest_folder = Path(folder)
            self.dest_entry.delete(0, tk.END)
            self.dest_entry.insert(0, str(self.dest_folder))
    
    def analyze_directory(self):
        """
        Запускає асинхронний аналіз обраної директорії
        """
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
        
        # Запускаємо аналіз у фоновому режимі
        recursive = self.recursive_var.get()
        run_in_background(self._analyze_folder_async)(self.source_folder, recursive)
    
    async def _analyze_folder_async(self, folder: Path, recursive=False):
        """
        Асинхронно аналізує директорію
        """
        try:
            extensions, files, folders = await analyze_folder(folder, recursive)
            
            # Відправляємо результати у GUI потік
            async_gui_queue.put({
                'action': 'update_analysis',
                'extensions': extensions,
                'files': files,
                'folders': folders
            })
            
        except Exception as e:
            logger.error(f"Error analyzing folder: {str(e)}")
            async_gui_queue.put({
                'action': 'show_error',
                'error': f"Помилка аналізу директорії: {str(e)}"
            })
    
    def _show_analysis_results(self, extensions, files, folders):
        """
        Відображає результати аналізу директорії в GUI
        """
        # Очищаємо попередні результати
        for widget in self.scrollable_frame.winfo_children():
            widget.destroy()
        self.extension_vars.clear()
        
        # Зберігаємо дані
        self.files = files
        self.folders = folders
        
        if not extensions:
            self.status_label.config(text=f"Файли не знайдено у {self.source_folder}")
            return
        
        # Оновлюємо статус
        files_count = len(self.files)
        folders_count = len(self.folders)
        
        text = f"Знайдено {files_count} файлів"
        if folders_count > 0:
            text += f", {folders_count} папок"
        text += f" (всього {len(extensions)} форматів)"
        
        self.status_label.config(text=text)
        
        # Створюємо чекбокси для кожного розширення
        for i, (ext, count) in enumerate(sorted(extensions.items())):
            var = tk.BooleanVar(value=True)
            self.extension_vars[ext] = var
            
            frame = ttk.Frame(self.scrollable_frame)
            frame.pack(fill=tk.X, expand=True, pady=2)
            
            # Додаємо відстеження зміни стану чекбокса
            cb = ttk.Checkbutton(frame, variable=var, command=self.update_selected_files_count)
            cb.pack(side=tk.LEFT)
            
            if ext == 'folders':
                ttk.Label(frame, text=f"Папки ({count} шт.)").pack(side=tk.LEFT, padx=5)
            else:
                ttk.Label(frame, text=f"{ext or 'Без розширення'} ({count} файлів)").pack(side=tk.LEFT, padx=5)
        
        # Вмикаємо кнопки сортування
        self.move_button.config(state=tk.NORMAL)
        self.copy_button.config(state=tk.NORMAL)
        
        # Ініціалізуємо лічильники
        self.update_selected_files_count()
    
    def update_selected_files_count(self):
        """
        Оновлює кількість вибраних файлів та їх загальний об'єм
        """
        selected_files_count = 0
        selected_files_size = 0
        
        for file in self.files:
            ext = file.suffix.lower()[1:] or 'no_extension'
            if ext in self.extension_vars and self.extension_vars.get(ext).get():
                selected_files_count += 1
                selected_files_size += file.stat().st_size
        
        # Форматуємо розмір для більш зручного відображення
        size_str = self.format_file_size(selected_files_size)
        
        self.files_count_label.config(text=f"Кількість файлів: {selected_files_count}")
        self.files_size_label.config(text=f"Загальний об'єм: {size_str}")
        
    def format_file_size(self, size_in_bytes):
        """
        Форматує розмір файлу для відображення у зручному вигляді
        """
        if size_in_bytes < 1024:
            return f"{size_in_bytes} байт"
        elif size_in_bytes < 1024 * 1024:
            return f"{size_in_bytes / 1024:.2f} КБ"
        elif size_in_bytes < 1024 * 1024 * 1024:
            return f"{size_in_bytes / (1024 * 1024):.2f} МБ"
        else:
            return f"{size_in_bytes / (1024 * 1024 * 1024):.2f} ГБ"
    
    def select_all(self):
        """
        Вибирає всі формати файлів
        """
        for var in self.extension_vars.values():
            var.set(True)
        self.update_selected_files_count()
    
    def select_none(self):
        """
        Знімає вибір з усіх форматів файлів
        """
        for var in self.extension_vars.values():
            var.set(False)
        self.update_selected_files_count()
    
    def start_sorting(self, operation='move'):
        """
        Запускає асинхронне сортування файлів
        """
        if not self.extension_vars:
            messagebox.showinfo("Інформація", "Спочатку проаналізуйте директорію")
            return
            
        dest_path = self.dest_entry.get()
        if not dest_path:
            messagebox.showerror("Помилка", "Будь ласка, оберіть директорію для збереження")
            return
            
        self.dest_folder = Path(dest_path)
        
        # Перевіряємо, чи співпадають вихідна та цільова директорії
        if self.source_folder == self.dest_folder:
            if not messagebox.askyesno(
                "Увага", 
                "Вихідна та цільова директорії однакові! Це може призвести до проблем. Продовжити?"
            ):
                return
            
        selected_extensions = [ext for ext, var in self.extension_vars.items() if var.get()]
        
        if not selected_extensions:
            messagebox.showinfo("Інформація", "Виберіть хоча б один формат файлів для сортування")
            return

        has_folders = 'folders' in selected_extensions
        
        recursive = self.recursive_var.get()
        
        action_name = "перенесення" if operation == 'move' else "копіювання"
        self.status_label.config(text=f"Сортування файлів ({action_name})...")
        self.move_button.config(state=tk.DISABLED)
        self.copy_button.config(state=tk.DISABLED)
        self.cancel_button.config(state=tk.NORMAL)
        self.is_sorting = True
        self.should_cancel = False
        
        # Скидаємо глобальні налаштування розв'язання конфліктів
        global FILE_CONFLICT_ACTION, FOLDER_CONFLICT_ACTION, APPLY_TO_ALL_FILES, APPLY_TO_ALL_FOLDERS
        FILE_CONFLICT_ACTION = None
        FOLDER_CONFLICT_ACTION = None
        APPLY_TO_ALL_FILES = False
        APPLY_TO_ALL_FOLDERS = False
        
        # Запускаємо асинхронне сортування
        run_in_background(self._run_sorting_async)(
            selected_extensions, 
            has_folders,
            recursive,
            operation
        )
    
    async def _run_sorting_async(self, selected_extensions, has_folders, recursive, operation):
        """
        Запускає асинхронне сортування файлів
        """
        try:
            # Очищаємо попередній список конфліктів
            self.pending_conflicts = []
            self.pending_operations = []
            self.current_operation = operation
            
            # Створюємо цільову папку, якщо вона не існує
            os.makedirs(self.dest_folder, exist_ok=True)
            
            # Отримуємо файли та папки
            files, folders = await read_folder(self.source_folder, recursive)
            
            if not files and not folders:
                async_gui_queue.put({
                    'action': 'update_status',
                    'text': f"Файли не знайдено у {self.source_folder}"
                })
                async_gui_queue.put({
                    'action': 'show_completion',
                    'operation': 'переміщено' if operation == 'move' else 'скопійовано'
                })
                return
            
            # Розраховуємо загальну кількість файлів і папок для обробки
            total_items = 0
            for file in files:
                ext = file.suffix.lower()[1:] or 'no_extension'
                # Враховуємо тільки файли з вибраними розширеннями
                if ext in selected_extensions:
                    total_items += 1
            
            # Додаємо папки, якщо вони вибрані
            if has_folders and 'folders' in selected_extensions:
                total_items += len(folders)
            
            # Ініціалізуємо лічильник оброблених елементів
            processed_items = 0
            
            # Оновлюємо прогрес-бар до 0
            self.after(0, lambda: self._update_progress(0, 0, total_items))
            
            # Обробляємо файли
            for file in files:
                if self.should_cancel:
                    logger.info("Sorting process was cancelled by the user.")
                    return  # Замінюємо break на return, оскільки break некоректний поза циклом
                    
                ext = file.suffix.lower()[1:] or 'no_extension'
                # Обробляємо тільки файли з вибраними розширеннями
                if ext in selected_extensions:
                    if operation == 'move':
                        await self._move_file_async(file, self.dest_folder)
                    else:
                        await self._copy_file_async(file, self.dest_folder)
                    
                    # Збільшуємо лічильник і оновлюємо прогрес
                    processed_items += 1
                    progress_percent = int(100 * processed_items / total_items) if total_items > 0 else 0
                    self.after(0, lambda p=processed_items, t=total_items, pp=progress_percent: 
                               self._update_progress(pp, p, t))
            
            # Обробляємо папки, якщо вибрані
            if has_folders and 'folders' in selected_extensions:
                for folder in folders:
                    if self.should_cancel:
                        logger.info("Sorting process was cancelled by the user.")
                        return  # Замінюємо break на return, оскільки break некоректний поза циклом
                        
                    # Пропускаємо папки, які можуть викликати рекурсію
                    if str(self.dest_folder).startswith(str(folder)):
                        logger.warning(f"Skipping folder {folder.name} to avoid recursion")
                        continue
                        
                    if operation == 'move':
                        await self._move_folder_async(folder, self.dest_folder)
                    else:
                        await self._copy_folder_async(folder, self.dest_folder)
                    
                    # Збільшуємо лічильник і оновлюємо прогрес
                    processed_items += 1
                    progress_percent = int(100 * processed_items / total_items) if total_items > 0 else 0
                    self.after(0, lambda p=processed_items, t=total_items, pp=progress_percent: 
                               self._update_progress(pp, p, t))

            # Якщо є накопичені конфлікти, обробляємо їх
            if self.pending_conflicts and not self.should_cancel:
                self.status_label.config(text=f"Обробка конфліктів файлів ({len(self.pending_conflicts)} шт.)...")
                await self.process_pending_conflicts()
            
            # Показуємо інформацію про завершення
            if not self.should_cancel:
                action_name = "переміщено" if operation == 'move' else "скопійовано"
                async_gui_queue.put({
                    'action': 'show_completion',
                    'operation': action_name
                })
            
        except Exception as e:
            logger.error(f"Error during sorting: {str(e)}")
            async_gui_queue.put({
                'action': 'show_error',
                'error': f"Помилка під час сортування: {str(e)}"
            })
        finally:
            # Гарантовано скидаємо стан сортування
            self.after(0, self._reset_sorting_state)
    
    def _update_progress(self, percent, processed, total):
        """
        Оновлює прогрес-бар і текст прогресу
        """
        self.progress_bar['value'] = percent
        # Змінюємо формат відображення прогресу, щоб показувати кількість оброблених файлів
        self.progress_label.config(text=f"{percent}% (оброблено {processed} з {total})")
    
    async def process_pending_conflicts(self):
        """
        Обробляє накопичені конфлікти файлів
        """
        if not self.pending_conflicts:
            return
            
        # Показуємо загальну кількість конфліктів
        total_conflicts = len(self.pending_conflicts)
        self.status_label.config(text=f"Виявлено {total_conflicts} конфліктів файлів. Очікування рішення...")
        
        # Обробляємо кожен конфлікт
        for i, (source_file, dest_path, is_file) in enumerate(self.pending_conflicts):
            if self.should_cancel:
                logger.info("Conflict resolution was cancelled by the user.")
                break
                
            # Оновлюємо статус
            self.status_label.config(text=f"Виконання конфлікту {i+1} з {total_conflicts}...")
            
            # Отримуємо рішення від користувача
            operation = self.pending_operations[i] if i < len(self.pending_operations) else 'copy'
            resolution = await self.resolve_conflict_with_counter(source_file, dest_path, is_file, i+1, total_conflicts)
            
            if resolution == "skip":
                logger.info(f"Skipped {operation} of {source_file.name}")
                continue
                
            elif resolution == "replace":
                # Замінюємо існуючий файл
                logger.info(f"Replacing existing file: {dest_path}")
                
                # Виконуємо переміщення чи копіювання
                try:
                    if operation == 'move':
                        await asyncio.to_thread(shutil.move, source_file, dest_path)
                        logger.info(f"Moved {source_file.name} to {dest_path}")
                    else:
                        await asyncio.to_thread(shutil.copy2, source_file, dest_path)
                        logger.info(f"Copied {source_file.name} to {dest_path}")
                except Exception as e:
                    logger.error(f"Error during {operation} for {source_file.name}: {str(e)}")
                    
            else:  # rename
                # Створюємо унікальне ім'я з суфіксом
                counter = 1
                stem = dest_path.stem
                suffix = dest_path.suffix
                parent = dest_path.parent
                
                while True:
                    new_name = f"{stem}_{counter}{suffix}"
                    new_path = parent / new_name
                    if not await asyncio.to_thread(lambda: new_path.exists()):
                        # Виконуємо переміщення чи копіювання з новим іменем
                        try:
                            if operation == 'move':
                                await asyncio.to_thread(shutil.move, source_file, new_path)
                                logger.info(f"Moved {source_file.name} to {new_path}")
                            else:
                                await asyncio.to_thread(shutil.copy2, source_file, new_path)
                                logger.info(f"Copied {source_file.name} to {new_path}")
                            break
                        except Exception as e:
                            logger.error(f"Error during {operation} for {source_file.name}: {str(e)}")
                            break
                    counter += 1
    
    async def resolve_conflict_with_counter(self, source_path, dest_path, is_file=True, current=1, total=1):
        """
        Асинхронно вирішує конфлікт файлів/папок через GUI з відображенням лічильника
        """
        global FILE_CONFLICT_ACTION, FOLDER_CONFLICT_ACTION, APPLY_TO_ALL_FILES, APPLY_TO_ALL_FOLDERS
        
        # Перевіряємо, чи є вже рішення для всіх файлів/папок
        if is_file and APPLY_TO_ALL_FILES and FILE_CONFLICT_ACTION:
            logger.info(f"Using saved file conflict action: {FILE_CONFLICT_ACTION}")
            return FILE_CONFLICT_ACTION
        elif not is_file and APPLY_TO_ALL_FOLDERS and FOLDER_CONFLICT_ACTION:
            logger.info(f"Using saved folder conflict action: {FOLDER_CONFLICT_ACTION}")
            return FOLDER_CONFLICT_ACTION
            
        # Скидаємо попередній стан
        self.conflict_resolved.clear()
        self.conflict_result = None
        
        # Запитуємо рішення через GUI
        future = asyncio.get_running_loop().run_in_executor(
            None, 
            lambda: self.show_conflict_dialog_with_counter(source_path, dest_path, is_file, current, total)
        )
        
        # Чекаємо результату з GUI
        action = await future
        
        # Зберігаємо дію для всіх файлів/папок, якщо вказано
        if is_file and APPLY_TO_ALL_FILES:
            FILE_CONFLICT_ACTION = action
        elif not is_file and APPLY_TO_ALL_FOLDERS:
            FOLDER_CONFLICT_ACTION = action
            
        logger.info(f"Conflict resolution for {source_path.name}: {action}")
        return action
    
    def show_conflict_dialog_with_counter(self, source_path, dest_path, is_file=True, current=1, total=1):
        """
        Показує діалог конфлікту в синхронному режимі з зазначенням номера конфлікту
        """
        global APPLY_TO_ALL_FILES, APPLY_TO_ALL_FOLDERS
        
        # Змінні для зберігання результату
        result = {"action": "skip"}  # За замовчуванням - пропустити
        
        # Створюємо діалогове вікно
        dialog = tk.Toplevel(self)
        dialog.title("Конфлікт " + ("файлів" if is_file else "папок"))
        dialog.geometry("450x280")
        dialog.resizable(False, False)
        dialog.transient(self)
        dialog.grab_set()  # Діалог модальний
        
        # Радіо-кнопки і прапорець
        action_var = tk.StringVar(value="rename")
        apply_all_var = tk.BooleanVar(value=False)
        
        frame = ttk.Frame(dialog, padding="20")
        frame.pack(fill=tk.BOTH, expand=True)
        
        # Показуємо лічильник конфліктів
        counter_text = f"Конфлікт {current} з {total}"
        ttk.Label(frame, text=counter_text, font=("", 10, "bold")).pack(pady=(0, 10))
        
        entity_type = "Файл" if is_file else "Папка"
        msg = f'{entity_type} "{source_path.name}" вже існує у папці призначення.'
        ttk.Label(frame, text=msg, wraplength=400).pack(pady=(0, 10))
        
        # Додаємо радіокнопки в залежності від типу об'єкта
        ttk.Radiobutton(frame, text="Перейменувати (додати номер)", variable=action_var, value="rename").pack(anchor=tk.W)
        ttk.Radiobutton(frame, text="Замінити існуючий", variable=action_var, value="replace").pack(anchor=tk.W)
        
        # Додаємо опцію "Об'єднати" тільки для директорій
        if not is_file:
            ttk.Radiobutton(frame, text="Об'єднати", variable=action_var, value="merge").pack(anchor=tk.W)
            
        ttk.Radiobutton(frame, text="Пропустити", variable=action_var, value="skip").pack(anchor=tk.W)
        
        apply_text = "Застосувати до всіх конфліктів " + ("файлів" if is_file else "папок") 
        ttk.Checkbutton(frame, text=apply_text, variable=apply_all_var).pack(pady=10, anchor=tk.W)
        
        def on_ok():
            # Зберігаємо обрану дію
            result["action"] = action_var.get()
            
            # Оновлюємо глобальні налаштування
            if is_file:
                global APPLY_TO_ALL_FILES, FILE_CONFLICT_ACTION
                APPLY_TO_ALL_FILES = apply_all_var.get()
                if APPLY_TO_ALL_FILES:
                    FILE_CONFLICT_ACTION = result["action"]
            else:
                global APPLY_TO_ALL_FOLDERS, FOLDER_CONFLICT_ACTION
                APPLY_TO_ALL_FOLDERS = apply_all_var.get()
                if APPLY_TO_ALL_FOLDERS:
                    FOLDER_CONFLICT_ACTION = result["action"]
                    
            # Закриваємо діалог
            dialog.destroy()
        
        def on_cancel():
            # При скасуванні - пропускаємо файл
            result["action"] = "skip"
            dialog.destroy()
        
        # Кнопки
        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(btn_frame, text="Скасувати", command=on_cancel).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="OK", command=on_ok).pack(side=tk.RIGHT)
        
        # При закритті вікна - пропускаємо файл
        dialog.protocol("WM_DELETE_WINDOW", on_cancel)
        
        # Центрування діалогу
        dialog.update_idletasks()
        width = dialog.winfo_width()
        height = dialog.winfo_height()
        x = (dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (dialog.winfo_screenheight() // 2) - (height // 2)
        dialog.geometry(f'+{x}+{y}')
        
        # Піднімаємо діалог на передній план і блокуємо основне вікно
        dialog.lift()
        dialog.focus_force()
        
        # Чекаємо закриття діалогу (блокуючий виклик)
        self.wait_window(dialog)
        
        # Повертаємо результат
        return result["action"]
    
    def _reset_sorting_state(self):
        """
        Скидає стан сортування
        """
        self.is_sorting = False
        self.move_button.config(state=tk.NORMAL)
        self.copy_button.config(state=tk.NORMAL)
        self.cancel_button.config(state=tk.DISABLED)
        
    def cancel_sorting(self):
        """
        Перериває процес сортування
        """
        if self.is_sorting:
            self.should_cancel = True
            self.status_label.config(text="Сортування зупинено користувачем.")
            self._reset_sorting_state()
            
            # Повідомлення користувачу
            messagebox.showinfo("Інформація", "Процес сортування буде зупинено після завершення поточної операції.")
    
    def on_closing(self):
        """
        Коректно завершуємо усі процеси при закритті програми
        """
        if messagebox.askokcancel("Вихід", "Ви впевнені, що хочете вийти?"):
            logger.info("Application closing, terminating all threads...")
            
            # Завершуємо всі активні потоки
            self.terminate_threads()
            
            # Завершуємо пул потоків
            thread_pool.shutdown(wait=False)
            
            # Закриваємо програму
            self.destroy()
            sys.exit(0)
    
    def terminate_threads(self):
        """
        Завершує всі активні потоки
        """
        global active_threads
        for thread in active_threads[:]:
            try:
                if thread.is_alive():
                    logger.info(f"Terminating thread {thread.name}")
                    # У Windows переривання потоків працює інакше
                    if hasattr(thread, "_thread"):
                        if hasattr(thread._thread, "kill"):
                            thread._thread.kill()
            except Exception as e:
                logger.error(f"Error terminating thread: {str(e)}")
    
    async def _move_file_async(self, source_file: Path, dest_folder: Path):
        """
        Асинхронно переміщує файл у папку призначення
        """
        try:
            # Отримуємо розширення файлу
            extension = source_file.suffix.lower()[1:] or 'no_extension'
            
            # Створюємо папку для розширення
            ext_folder = dest_folder / extension
            await asyncio.to_thread(ext_folder.mkdir, exist_ok=True)
            
            # Створюємо шлях призначення
            dest_path = ext_folder / source_file.name
            
            # Перевірка на той самий файл - запобігає переміщенню файлу "у себе"
            if source_file.absolute() == dest_path.absolute():
                logger.info(f"Skipping move of {source_file.name} to itself")
                return
            
            # Перевіряємо конфлікт імен файлів
            if await asyncio.to_thread(dest_path.exists):
                # Якщо це той самий файл (за вмістом), пропускаємо
                if await self._is_same_file(source_file, dest_path):
                    logger.info(f"Skipping identical file {source_file.name}")
                    return
                
                # Перевіряємо, чи є вже глобальне рішення для всіх файлів
                global APPLY_TO_ALL_FILES, FILE_CONFLICT_ACTION
                if APPLY_TO_ALL_FILES and FILE_CONFLICT_ACTION:
                    logger.info(f"Using saved file conflict action: {FILE_CONFLICT_ACTION}")
                    resolution = FILE_CONFLICT_ACTION
                else:
                    # Додаємо до списку очікуючих розв'язання конфліктів
                    logger.info(f"Adding conflict for {source_file.name} to pending list")
                    self.pending_conflicts.append((source_file, dest_path, True))
                    self.pending_operations.append('move')
                    return
                
                if resolution == "skip":
                    logger.info(f"Skipped moving {source_file.name}")
                    return
                elif resolution == "replace":
                    # Використовуємо існуючий шлях
                    logger.info(f"Replacing existing file: {dest_path}")
                else:  # rename
                    # Для перейменування створюємо новий шлях із суфіксом
                    counter = 1
                    stem = dest_path.stem
                    suffix = dest_path.suffix
                    parent = dest_path.parent
                    
                    while True:
                        new_name = f"{stem}_{counter}{suffix}"
                        new_path = parent / new_name
                        if not await asyncio.to_thread(lambda: new_path.exists()):
                            dest_path = new_path
                            logger.info(f"File renamed to: {dest_path}")
                            break
                        counter += 1
            
            # Переміщуємо файл
            await asyncio.to_thread(shutil.move, source_file, dest_path)
            logger.info(f"Moved {source_file.name} to {dest_path}")
            
        except Exception as e:
            logger.error(f"Error moving {source_file}: {str(e)}")
            
    async def _copy_file_async(self, source_file: Path, dest_folder: Path):
        """
        Асинхронно копіює файл у папку призначення
        """
        try:
            # Отримуємо розширення файлу
            extension = source_file.suffix.lower()[1:] or 'no_extension'
            
            # Створюємо папку для розширення
            ext_folder = dest_folder / extension
            await asyncio.to_thread(ext_folder.mkdir, exist_ok=True)
            
            # Створюємо шлях призначення
            dest_path = ext_folder / source_file.name
            
            # Перевірка на той самий файл - запобігає непотрібному копіюванню
            if source_file.absolute() == dest_path.absolute():
                logger.info(f"Skipping copy of {source_file.name} to itself")
                return
            
            # Перевіряємо конфлікт імен файлів
            if await asyncio.to_thread(dest_path.exists):
                # Якщо це той самий файл (за вмістом), пропускаємо
                if await self._is_same_file(source_file, dest_path):
                    logger.info(f"Skipping identical file {source_file.name}")
                    return
                
                # Перевіряємо, чи є вже глобальне рішення для всіх файлів
                global APPLY_TO_ALL_FILES, FILE_CONFLICT_ACTION
                if APPLY_TO_ALL_FILES and FILE_CONFLICT_ACTION:
                    logger.info(f"Using saved file conflict action: {FILE_CONFLICT_ACTION}")
                    resolution = FILE_CONFLICT_ACTION
                else:
                    # Додаємо до списку очікуючих розв'язання конфліктів
                    logger.info(f"Adding conflict for {source_file.name} to pending list")
                    self.pending_conflicts.append((source_file, dest_path, True))
                    self.pending_operations.append('copy')
                    return
                
                if resolution == "skip":
                    logger.info(f"Skipped copying {source_file.name}")
                    return
                elif resolution == "replace":
                    # Використовуємо існуючий шлях
                    logger.info(f"Replacing existing file: {dest_path}")
                else:  # rename
                    # Для перейменування створюємо новий шлях із суфіксом
                    counter = 1
                    stem = dest_path.stem
                    suffix = dest_path.suffix
                    parent = dest_path.parent
                    
                    while True:
                        new_name = f"{stem}_{counter}{suffix}"
                        new_path = parent / new_name
                        if not await asyncio.to_thread(lambda: new_path.exists()):
                            dest_path = new_path
                            logger.info(f"File renamed to: {dest_path}")
                            break
                        counter += 1
            
            # Копіюємо файл
            await asyncio.to_thread(shutil.copy2, source_file, dest_path)
            logger.info(f"Copied {source_file.name} to {dest_path}")
            
        except Exception as e:
            logger.error(f"Error copying {source_file}: {str(e)}")
    
    async def _move_folder_async(self, source_folder: Path, dest_root_folder: Path):
        """
        Асинхронно переміщує папку у папку призначення
        """
        try:
            # Створюємо папку "folders", якщо вона не існує
            folders_dir = dest_root_folder / "folders"
            await asyncio.to_thread(folders_dir.mkdir, exist_ok=True)
            
            # Створюємо шлях призначення
            dest_path = folders_dir / source_folder.name
            
            # Перевірка на переміщення папки в саму себе
            if (source_folder.absolute() == dest_path.absolute() or 
                str(dest_path.absolute()).startswith(str(source_folder.absolute()))):
                logger.warning(f"Skipping folder {source_folder.name} to avoid moving folder into itself")
                return
            
            # Перевіряємо рекурсію - неможливо переміщувати папку саму в себе
            if str(dest_path).startswith(str(source_folder)):
                logger.warning(f"Skipping folder {source_folder.name} to avoid recursion")
                return
            
            # Перевіряємо конфлікт імен папок
            if await asyncio.to_thread(dest_path.exists):
                # Перевіряємо, чи є вже глобальне рішення для всіх папок
                global APPLY_TO_ALL_FOLDERS, FOLDER_CONFLICT_ACTION
                if APPLY_TO_ALL_FOLDERS and FOLDER_CONFLICT_ACTION:
                    logger.info(f"Using saved folder conflict action: {FOLDER_CONFLICT_ACTION}")
                    resolution = FOLDER_CONFLICT_ACTION
                else:
                    # Додаємо до списку очікуючих розв'язання конфліктів
                    logger.info(f"Adding folder conflict for {source_folder.name} to pending list")
                    self.pending_conflicts.append((source_folder, dest_path, False))
                    self.pending_operations.append('move')
                    return
                
                if resolution == "skip":
                    logger.info(f"Skipped moving folder {source_folder.name}")
                    return
                elif resolution == "replace":
                    # Якщо потрібно замінити існуючу папку
                    if await asyncio.to_thread(dest_path.exists) and dest_path != source_folder:
                        await asyncio.to_thread(shutil.rmtree, dest_path)
                elif resolution == "merge":
                    # Для об'єднання папок - не робимо нічого особливого тут,
                    # оскільки shutil.move буде об'єднувати вміст
                    logger.info(f"Merging folder {source_folder.name} with existing folder")
                else:  # rename
                    # Для перейменування створюємо новий шлях із суфіксом
                    counter = 1
                    name = dest_path.name
                    parent = dest_path.parent
                    
                    while True:
                        new_name = f"{name}_{counter}"
                        new_path = parent / new_name
                        if not await asyncio.to_thread(lambda: new_path.exists()):
                            dest_path = new_path
                            logger.info(f"Folder renamed to: {dest_path}")
                            break
                        counter += 1
            
            # Переміщуємо папку
            if source_folder != dest_path:
                await asyncio.to_thread(shutil.move, source_folder, dest_path)
                logger.info(f"Moved folder {source_folder.name} to {folders_dir}")
            else:
                logger.info(f"Skipped moving folder {source_folder.name} (source and destination are the same)")
                
        except Exception as e:
            logger.error(f"Error moving folder {source_folder}: {str(e)}")
    
    async def _copy_folder_async(self, source_folder: Path, dest_root_folder: Path):
        """
        Асинхронно копіює папку у папку призначення
        """
        try:
            # Створюємо папку "folders", якщо вона не існує
            folders_dir = dest_root_folder / "folders"
            await asyncio.to_thread(folders_dir.mkdir, exist_ok=True)
            
            # Створюємо шлях призначення
            dest_path = folders_dir / source_folder.name
            
            # Перевірка на копіювання папки в саму себе
            if (source_folder.absolute() == dest_path.absolute() or 
                str(dest_path.absolute()).startswith(str(source_folder.absolute()))):
                logger.warning(f"Skipping folder {source_folder.name} to avoid copying folder into itself")
                return
            
            # Перевіряємо рекурсію - неможливо копіювати папку саму в себе
            if str(dest_path).startswith(str(source_folder)):
                logger.warning(f"Skipping folder {source_folder.name} to avoid recursion")
                return
            
            # Перевіряємо конфлікт імен папок
            if await asyncio.to_thread(dest_path.exists):
                # Перевіряємо, чи є вже глобальне рішення для всіх папок
                global APPLY_TO_ALL_FOLDERS, FOLDER_CONFLICT_ACTION
                if APPLY_TO_ALL_FOLDERS and FOLDER_CONFLICT_ACTION:
                    logger.info(f"Using saved folder conflict action: {FOLDER_CONFLICT_ACTION}")
                    resolution = FOLDER_CONFLICT_ACTION
                else:
                    # Додаємо до списку очікуючих розв'язання конфліктів
                    logger.info(f"Adding folder conflict for {source_folder.name} to pending list")
                    self.pending_conflicts.append((source_folder, dest_path, False))
                    self.pending_operations.append('copy')
                    return
                
                if resolution == "skip":
                    logger.info(f"Skipped copying folder {source_folder.name}")
                    return
                elif resolution == "replace":
                    # Якщо потрібно замінити існуючу папку
                    if await asyncio.to_thread(dest_path.exists):
                        await asyncio.to_thread(shutil.rmtree, dest_path)
                elif resolution == "merge":
                    # При об'єднанні папок копіюємо вміст папки-джерела в папку призначення
                    logger.info(f"Merging folder {source_folder.name} with existing folder")
                    
                    # Отримуємо список файлів і папок у вихідній директорії
                    for item in source_folder.iterdir():
                        dest_item = dest_path / item.name
                        
                        if item.is_file():
                            # Копіюємо файл, якщо його ще немає або якщо він відрізняється
                            if not dest_item.exists() or not await self._is_same_file(item, dest_item):
                                await asyncio.to_thread(shutil.copy2, item, dest_item)
                        elif item.is_dir():
                            # Для директорій рекурсивно копіюємо, створюючи за необхідності
                            if not dest_item.exists():
                                await asyncio.to_thread(shutil.copytree, item, dest_item)
                            else:
                                # Копіюємо вміст папки
                                for sub_item in item.iterdir():
                                    dest_sub_item = dest_item / sub_item.name
                                    if sub_item.is_file():
                                        if not dest_sub_item.exists() or not await self._is_same_file(sub_item, dest_sub_item):
                                            await asyncio.to_thread(shutil.copy2, sub_item, dest_sub_item)
                                    elif sub_item.is_dir() and not dest_sub_item.exists():
                                        await asyncio.to_thread(shutil.copytree, sub_item, dest_sub_item)
                    
                    # Після об'єднання припиняємо обробку, оскільки вміст уже скопійовано
                    return
                else:  # rename
                    # Для перейменування створюємо новий шлях із суфіксом
                    counter = 1
                    name = dest_path.name
                    parent = dest_path.parent
                    
                    while True:
                        new_name = f"{name}_{counter}"
                        new_path = parent / new_name
                        if not await asyncio.to_thread(lambda: new_path.exists()):
                            dest_path = new_path
                            logger.info(f"Folder renamed to: {dest_path}")
                            break
                        counter += 1
            
            # Копіюємо папку
            await asyncio.to_thread(shutil.copytree, source_folder, dest_path)
            logger.info(f"Copied folder {source_folder.name} to {folders_dir}")
                
        except Exception as e:
            logger.error(f"Error copying folder {source_folder}: {str(e)}")
    
    async def resolve_conflict(self, source_path, dest_path, is_file=True):
        """
        Асинхронно вирішує конфлікт файлів/папок через GUI
        """
        global FILE_CONFLICT_ACTION, FOLDER_CONFLICT_ACTION, APPLY_TO_ALL_FILES, APPLY_TO_ALL_FOLDERS
        
        # Перевіряємо, чи є вже рішення для всіх файлів/папок
        if is_file and APPLY_TO_ALL_FILES and FILE_CONFLICT_ACTION:
            logger.info(f"Using saved file conflict action: {FILE_CONFLICT_ACTION}")
            return FILE_CONFLICT_ACTION
        elif not is_file and APPLY_TO_ALL_FOLDERS and FOLDER_CONFLICT_ACTION:
            logger.info(f"Using saved folder conflict action: {FOLDER_CONFLICT_ACTION}")
            return FOLDER_CONFLICT_ACTION
            
        # Скидаємо попередній стан
        self.conflict_resolved.clear()
        self.conflict_result = None
        
        # Запитуємо рішення через GUI
        future = asyncio.get_running_loop().run_in_executor(
            None, 
            lambda: self.show_conflict_dialog_sync(source_path, dest_path, is_file)
        )
        
        # Чекаємо результату з GUI
        action = await future
        
        # Зберігаємо дію для всіх файлів/папок, якщо вказано
        if is_file and APPLY_TO_ALL_FILES:
            FILE_CONFLICT_ACTION = action
        elif not is_file and APPLY_TO_ALL_FOLDERS:
            FOLDER_CONFLICT_ACTION = action
            
        logger.info(f"Conflict resolution for {source_path.name}: {action}")
        return action
    
    def show_conflict_dialog_sync(self, source_path, dest_path, is_file=True):
        """
        Показує діалог конфлікту в синхронному режимі
        """
        global APPLY_TO_ALL_FILES, APPLY_TO_ALL_FOLDERS
        
        # Змінні для зберігання результату
        result = {"action": "skip"}  # За замовчуванням - пропустити
        
        # Створюємо діалогове вікно
        dialog = tk.Toplevel(self)
        dialog.title("Конфлікт " + ("файлів" if is_file else "папок"))
        dialog.geometry("450x250")
        dialog.resizable(False, False)
        dialog.transient(self)
        dialog.grab_set()  # Діалог модальний
        
        # Радіо-кнопки і прапорець
        action_var = tk.StringVar(value="rename")
        apply_all_var = tk.BooleanVar(value=False)
        
        frame = ttk.Frame(dialog, padding="20")
        frame.pack(fill=tk.BOTH, expand=True)
        
        entity_type = "Файл" if is_file else "Папка"
        msg = f'{entity_type} "{source_path.name}" вже існує у папці призначення.'
        ttk.Label(frame, text=msg, wraplength=400).pack(pady=(0, 10))
        
        ttk.Radiobutton(frame, text="Перейменувати (додати номер)", variable=action_var, value="rename").pack(anchor=tk.W)
        ttk.Radiobutton(frame, text="Замінити існуючий", variable=action_var, value="replace").pack(anchor=tk.W)
        ttk.Radiobutton(frame, text="Об'єднати", variable=action_var, value="merge").pack(anchor=tk.W)
        ttk.Radiobutton(frame, text="Пропустити", variable=action_var, value="skip").pack(anchor=tk.W)
        
        apply_text = "Застосувати до всіх конфліктів " + ("файлів" if is_file else "папок") 
        ttk.Checkbutton(frame, text=apply_text, variable=apply_all_var).pack(pady=10, anchor=tk.W)
        
        def on_ok():
            # Зберігаємо обрану дію
            result["action"] = action_var.get()
            
            # Оновлюємо глобальні налаштування
            if is_file:
                global APPLY_TO_ALL_FILES, FILE_CONFLICT_ACTION
                APPLY_TO_ALL_FILES = apply_all_var.get()
                if APPLY_TO_ALL_FILES:
                    FILE_CONFLICT_ACTION = result["action"]
            else:
                global APPLY_TO_ALL_FOLDERS, FOLDER_CONFLICT_ACTION
                APPLY_TO_ALL_FOLDERS = apply_all_var.get()
                if APPLY_TO_ALL_FOLDERS:
                    FOLDER_CONFLICT_ACTION = result["action"]
                    
            # Закриваємо діалог
            dialog.destroy()
        
        def on_cancel():
            # При скасуванні - пропускаємо файл
            result["action"] = "skip"
            dialog.destroy()
        
        # Кнопки
        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(btn_frame, text="Скасувати", command=on_cancel).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="OK", command=on_ok).pack(side=tk.RIGHT)
        
        # При закритті вікна - пропускаємо файл
        dialog.protocol("WM_DELETE_WINDOW", on_cancel)
        
        # Центрування діалогу
        dialog.update_idletasks()
        width = dialog.winfo_width()
        height = dialog.winfo_height()
        x = (dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (dialog.winfo_screenheight() // 2) - (height // 2)
        dialog.geometry(f'+{x}+{y}')
        
        # Піднімаємо діалог на передній план і блокуємо основне вікно
        dialog.lift()
        dialog.focus_force()
        
        # Чекаємо закриття діалогу (блокуючий виклик)
        self.wait_window(dialog)
        
        # Повертаємо результат
        return result["action"]
    
    async def _is_same_file(self, file1: Path, file2: Path):
        """
        Перевіряє, чи є два файли одним і тим самим фізичним файлом
        """
        try:
            # Для Windows - порівнюємо шляхи
            if os.name == 'nt':
                return os.path.normcase(file1.absolute()) == os.path.normcase(file2.absolute())
            
            # Для POSIX-систем - порівнюємо inode
            else:
                stat1 = await asyncio.to_thread(os.stat, file1)
                stat2 = await asyncio.to_thread(os.stat, file2)
                return (stat1.st_dev == stat2.st_dev and
                        stat1.st_ino == stat2.st_ino)
        except Exception:
            # У випадку помилки вважаємо, що це різні файли
            return False
    
    def _show_completion_dialog(self, action_name):
        """
        Показує діалог завершення
        """
        if self.should_cancel:
            self.status_label.config(text="Сортування зупинено користувачем.")
            self._reset_sorting_state()
            return
            
        self.status_label.config(text=f"Сортування завершено. Файли {action_name}.")
        
        completion_window = tk.Toplevel(self)
        completion_window.title("Завершено")
        completion_window.geometry("300x150")
        completion_window.resizable(False, False)
        completion_window.transient(self)
        completion_window.grab_set()
        
        frame = ttk.Frame(completion_window, padding="20")
        frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(
            frame,
            text=f"Сортування файлів успішно завершено! Файли {action_name}.",
            wraplength=250
        ).pack(pady=(0, 20))
        
        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill=tk.X)
        
        ttk.Button(
            btn_frame,
            text="Відкрити папку",
            command=lambda: [self._open_dest_folder(), completion_window.destroy()]
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            btn_frame,
            text="OK",
            command=completion_window.destroy
        ).pack(side=tk.RIGHT, padx=5)
        
        # Центруємо діалог
        completion_window.update_idletasks()
        width = completion_window.winfo_width()
        height = completion_window.winfo_height()
        x = (completion_window.winfo_screenwidth() // 2) - (width // 2)
        y = (completion_window.winfo_screenheight() // 2) - (height // 2)
        completion_window.geometry(f'+{x}+{y}')
        
        completion_window.focus_force()
        self._reset_sorting_state()
    
    def _open_dest_folder(self):
        """
        Відкриває папку призначення
        """
        try:
            if os.name == 'nt':
                os.startfile(self.dest_folder)
            elif os.name == 'posix':
                subprocess.run(['xdg-open', self.dest_folder], check=True)
        except Exception as e:
            logger.error(f"Error opening destination folder: {str(e)}")

def main():
    app = SortHubApp()
    app.mainloop()

if __name__ == "__main__":
    main()