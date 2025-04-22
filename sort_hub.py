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

# Queue для обмена сообщениями между асинхронным и GUI потоками
async_gui_queue = queue.Queue()

# Список активных потоков для корректного завершения
active_threads = []

# Создаем глобальный ThreadPoolExecutor для запуска блокирующих операций
thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)

def run_in_background(func):
    """
    Декоратор для запуска асинхронных функций в фоновом режиме
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
                # Удаляем поток из списка активных при завершении
                global active_threads
                current_thread = threading.current_thread()
                if current_thread in active_threads:
                    active_threads.remove(current_thread)
                
        # Запускаем в отдельном потоке
        thread = threading.Thread(target=run_async)
        thread.daemon = True
        
        # Добавляем поток в список активных
        active_threads.append(thread)
        
        thread.start()
        return thread
    return wrapper

async def read_folder(source_folder: Path, recursive=False):
    """
    Асинхронно читает файлы и папки в исходной директории
    """
    files = []
    folders = []
    
    try:
        if recursive:
            # Используем rglob для рекурсивного поиска
            entries = await asyncio.to_thread(list, source_folder.rglob('*'))
            for entry in entries:
                # Пропускаем саму исходную папку
                if entry == source_folder:
                    continue
                if entry.is_file():
                    files.append(entry)
                elif entry.is_dir():
                    folders.append(entry)
        else:
            # Используем glob для нерекурсивного поиска (только текущая директория)
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
    Анализирует исходную папку и возвращает словари с расширениями файлов и папками
    """
    files, folders = await read_folder(source_folder, recursive)
    
    extensions = defaultdict(int)
    
    for file in files:
        extension = file.suffix.lower()[1:] or 'no_extension'
        extensions[extension] += 1
        
    # Считаем папки, если они есть
    if folders:
        extensions['folders'] = len(folders)
        
    return extensions, files, folders

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
        
        # Scrollable frame for extensions
        self.canvas = tk.Canvas(self.extensions_frame, height=120)
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
            
        # Привязываем обработчики только к canvas, а не ко всем виджетам
        self.canvas.bind("<MouseWheel>", _on_mousewheel)
        self.canvas.bind("<Button-4>", lambda e: self.canvas.yview_scroll(-1, "units"))
        self.canvas.bind("<Button-5>", lambda e: self.canvas.yview_scroll(1, "units"))
        
        self.canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Status label
        self.status_label = ttk.Label(main_frame, text="Оберіть директорію для початку", font=("", 10))
        self.status_label.pack(pady=5, anchor=tk.W)
        
        # Control buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)
        
        # Select all/none buttons
        ttk.Button(button_frame, text="Вибрати все", command=self.select_all).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Скасувати все", command=self.select_none).pack(side=tk.LEFT, padx=5)
        
        # Cancel button (initially disabled)
        self.cancel_button = ttk.Button(
            button_frame, 
            text="Зупинити процес", 
            command=self.cancel_sorting,
            state=tk.DISABLED
        )
        self.cancel_button.pack(side=tk.RIGHT, padx=5)
        
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

    def check_async_messages(self):
        """
        Проверяет сообщения от асинхронных функций
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
            
        # Проверяем очередь каждые 100 мс
        self.after(100, self.check_async_messages)
    
    def select_source_dir(self):
        """
        Выбирает исходную директорию через диалог
        """
        folder = filedialog.askdirectory(title="Оберіть директорію для сортування")
        if folder:
            self.source_folder = Path(folder)
            self.source_entry.delete(0, tk.END)
            self.source_entry.insert(0, str(self.source_folder))
            self.analyze_directory()
    
    def select_dest_dir(self):
        """
        Выбирает целевую директорию через диалог
        """
        folder = filedialog.askdirectory(title="Оберіть директорію для збереження")
        if folder:
            self.dest_folder = Path(folder)
            self.dest_entry.delete(0, tk.END)
            self.dest_entry.insert(0, str(self.dest_folder))
    
    def analyze_directory(self):
        """
        Запускает асинхронный анализ выбранной директории
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
        
        # Запускаем анализ в фоновом режиме
        recursive = self.recursive_var.get()
        run_in_background(self._analyze_folder_async)(self.source_folder, recursive)
    
    async def _analyze_folder_async(self, folder: Path, recursive=False):
        """
        Асинхронно анализирует директорию
        """
        try:
            extensions, files, folders = await analyze_folder(folder, recursive)
            
            # Отправляем результаты в GUI поток
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
        Отображает результаты анализа директории в GUI
        """
        # Очищаем предыдущие результаты
        for widget in self.scrollable_frame.winfo_children():
            widget.destroy()
        self.extension_vars.clear()
        
        # Сохраняем данные
        self.files = files
        self.folders = folders
        
        if not extensions:
            self.status_label.config(text=f"Файли не знайдено у {self.source_folder}")
            return
        
        # Обновляем статус
        files_count = len(self.files)
        folders_count = len(self.folders)
        
        text = f"Знайдено {files_count} файлів"
        if folders_count > 0:
            text += f", {folders_count} папок"
        text += f" (всього {len(extensions)} форматів)"
        
        self.status_label.config(text=text)
        
        # Создаем чекбоксы для каждого расширения
        for i, (ext, count) in enumerate(sorted(extensions.items())):
            var = tk.BooleanVar(value=True)
            self.extension_vars[ext] = var
            
            frame = ttk.Frame(self.scrollable_frame)
            frame.pack(fill=tk.X, expand=True, pady=2)
            
            ttk.Checkbutton(frame, variable=var).pack(side=tk.LEFT)
            
            if ext == 'folders':
                ttk.Label(frame, text=f"Папки ({count} шт.)").pack(side=tk.LEFT, padx=5)
            else:
                ttk.Label(frame, text=f"{ext or 'Без розширення'} ({count} файлів)").pack(side=tk.LEFT, padx=5)
        
        # Включаем кнопки сортировки
        self.move_button.config(state=tk.NORMAL)
        self.copy_button.config(state=tk.NORMAL)
    
    def select_all(self):
        """
        Выбирает все форматы файлов
        """
        for var in self.extension_vars.values():
            var.set(True)
    
    def select_none(self):
        """
        Снимает выбор со всех форматов файлов
        """
        for var in self.extension_vars.values():
            var.set(False)
    
    def start_sorting(self, operation='move'):
        """
        Запускает асинхронную сортировку файлов
        """
        if not self.extension_vars:
            messagebox.showinfo("Інформація", "Спочатку проаналізуйте директорію")
            return
            
        dest_path = self.dest_entry.get()
        if not dest_path:
            messagebox.showerror("Помилка", "Будь ласка, оберіть директорію для збереження")
            return
            
        self.dest_folder = Path(dest_path)
        
        # Проверяем, совпадают ли исходная и целевая директории
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
        
        # Сбрасываем глобальные настройки разрешения конфликтов
        global FILE_CONFLICT_ACTION, FOLDER_CONFLICT_ACTION, APPLY_TO_ALL_FILES, APPLY_TO_ALL_FOLDERS
        FILE_CONFLICT_ACTION = None
        FOLDER_CONFLICT_ACTION = None
        APPLY_TO_ALL_FILES = False
        APPLY_TO_ALL_FOLDERS = False
        
        # Запускаем асинхронную сортировку
        run_in_background(self._run_sorting_async)(
            selected_extensions, 
            has_folders,
            recursive,
            operation
        )
    
    async def _run_sorting_async(self, selected_extensions, has_folders, recursive, operation):
        """
        Запускает асинхронную сортировку файлов
        """
        try:
            # Очищаем предыдущий список конфликтов
            self.pending_conflicts = []
            self.pending_operations = []
            self.current_operation = operation
            
            # Создаем целевую папку, если она не существует
            os.makedirs(self.dest_folder, exist_ok=True)
            
            # Получаем файлы и папки
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
            
            # Обрабатываем файлы
            for file in files:
                if self.should_cancel:
                    logger.info("Sorting process was cancelled by the user.")
                    break
                    
                ext = file.suffix.lower()[1:] or 'no_extension'
                # Обрабатываем только файлы с выбранными расширениями
                if ext in selected_extensions:
                    if operation == 'move':
                        await self._move_file_async(file, self.dest_folder)
                    else:
                        await self._copy_file_async(file, self.dest_folder)
            
            # Обрабатываем папки если выбраны
            if has_folders and 'folders' in selected_extensions:
                for folder in folders:
                    if self.should_cancel:
                        logger.info("Sorting process was cancelled by the user.")
                        break
                        
                    # Пропускаем папки, которые могут вызвать рекурсию
                    if str(self.dest_folder).startswith(str(folder)):
                        logger.warning(f"Skipping folder {folder.name} to avoid recursion")
                        continue
                        
                    if operation == 'move':
                        await self._move_folder_async(folder, self.dest_folder)
                    else:
                        await self._copy_folder_async(folder, self.dest_folder)

            # Если есть накопленные конфликты, обрабатываем их
            if self.pending_conflicts and not self.should_cancel:
                self.status_label.config(text=f"Обробка конфліктів файлів ({len(self.pending_conflicts)} шт.)...")
                await self.process_pending_conflicts()
            
            # Показываем информацию о завершении
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
            # Гарантированно сбрасываем состояние сортировки
            self.after(0, self._reset_sorting_state)
            
    async def process_pending_conflicts(self):
        """
        Обрабатывает накопленные конфликты файлов
        """
        if not self.pending_conflicts:
            return
            
        # Показываем общее количество конфликтов
        total_conflicts = len(self.pending_conflicts)
        self.status_label.config(text=f"Виявлено {total_conflicts} конфліктів файлів. Очікування рішення...")
        
        # Обрабатываем каждый конфликт
        for i, (source_file, dest_path, is_file) in enumerate(self.pending_conflicts):
            if self.should_cancel:
                logger.info("Conflict resolution was cancelled by the user.")
                break
                
            # Обновляем статус
            self.status_label.config(text=f"Виконання конфлікту {i+1} з {total_conflicts}...")
            
            # Получаем решение от пользователя
            operation = self.pending_operations[i] if i < len(self.pending_operations) else 'copy'
            resolution = await self.resolve_conflict_with_counter(source_file, dest_path, is_file, i+1, total_conflicts)
            
            if resolution == "skip":
                logger.info(f"Skipped {operation} of {source_file.name}")
                continue
                
            elif resolution == "replace":
                # Заменяем существующий файл
                logger.info(f"Replacing existing file: {dest_path}")
                
                # Выполняем перемещение или копирование
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
                # Создаем уникальное имя с суффиксом
                counter = 1
                stem = dest_path.stem
                suffix = dest_path.suffix
                parent = dest_path.parent
                
                while True:
                    new_name = f"{stem}_{counter}{suffix}"
                    new_path = parent / new_name
                    if not await asyncio.to_thread(lambda: new_path.exists()):
                        # Выполняем перемещение или копирование с новым именем
                        try:
                            if operation == 'move':
                                await asyncio.to_thread(shutil.move, source_file, new_path)
                                logger.info(f"Moved {source_file.name} to {new_path}")
                            else:
                                await asyncio.to_thread(shutil.copy2, source_file, new_path)
                                logger.info(f"Copied {source_file.name} to {new_path}")
                        except Exception as e:
                            logger.error(f"Error during {operation} for {source_file.name}: {str(e)}")
                        break
                    counter += 1
    
    async def resolve_conflict_with_counter(self, source_path, dest_path, is_file=True, current=1, total=1):
        """
        Асинхронно разрешает конфликт файлов/папок через GUI с отображением счётчика
        """
        global FILE_CONFLICT_ACTION, FOLDER_CONFLICT_ACTION, APPLY_TO_ALL_FILES, APPLY_TO_ALL_FOLDERS
        
        # Проверяем, есть ли уже решение для всех файлов/папок
        if is_file and APPLY_TO_ALL_FILES and FILE_CONFLICT_ACTION:
            logger.info(f"Using saved file conflict action: {FILE_CONFLICT_ACTION}")
            return FILE_CONFLICT_ACTION
        elif not is_file and APPLY_TO_ALL_FOLDERS and FOLDER_CONFLICT_ACTION:
            logger.info(f"Using saved folder conflict action: {FOLDER_CONFLICT_ACTION}")
            return FOLDER_CONFLICT_ACTION
            
        # Сбрасываем предыдущее состояние
        self.conflict_resolved.clear()
        self.conflict_result = None
        
        # Запрашиваем решение через GUI
        future = asyncio.get_running_loop().run_in_executor(
            None, 
            lambda: self.show_conflict_dialog_with_counter(source_path, dest_path, is_file, current, total)
        )
        
        # Ждем результата из GUI
        action = await future
        
        # Сохраняем действие для всех файлов/папок, если указано
        if is_file and APPLY_TO_ALL_FILES:
            FILE_CONFLICT_ACTION = action
        elif not is_file and APPLY_TO_ALL_FOLDERS:
            FOLDER_CONFLICT_ACTION = action
            
        logger.info(f"Conflict resolution for {source_path.name}: {action}")
        return action
    
    def show_conflict_dialog_with_counter(self, source_path, dest_path, is_file=True, current=1, total=1):
        """
        Показывает диалог конфликта в синхронном режиме с указанием номера конфликта
        """
        global APPLY_TO_ALL_FILES, APPLY_TO_ALL_FOLDERS
        
        # Переменные для хранения результата
        result = {"action": "skip"}  # По умолчанию - пропустить
        
        # Создаем диалоговое окно
        dialog = tk.Toplevel(self)
        dialog.title("Конфлікт " + ("файлів" if is_file else "папок"))
        dialog.geometry("450x280")
        dialog.resizable(False, False)
        dialog.transient(self)
        dialog.grab_set()  # Диалог модальный
        
        # Радио-кнопки и флажок
        action_var = tk.StringVar(value="rename")
        apply_all_var = tk.BooleanVar(value=False)
        
        frame = ttk.Frame(dialog, padding="20")
        frame.pack(fill=tk.BOTH, expand=True)
        
        # Показываем счётчик конфликтов
        counter_text = f"Конфлікт {current} з {total}"
        ttk.Label(frame, text=counter_text, font=("", 10, "bold")).pack(pady=(0, 10))
        
        entity_type = "Файл" if is_file else "Папка"
        msg = f'{entity_type} "{source_path.name}" вже існує у папці призначення.'
        ttk.Label(frame, text=msg, wraplength=400).pack(pady=(0, 10))
        
        ttk.Radiobutton(frame, text="Перейменувати (додати номер)", variable=action_var, value="rename").pack(anchor=tk.W)
        ttk.Radiobutton(frame, text="Замінити існуючий", variable=action_var, value="replace").pack(anchor=tk.W)
        ttk.Radiobutton(frame, text="Пропустити", variable=action_var, value="skip").pack(anchor=tk.W)
        
        apply_text = "Застосувати до всіх конфліктів " + ("файлів" if is_file else "папок") 
        ttk.Checkbutton(frame, text=apply_text, variable=apply_all_var).pack(pady=10, anchor=tk.W)
        
        def on_ok():
            # Сохраняем выбранное действие
            result["action"] = action_var.get()
            
            # Обновляем глобальные настройки
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
                    
            # Закрываем диалог
            dialog.destroy()
        
        def on_cancel():
            # При отмене - пропускаем файл
            result["action"] = "skip"
            dialog.destroy()
        
        # Кнопки
        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(btn_frame, text="Скасувати", command=on_cancel).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="OK", command=on_ok).pack(side=tk.RIGHT)
        
        # При закрытии окна - пропускаем файл
        dialog.protocol("WM_DELETE_WINDOW", on_cancel)
        
        # Центрирование диалога
        dialog.update_idletasks()
        width = dialog.winfo_width()
        height = dialog.winfo_height()
        x = (dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (dialog.winfo_screenheight() // 2) - (height // 2)
        dialog.geometry(f'+{x}+{y}')
        
        # Поднимаем диалог на передний план и блокируем основное окно
        dialog.lift()
        dialog.focus_force()
        
        # Ждем закрытия диалога (блокирующий вызов)
        self.wait_window(dialog)
        
        # Возвращаем результат
        return result["action"]
    
    def _reset_sorting_state(self):
        """
        Сбрасывает состояние сортировки
        """
        self.is_sorting = False
        self.move_button.config(state=tk.NORMAL)
        self.copy_button.config(state=tk.NORMAL)
        self.cancel_button.config(state=tk.DISABLED)
        
    def cancel_sorting(self):
        """
        Прерывает процесс сортировки
        """
        if self.is_sorting:
            self.should_cancel = True
            self.status_label.config(text="Сортування зупинено користувачем.")
            self._reset_sorting_state()
            
            # Сообщение пользователю
            messagebox.showinfo("Інформація", "Процес сортування буде зупинено після завершення поточної операції.")
    
    def on_closing(self):
        """
        Корректно завершаем все процессы при закрытии приложения
        """
        if messagebox.askokcancel("Вихід", "Ви впевнені, що хочете вийти?"):
            logger.info("Application closing, terminating all threads...")
            
            # Завершаем все активные потоки
            self.terminate_threads()
            
            # Завершаем пул потоков
            thread_pool.shutdown(wait=False)
            
            # Закрываем приложение
            self.destroy()
            sys.exit(0)
    
    def terminate_threads(self):
        """
        Завершает все активные потоки
        """
        global active_threads
        for thread in active_threads[:]:
            try:
                if thread.is_alive():
                    logger.info(f"Terminating thread {thread.name}")
                    # В Windows прерывание потоков работает иначе
                    if hasattr(thread, "_thread"):
                        if hasattr(thread._thread, "kill"):
                            thread._thread.kill()
            except Exception as e:
                logger.error(f"Error terminating thread: {str(e)}")
    
    async def _move_file_async(self, source_file: Path, dest_folder: Path):
        """
        Асинхронно перемещает файл в папку назначения
        """
        try:
            # Получаем расширение файла
            extension = source_file.suffix.lower()[1:] or 'no_extension'
            
            # Создаем папку для расширения
            ext_folder = dest_folder / extension
            await asyncio.to_thread(ext_folder.mkdir, exist_ok=True)
            
            # Создаем путь назначения
            dest_path = ext_folder / source_file.name
            
            # Проверка на то же самый файл - предотвращает перемещение файла "в себя"
            if source_file.absolute() == dest_path.absolute():
                logger.info(f"Skipping move of {source_file.name} to itself")
                return
            
            # Проверяем конфликт имен файлов
            if await asyncio.to_thread(dest_path.exists):
                # Если это тот же самый файл (по содержимому), пропускаем
                if await self._is_same_file(source_file, dest_path):
                    logger.info(f"Skipping identical file {source_file.name}")
                    return
                
                # Проверяем, есть ли уже глобальное решение для всех файлов
                global APPLY_TO_ALL_FILES, FILE_CONFLICT_ACTION
                if APPLY_TO_ALL_FILES and FILE_CONFLICT_ACTION:
                    logger.info(f"Using saved file conflict action: {FILE_CONFLICT_ACTION}")
                    resolution = FILE_CONFLICT_ACTION
                else:
                    # Добавляем в список ожидающих разрешения конфликтов
                    logger.info(f"Adding conflict for {source_file.name} to pending list")
                    self.pending_conflicts.append((source_file, dest_path, True))
                    self.pending_operations.append('move')
                    return
                
                if resolution == "skip":
                    logger.info(f"Skipped moving {source_file.name}")
                    return
                elif resolution == "replace":
                    # Используем существующий путь
                    logger.info(f"Replacing existing file: {dest_path}")
                else:  # rename
                    # Для переименования создаем новый путь с суффиксом
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
            
            # Перемещаем файл
            await asyncio.to_thread(shutil.move, source_file, dest_path)
            logger.info(f"Moved {source_file.name} to {dest_path}")
            
        except Exception as e:
            logger.error(f"Error moving {source_file}: {str(e)}")
            
    async def _copy_file_async(self, source_file: Path, dest_folder: Path):
        """
        Асинхронно копирует файл в папку назначения
        """
        try:
            # Получаем расширение файла
            extension = source_file.suffix.lower()[1:] or 'no_extension'
            
            # Создаем папку для расширения
            ext_folder = dest_folder / extension
            await asyncio.to_thread(ext_folder.mkdir, exist_ok=True)
            
            # Создаем путь назначения
            dest_path = ext_folder / source_file.name
            
            # Проверка на то же самый файл - предотвращает ненужное копирование
            if source_file.absolute() == dest_path.absolute():
                logger.info(f"Skipping copy of {source_file.name} to itself")
                return
            
            # Проверяем конфликт имен файлов
            if await asyncio.to_thread(dest_path.exists):
                # Если это тот же самый файл (по содержимому), пропускаем
                if await self._is_same_file(source_file, dest_path):
                    logger.info(f"Skipping identical file {source_file.name}")
                    return
                
                # Проверяем, есть ли уже глобальное решение для всех файлов
                global APPLY_TO_ALL_FILES, FILE_CONFLICT_ACTION
                if APPLY_TO_ALL_FILES and FILE_CONFLICT_ACTION:
                    logger.info(f"Using saved file conflict action: {FILE_CONFLICT_ACTION}")
                    resolution = FILE_CONFLICT_ACTION
                else:
                    # Добавляем в список ожидающих разрешения конфликтов
                    logger.info(f"Adding conflict for {source_file.name} to pending list")
                    self.pending_conflicts.append((source_file, dest_path, True))
                    self.pending_operations.append('copy')
                    return
                
                if resolution == "skip":
                    logger.info(f"Skipped copying {source_file.name}")
                    return
                elif resolution == "replace":
                    # Используем существующий путь
                    logger.info(f"Replacing existing file: {dest_path}")
                else:  # rename
                    # Для переименования создаем новый путь с суффиксом
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
            
            # Копируем файл
            await asyncio.to_thread(shutil.copy2, source_file, dest_path)
            logger.info(f"Copied {source_file.name} to {dest_path}")
            
        except Exception as e:
            logger.error(f"Error copying {source_file}: {str(e)}")
    
    async def _move_folder_async(self, source_folder: Path, dest_root_folder: Path):
        """
        Асинхронно перемещает папку в папку назначения
        """
        try:
            # Создаем папку "folders" если она не существует
            folders_dir = dest_root_folder / "folders"
            await asyncio.to_thread(folders_dir.mkdir, exist_ok=True)
            
            # Создаем путь назначения
            dest_path = folders_dir / source_folder.name
            
            # Проверка на перемещение папки в саму себя
            if (source_folder.absolute() == dest_path.absolute() or 
                str(dest_path.absolute()).startswith(str(source_folder.absolute()))):
                logger.warning(f"Skipping folder {source_folder.name} to avoid moving folder into itself")
                return
            
            # Проверяем рекурсию - нельзя перемещать папку саму в себя
            if str(dest_path).startswith(str(source_folder)):
                logger.warning(f"Skipping folder {source_folder.name} to avoid recursion")
                return
            
            # Проверяем конфликт имен папок
            if await asyncio.to_thread(dest_path.exists):
                # Получаем решение пользователя о конфликте
                resolution = await self.resolve_conflict(source_folder, dest_path, False)
                
                if resolution == "skip":
                    logger.info(f"Skipped moving folder {source_folder.name}")
                    return
                elif resolution == "replace":
                    # Если нужно заменить существующую папку
                    if await asyncio.to_thread(dest_path.exists) and dest_path != source_folder:
                        await asyncio.to_thread(shutil.rmtree, dest_path)
                else:  # rename
                    # Для переименования создаем новый путь с суффиксом
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
            
            # Перемещаем папку
            if source_folder != dest_path:
                await asyncio.to_thread(shutil.move, source_folder, dest_path)
                logger.info(f"Moved folder {source_folder.name} to {folders_dir}")
            else:
                logger.info(f"Skipped moving folder {source_folder.name} (source and destination are the same)")
                
        except Exception as e:
            logger.error(f"Error moving folder {source_folder}: {str(e)}")
    
    async def _copy_folder_async(self, source_folder: Path, dest_root_folder: Path):
        """
        Асинхронно копирует папку в папку назначения
        """
        try:
            # Создаем папку "folders" если она не существует
            folders_dir = dest_root_folder / "folders"
            await asyncio.to_thread(folders_dir.mkdir, exist_ok=True)
            
            # Создаем путь назначения
            dest_path = folders_dir / source_folder.name
            
            # Проверка на копирование папки в саму себя
            if (source_folder.absolute() == dest_path.absolute() or 
                str(dest_path.absolute()).startswith(str(source_folder.absolute()))):
                logger.warning(f"Skipping folder {source_folder.name} to avoid copying folder into itself")
                return
            
            # Проверяем рекурсию - нельзя копировать папку саму в себя
            if str(dest_path).startswith(str(source_folder)):
                logger.warning(f"Skipping folder {source_folder.name} to avoid recursion")
                return
            
            # Проверяем конфликт имен папок
            if await asyncio.to_thread(dest_path.exists):
                # Получаем решение пользователя о конфликте
                resolution = await self.resolve_conflict(source_folder, dest_path, False)
                
                if resolution == "skip":
                    logger.info(f"Skipped copying folder {source_folder.name}")
                    return
                elif resolution == "replace":
                    # Если нужно заменить существующую папку
                    if await asyncio.to_thread(dest_path.exists):
                        await asyncio.to_thread(shutil.rmtree, dest_path)
                else:  # rename
                    # Для переименования создаем новый путь с суффиксом
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
            
            # Копируем папку
            await asyncio.to_thread(shutil.copytree, source_folder, dest_path)
            logger.info(f"Copied folder {source_folder.name} to {folders_dir}")
                
        except Exception as e:
            logger.error(f"Error copying folder {source_folder}: {str(e)}")
    
    async def resolve_conflict(self, source_path, dest_path, is_file=True):
        """
        Асинхронно разрешает конфликт файлов/папок через GUI
        """
        global FILE_CONFLICT_ACTION, FOLDER_CONFLICT_ACTION, APPLY_TO_ALL_FILES, APPLY_TO_ALL_FOLDERS
        
        # Проверяем, есть ли уже решение для всех файлов/папок
        if is_file and APPLY_TO_ALL_FILES and FILE_CONFLICT_ACTION:
            logger.info(f"Using saved file conflict action: {FILE_CONFLICT_ACTION}")
            return FILE_CONFLICT_ACTION
        elif not is_file and APPLY_TO_ALL_FOLDERS and FOLDER_CONFLICT_ACTION:
            logger.info(f"Using saved folder conflict action: {FOLDER_CONFLICT_ACTION}")
            return FOLDER_CONFLICT_ACTION
            
        # Сбрасываем предыдущее состояние
        self.conflict_resolved.clear()
        self.conflict_result = None
        
        # Запрашиваем решение через GUI
        future = asyncio.get_running_loop().run_in_executor(
            None, 
            lambda: self.show_conflict_dialog_sync(source_path, dest_path, is_file)
        )
        
        # Ждем результата из GUI
        action = await future
        
        # Сохраняем действие для всех файлов/папок, если указано
        if is_file and APPLY_TO_ALL_FILES:
            FILE_CONFLICT_ACTION = action
        elif not is_file and APPLY_TO_ALL_FOLDERS:
            FOLDER_CONFLICT_ACTION = action
            
        logger.info(f"Conflict resolution for {source_path.name}: {action}")
        return action
    
    def show_conflict_dialog_sync(self, source_path, dest_path, is_file=True):
        """
        Показывает диалог конфликта в синхронном режиме
        """
        global APPLY_TO_ALL_FILES, APPLY_TO_ALL_FOLDERS
        
        # Переменные для хранения результата
        result = {"action": "skip"}  # По умолчанию - пропустить
        
        # Создаем диалоговое окно
        dialog = tk.Toplevel(self)
        dialog.title("Конфлікт " + ("файлів" if is_file else "папок"))
        dialog.geometry("450x250")
        dialog.resizable(False, False)
        dialog.transient(self)
        dialog.grab_set()  # Диалог модальный
        
        # Радио-кнопки и флажок
        action_var = tk.StringVar(value="rename")
        apply_all_var = tk.BooleanVar(value=False)
        
        frame = ttk.Frame(dialog, padding="20")
        frame.pack(fill=tk.BOTH, expand=True)
        
        entity_type = "Файл" if is_file else "Папка"
        msg = f'{entity_type} "{source_path.name}" вже існує у папці призначення.'
        ttk.Label(frame, text=msg, wraplength=400).pack(pady=(0, 10))
        
        ttk.Radiobutton(frame, text="Перейменувати (додати номер)", variable=action_var, value="rename").pack(anchor=tk.W)
        ttk.Radiobutton(frame, text="Замінити існуючий", variable=action_var, value="replace").pack(anchor=tk.W)
        ttk.Radiobutton(frame, text="Пропустити", variable=action_var, value="skip").pack(anchor=tk.W)
        
        apply_text = "Застосувати до всіх конфліктів " + ("файлів" if is_file else "папок") 
        ttk.Checkbutton(frame, text=apply_text, variable=apply_all_var).pack(pady=10, anchor=tk.W)
        
        def on_ok():
            # Сохраняем выбранное действие
            result["action"] = action_var.get()
            
            # Обновляем глобальные настройки
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
                    
            # Закрываем диалог
            dialog.destroy()
        
        def on_cancel():
            # При отмене - пропускаем файл
            result["action"] = "skip"
            dialog.destroy()
        
        # Кнопки
        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(btn_frame, text="Скасувати", command=on_cancel).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="OK", command=on_ok).pack(side=tk.RIGHT)
        
        # При закрытии окна - пропускаем файл
        dialog.protocol("WM_DELETE_WINDOW", on_cancel)
        
        # Центрирование диалога
        dialog.update_idletasks()
        width = dialog.winfo_width()
        height = dialog.winfo_height()
        x = (dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (dialog.winfo_screenheight() // 2) - (height // 2)
        dialog.geometry(f'+{x}+{y}')
        
        # Поднимаем диалог на передний план и блокируем основное окно
        dialog.lift()
        dialog.focus_force()
        
        # Ждем закрытия диалога (блокирующий вызов)
        self.wait_window(dialog)
        
        # Возвращаем результат
        return result["action"]
    
    async def _is_same_file(self, file1: Path, file2: Path):
        """
        Проверяет, являются ли два файла одним и тем же физическим файлом
        """
        try:
            # Для Windows - сравниваем пути
            if os.name == 'nt':
                return os.path.normcase(file1.absolute()) == os.path.normcase(file2.absolute())
            
            # Для POSIX-систем - сравниваем inode
            else:
                stat1 = await asyncio.to_thread(os.stat, file1)
                stat2 = await asyncio.to_thread(os.stat, file2)
                return (stat1.st_dev == stat2.st_dev and
                        stat1.st_ino == stat2.st_ino)
        except Exception:
            # В случае ошибки считаем, что это разные файлы
            return False
    
    def _show_completion_dialog(self, action_name):
        """
        Показывает диалог завершения
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
        
        # Центрируем диалог
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
        Открывает папку назначения
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