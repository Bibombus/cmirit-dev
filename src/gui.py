from abc import abstractmethod
import os
from datetime import datetime
import pandas as pd
import threading
from sqlalchemy import func, select, MetaData, Table
from main import make_engine

from tkinter import *
from tkinter import messagebox
from tkinter import ttk
from tkinter.ttk import *
from tkinter import filedialog
import re
from tkinter.ttk import Combobox

from .OutputWorker.outputWorker import GUILogger, LoggersCollection
from main import process_excel, process_db, ErrorHandlingMode, ProcessingStats
from .OutputWorker.outputWorker import LoggersCollection as logger
import time

def make_field_frame(parent: Widget, label: str) -> Entry:
    """Создает фрейм и вложенные в него однострочное поле для ввода и подпись к нему (слева от поля).

    Args:
        parent (Widget): Родительский виджет для данного фрейма.
        label (str): Текстовая подпись к полю.

    Returns:
        Entry: Поле для ввода.
    """

    frame = Frame(parent)
    frame.pack(fill=X)

    label = Label(frame, text=label, width=35)
    label.pack(side=LEFT, padx=5, pady=5)

    entry = Entry(frame)
    entry.pack(fill=X, padx=5, expand=True)

    return entry


class ProgressWindow(Toplevel):
    def __init__(self, parent, total: int):
        super().__init__(parent)
        self.title("Прогресс обработки")
        self.geometry("400x150")
        self.resizable(False, False)
        
        self.total = total
        self.current = 0
        
        # Прогресс-бар
        self.progress_var = DoubleVar()
        self.progress_bar = ttk.Progressbar(self, variable=self.progress_var, maximum=100)
        self.progress_bar.pack(fill=X, padx=10, pady=10)
        
        # Статистика
        self.stats_frame = Frame(self)
        self.stats_frame.pack(fill=X, padx=10, pady=5)
        
        self.current_label = Label(self.stats_frame, text="Обработано: 0")
        self.current_label.pack(side=LEFT)
        
        self.total_label = Label(self.stats_frame, text=f"Всего: {total}")
        self.total_label.pack(side=RIGHT)
        
        self.percent_label = Label(self, text="0%")
        self.percent_label.pack(pady=5)
        
        # Центрируем окно
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f'{width}x{height}+{x}+{y}')
        
    def update_progress(self, current: int):
        self.current = current
        progress = (current / self.total) * 100
        self.progress_var.set(progress)
        self.current_label.config(text=f"Обработано: {current}")
        self.percent_label.config(text=f"{progress:.1f}%")
        self.update_idletasks()


class ProcessFrame(Frame):
    """ Абстрактный класс формы заполнения данных в графическом интерфейсе. """


    def __init__(self, parent, process_handler, logger, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)
        self._process_handler = process_handler
        self.logger = logger
        self.stats = None
        self.progress_var = DoubleVar()
        self.progress_bar = None
        self.stats_label = None
        self.export_errors_button = None
        self.progress_window = None
        self.processing_thread = None
        self.stop_processing = False
        self.last_update = 0
        self.update_interval = 0.1  # Обновлять каждые 100мс
        self.pending_update = False
        self._update_id = None


    @abstractmethod
    def initUI(self):
        """ Производит размещение элементов интерфейса на фрейме. """
        
        pass

    @property
    def process_handler(self):
        """ Обработчик адресов. """

        return self._process_handler

    @process_handler.setter
    def process_handler(self, _value):
        """ Обработчик адресов. """

        self._process_handler = _value


    @abstractmethod
    def _get_args(self) -> dict:
        """ Возвращает все параметры, введенные пользователем в интерфейсе.

        Returns:
            dict: Параметры, введенные пользователем.
        """

        pass

    
    @abstractmethod
    def validate(self) -> bool:
        """ Производит валидацию формы.

        Returns:
            bool: Результат валидации.
        """

        pass


    def _real_handler(self):
        self.master.master.clear_logs()
        if self._process_handler is not None:
            try:
                args = self._get_args()
                
                # Создаем окно прогресса
                if isinstance(self, ExcelFileFrame):
                    total_rows = len(pd.read_excel(args['input_path'], args['input_sheet']))
                else:
                    engine = make_engine(**args)
                    metadata = MetaData()
                    table = Table(args['input_table_name'], metadata, autoload_with=engine)
                    with engine.connect() as conn:
                        total_rows = conn.execute(select(func.count()).select_from(table)).scalar()
                
                self.progress_window = ProgressWindow(self.master, total_rows)
                
                # Запускаем обработку в отдельном потоке
                self.stop_processing = False
                self.processing_thread = threading.Thread(
                    target=self._process_in_thread,
                    args=(args, total_rows)
                )
                self.processing_thread.start()
                
            except Exception as e:
                messagebox.showerror("Ошибка!", str(e))
                if self.progress_window:
                    self.progress_window.destroy()
                    self.progress_window = None

    def _process_in_thread(self, args, total_rows):
        try:
            def progress_callback(current: int):
                if self.stop_processing:
                    return
                if not self.pending_update:
                    self.pending_update = True
                    if self._update_id:
                        self.after_cancel(self._update_id)
                    self._update_id = self.after(100, lambda: self._update_progress(current, total_rows))
            
            stats = self._process_handler(**args, progress_callback=progress_callback)
            self.after(0, lambda: self._update_stats(stats))
            
        except Exception as error:
            error_message = str(error)
            self.after(0, lambda: messagebox.showerror("Ошибка!", error_message))
        finally:
            self.after(0, lambda: self._cleanup_processing())

    def _cleanup_processing(self):
        if self._update_id:
            self.after_cancel(self._update_id)
            self._update_id = None
        if self.progress_window:
            self.progress_window.destroy()
            self.progress_window = None
        self.processing_thread = None
        self.pending_update = False

    def _update_progress(self, current: int, total: int):
        if self.progress_window:
            self.progress_window.update_progress(current)
            self.pending_update = False
            self._update_id = None

    def _update_stats(self, stats: ProcessingStats):
        self.stats = stats
        self.stats_label.config(text=stats.get_summary())
        if stats.failed > 0:
            self.export_errors_button.pack(side=RIGHT, padx=5, pady=5)
        else:
            self.export_errors_button.pack_forget()
        messagebox.showinfo('Операция завершена!', 'Выполнение операции завершено. Подробности отображены в нижней части интерфейса.')

    def _create_progress_section(self):
        progress_frame = Frame(self)
        progress_frame.pack(fill=X, padx=5, pady=5, anchor=N)

        self.stats_label = Label(progress_frame, text="")
        self.stats_label.pack(fill=X, padx=5, pady=5)

        self.export_errors_button = Button(progress_frame, text="Экспорт ошибок", command=self._export_errors)
        self.export_errors_button.pack(side=RIGHT, padx=5, pady=5)
        self.export_errors_button.pack_forget()

    def _export_errors(self):
        if self.stats and self.stats.failed > 0:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"./logs/errors_{timestamp}.csv"
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, 'w', encoding='utf-8-sig') as f:  # Добавляем BOM для корректного отображения в Excel
                f.write("Адрес,Ошибка\n")
                for addr, error in self.stats.failed_addresses:
                    f.write(f'"{addr}","{error}"\n')
            messagebox.showinfo("Экспорт завершен", f"Ошибки сохранены в файл: {output_path}")


class ExcelFileFrame(ProcessFrame):
    """ Форма заполнения для обработки адресов в excel-файле. """


    def __init__(self, parent, process_handler, logger):
        super().__init__(parent, process_handler, logger)
        self.initUI()


    def validate(self) -> bool:
        input_path = self.path_entry.get().strip()
        if input_path == '' or not os.path.exists(input_path):
            return False
        
        sheet = self.sheet_entry.get().strip()
        if sheet == '':
            return False

        addr = self.address_entry.get().strip()
        if addr == '':
            return False

        output_path = self.output_path_entry.get().strip()
        if output_path == '':
            return False

        return True


    def initUI(self):
        """ Производит инициализацию интерфейса. """

        self.pack(fill=BOTH, expand=True)

        # Данные о входном файле
        input_frame = LabelFrame(self, text="Данные о входном файле")
        input_frame.pack(fill=X, padx=5, pady=5, anchor=N)

        self.path_entry = make_field_frame(input_frame, "Путь:")
        self.path_entry.insert(0, './input.xlsx')

        self.sheet_entry = make_field_frame(input_frame, "Название листа:")
        self.sheet_entry.insert(0, 'Sheet 1')

        self.id_entry = make_field_frame(input_frame, "Название ключевого столбца:")
        self.address_entry = make_field_frame(input_frame, "Название столбца с адресом:")
        self.address_entry.insert(0, 'Address')

        # Данные о выходном файле
        output_frame = LabelFrame(self, text='Данные о выходном файле')
        output_frame.pack(fill=X, padx=5, pady=5, anchor=N)

        self.output_path_entry = make_field_frame(output_frame, "Путь:")
        self.output_path_entry.insert(0, './output.xlsx')

        # Кнопки действий
        actions_frame = Frame(self)
        actions_frame.pack(fill=X, padx=5, pady=5, anchor=S, side=BOTTOM)

        process_button = Button(actions_frame, text="Обработать", width=20, command=lambda: self._real_handler())
        process_button.pack(padx=5, pady=5)

        self._create_progress_section()

        # Добавляем выбор режима обработки ошибок
        error_mode_frame = Frame(self)
        error_mode_frame.pack(fill=X, padx=5, pady=5, anchor=N)

        Label(error_mode_frame, text="Режим обработки ошибок:").pack(side=LEFT, padx=5)
        self.error_mode_var = StringVar(value=ErrorHandlingMode.STOP.value)
        ttk.Radiobutton(error_mode_frame, text="Останавливаться", variable=self.error_mode_var, value=ErrorHandlingMode.STOP.value).pack(side=LEFT, padx=5)
        ttk.Radiobutton(error_mode_frame, text="Пропускать", variable=self.error_mode_var, value=ErrorHandlingMode.SKIP.value).pack(side=LEFT, padx=5)


    def _get_args(self) -> dict:
        if not self.validate():
            raise Exception("Введенные данные невалидны. Проверьте их.")
        args = {
            'input_path': self.path_entry.get(),
            'input_sheet': self.sheet_entry.get(),
            'id_name': self.id_entry.get() if self.id_entry.get().strip() != '' else None,
            'address_name': self.address_entry.get(),
            'output_path': self.output_path_entry.get(),
            'error_mode': ErrorHandlingMode(self.error_mode_var.get())
        }
        return args


class DataBaseFrame(ProcessFrame):
    """ Форма заполнения для обработки адресов в БД """


    def __init__(self, parent, process_handler, make_engine, logger, check_connection_handler):
        super().__init__(parent, process_handler, logger)
        self._check_connection_handler = check_connection_handler
        self._make_engine = make_engine
        self.process_button = None
        self.initUI()


    @property
    def check_connection_handler(self):
        return self._check_connection_handler


    @check_connection_handler.setter
    def check_connection_handler(self, _value):
        self._check_connection_handler = _value


    def _validate_connection_params(self) -> tuple[bool, list[str]]:
        """Проверяет параметры подключения к базе данных."""
        errors = []
        
        # Проверка выбора СУБД
        if not self.db_combobox.get():
            errors.append("Не выбрана СУБД")
        
        # Проверка обязательных полей
        required_fields = {
            'user': 'Пользователь',
            'password': 'Пароль',
            'host': 'Хост',
            'port': 'Порт',
            'db_name': 'База данных'
        }
        
        for field, label in required_fields.items():
            value = getattr(self, f'{field}_entry').get()
            if not value:
                errors.append(f"Поле '{label}' не заполнено")
            else:
                # Дополнительные проверки для каждого поля
                if field == 'host':
                    # Проверка формата хоста (IP или доменное имя)
                    if not re.match(r'^[a-zA-Z0-9.-]+$', value):
                        errors.append("Некорректный формат хоста")
                elif field == 'port':
                    try:
                        port_num = int(value)
                        if not (0 < port_num < 65536):
                            errors.append("Порт должен быть числом от 1 до 65535")
                    except ValueError:
                        errors.append("Порт должен быть числом")
                elif field == 'db_name':
                    # Проверка формата имени базы данных
                    if not re.match(r'^[a-zA-Z0-9_]+$', value):
                        errors.append("Некорректный формат имени базы данных")
        
        return len(errors) == 0, errors


    def _validate_table_params(self) -> tuple[bool, list[str]]:
        """Проверяет параметры таблиц."""
        errors = []
        
        # Проверка входной таблицы
        input_table = self.input_table_name_entry.get()
        if not input_table:
            errors.append("Не указано имя входной таблицы")
        elif not re.match(r'^[a-zA-Z0-9_]+$', input_table):
            errors.append("Некорректный формат имени входной таблицы")
        
        # Проверка поля адреса
        address_field = self.address_entry.get()
        if not address_field:
            errors.append("Не указано имя поля адреса")
        elif not re.match(r'^[a-zA-Z0-9_]+$', address_field):
            errors.append("Некорректный формат имени поля адреса")
        
        # Проверка выходной таблицы
        output_table = self.output_table_entry.get()
        if not output_table:
            errors.append("Не указано имя выходной таблицы")
        elif not re.match(r'^[a-zA-Z0-9_]+$', output_table):
            errors.append("Некорректный формат имени выходной таблицы")
        else:
            try:
                # Проверяем только если все параметры подключения заполнены
                if all([
                    self.host_entry.get(),
                    self.port_entry.get(),
                    self.user_entry.get(),
                    self.password_entry.get(),
                    self.db_name_entry.get()
                ]):
                    args = self._get_args()
                    engine = self._make_engine(**args)
                    metadata = MetaData()
                    
                    # Проверяем существование входной таблицы
                    try:
                        input_table_obj = Table(input_table, metadata, autoload_with=engine)
                        # Проверяем существование поля адреса
                        if address_field not in input_table_obj.columns:
                            errors.append(f"Поле '{address_field}' не найдено в таблице '{input_table}'")
                    except Exception as e:
                        errors.append(f"Таблица '{input_table}' не найдена в базе данных")
                        return len(errors) == 0, errors
                    
                    # Проверяем существование выходной таблицы
                    try:
                        Table(output_table, metadata, autoload_with=engine)
                        errors.append(f"Таблица '{output_table}' уже существует в базе данных")
                    except Exception:
                        pass
            except Exception as e:
                errors.append(f"Параметры подключения введены неверно, проверьте соответствие данных пользователя, сервера и БД: {str(e)}")
        
        return len(errors) == 0, errors


    def _real_conn_check(self):
        """Проверяет подключение к базе данных и параметры таблиц."""
        print("Начало проверки подключения")  # Отладочная информация
        
        # Проверка параметров подключения
        conn_valid, conn_errors = self._validate_connection_params()
        if not conn_valid:
            print(f"Ошибки в параметрах подключения: {conn_errors}")  # Отладочная информация
            messagebox.showerror("Ошибка!", "\n".join(conn_errors))
            self.process_button.config(state=DISABLED)
            return
        
        # Проверка параметров таблиц
        table_valid, table_errors = self._validate_table_params()
        if not table_valid:
            print(f"Ошибки в параметрах таблиц: {table_errors}")  
            messagebox.showerror("Ошибка!", "\n".join(table_errors))
            self.process_button.config(state=DISABLED)
            return
        
        # Если все проверки пройдены, проверяем подключение
        try:
            print(f"check_connection_handler: {self._check_connection_handler}")  
            if self._check_connection_handler:
                print("Вызов check_connection_handler")  
                self._check_connection_handler(**self._get_args())
                print("check_connection_handler выполнен успешно") 
            else:
                print("check_connection_handler не установлен")  
                messagebox.showerror("Ошибка!", "Обработчик проверки подключения не установлен")
                return
                
            messagebox.showinfo("Успех!", "Подключение к базе данных успешно установлено")
            self.process_button.config(state=NORMAL)
        except Exception as e:
            print(f"Ошибка при проверке подключения: {str(e)}")  
            messagebox.showerror("Ошибка!", f"Не удалось подключиться к базе данных: {str(e)}")
            self.process_button.config(state=DISABLED)


    def initUI(self):
        """ Производит инициализацию интерфейса. """

        self.pack(fill=BOTH, expand=True)

        # Выбор СУБД
        db_choice_frame = Frame(self)
        db_choice_frame.pack(fill=X, padx=5, pady=5, anchor=N)

        db_choice_label = Label(db_choice_frame, text="СУБД:")
        db_choice_label.pack(side=LEFT, padx=5, pady=5)

        self.db_combobox = Combobox(db_choice_frame, values=["Oracle", "PostgreSQL", "MSSQL Server"], state='readonly') 
        self.db_combobox.pack(fill=X, padx=5, expand=True)

        # Данные подключения
        connection_frame = LabelFrame(self, text="Данные подключения")
        connection_frame.pack(fill=X, padx=5, pady=5, anchor=N)

        self.user_entry = make_field_frame(connection_frame, "Пользователь:")
        self.password_entry = make_field_frame(connection_frame, "Пароль:")
        self.host_entry = make_field_frame(connection_frame, "Хост:")
        self.port_entry = make_field_frame(connection_frame, "Порт:")
        self.db_name_entry = make_field_frame(connection_frame, "Название БД:")

        # Данные о входной таблице в БД
        input_table_frame = LabelFrame(self, text='Данные о входной таблице')
        input_table_frame.pack(fill=X, padx=5, pady=5, anchor=N)

        self.input_table_name_entry = make_field_frame(input_table_frame, "Название исходной таблицы:")
        self.id_entry = make_field_frame(input_table_frame, "Название ключевого поля:")
        self.address_entry = make_field_frame(input_table_frame, "Название поля адреса:")

        # Данные о выходной таблице в БД
        output_table_frame = LabelFrame(self, text='Данные о выходной таблице')
        output_table_frame.pack(fill=X, padx=5, pady=5, anchor=N)

        self.output_table_entry = make_field_frame(output_table_frame, "Название таблицы для результатов:")

        # Кнопки действий
        actions_frame = Frame(self)
        actions_frame.pack(fill=X, padx=5, pady=5, anchor=S, side=BOTTOM)

        check_button = Button(actions_frame, text="Проверить подключение", command=self._real_conn_check)
        check_button.pack(side=LEFT, padx=5, pady=5)

        self.process_button = Button(actions_frame, text="Обработать", width=20, command=self._real_handler, state=DISABLED)
        self.process_button.pack(padx=5, pady=5)

        self._create_progress_section()

        # Добавляем выбор режима обработки ошибок
        error_mode_frame = Frame(self)
        error_mode_frame.pack(fill=X, padx=5, pady=5, anchor=N)

        Label(error_mode_frame, text="Режим обработки ошибок:").pack(side=LEFT, padx=5)
        self.error_mode_var = StringVar(value=ErrorHandlingMode.STOP.value)
        ttk.Radiobutton(error_mode_frame, text="Останавливаться", variable=self.error_mode_var, value=ErrorHandlingMode.STOP.value).pack(side=LEFT, padx=5)
        ttk.Radiobutton(error_mode_frame, text="Пропускать", variable=self.error_mode_var, value=ErrorHandlingMode.SKIP.value).pack(side=LEFT, padx=5)

    
    def _get_args(self) -> dict:
        """Возвращает все параметры, введенные пользователем в интерфейсе."""
        args = {
            'dbms': self.db_combobox.get(),
            'user': self.user_entry.get(),
            'password': self.password_entry.get(),
            'host': self.host_entry.get(),
            'port': self.port_entry.get(),
            'db_name': self.db_name_entry.get(),
            'input_table_name': self.input_table_name_entry.get(),
            'id_column': self.id_entry.get().strip() if self.id_entry.get().strip() != '' else None,
            'address_column': self.address_entry.get(),
            'output_table_name': self.output_table_entry.get(),
            'error_mode': ErrorHandlingMode(self.error_mode_var.get())
        }
        return args

    def validate(self) -> bool:
        """Проверяет все параметры перед обработкой."""
        conn_valid, conn_errors = self._validate_connection_params()
        if not conn_valid:
            messagebox.showerror("Ошибка!", "\n".join(conn_errors))
            return False
            
        table_valid, table_errors = self._validate_table_params()
        if not table_valid:
            messagebox.showerror("Ошибка!", "\n".join(table_errors))
            return False
            
        return True

class TextLogger:
    def __init__(self, text_widget):
        self.text_widget = text_widget
        
    def write(self, text):
        self.text_widget.insert(END, text)
        self.text_widget.see(END)
        self.text_widget.update_idletasks()
        
    def flush(self):
        pass

class MainWindow(Tk):
    """ Главное окно программы. """


    def __init__(self, logger: LoggersCollection, process_excel, process_db, make_engine, check_connection_handler):
        super().__init__()
        self.logger = logger
        self.process_excel = process_excel
        self.process_db = process_db
        self.make_engine = make_engine
        self.check_connection_handler = check_connection_handler
        
        self.title("Обработка адресов")
        self.geometry("800x600")
        
        # Создаем панель вкладок
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill=BOTH, expand=True, padx=5, pady=5)
        
        # Создаем фреймы для вкладок
        self.excel_frame = ExcelFileFrame(self.notebook, self.process_excel, self.logger)
        self.db_frame = DataBaseFrame(self.notebook, self.process_db, self.make_engine, self.logger, self.check_connection_handler)
        self.errors_frame = ErrorsFrame(self.notebook, self.logger)
        
        # Добавляем вкладки
        self.notebook.add(self.excel_frame, text="Excel")
        self.notebook.add(self.db_frame, text="База данных")
        self.notebook.add(self.errors_frame, text="Ошибки")
        
        # Создаем фрейм для логов
        self.log_frame = Frame(self)
        self.log_frame.pack(fill=X, padx=5, pady=5)
        
        # Добавляем кнопку для изменения размера
        self.resize_button = Button(self.log_frame, text="▼", command=self.toggle_log_size)
        self.resize_button.pack(side=RIGHT, padx=5)
        
        # Создаем текстовое поле для логов с прокруткой
        self.log_text = Text(self.log_frame, height=5, wrap=WORD)
        self.log_scroll = Scrollbar(self.log_frame, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=self.log_scroll.set)
        
        self.log_scroll.pack(side=RIGHT, fill=Y)
        self.log_text.pack(side=LEFT, fill=BOTH, expand=True)
        
        # Создаем и добавляем логгер
        self.text_logger = TextLogger(self.log_text)
        self.logger.append(self.text_logger)
        
        # Флаг для отслеживания размера логов
        self.log_expanded = False
        
    def clear_logs(self):
        self.log_text.delete(1.0, END)
        
    def toggle_log_size(self):
        if self.log_expanded:
            self.log_text.configure(height=5)
            self.resize_button.configure(text="▼")
        else:
            self.log_text.configure(height=15)
            self.resize_button.configure(text="▲")
        self.log_expanded = not self.log_expanded

class ErrorsFrame(Frame):
    def __init__(self, parent, logger):
        super().__init__(parent)
        self.logger = logger
        
        # Создаем фрейм для выбора файла
        file_frame = Frame(self)
        file_frame.pack(fill=X, padx=5, pady=5)
        
        Label(file_frame, text="Файл с ошибками:").pack(side=LEFT, padx=5)
        self.file_path = StringVar()
        Entry(file_frame, textvariable=self.file_path, state='readonly').pack(side=LEFT, fill=X, expand=True, padx=5)
        Button(file_frame, text="Обзор", command=self.browse_file).pack(side=LEFT, padx=5)
        
        # Создаем таблицу для отображения ошибок
        self.tree = ttk.Treeview(self, columns=("address", "error"), show="headings")
        self.tree.heading("address", text="Адрес")
        self.tree.heading("error", text="Ошибка")
        self.tree.column("address", width=300)
        self.tree.column("error", width=400)
        
        # Добавляем прокрутку
        scrollbar = Scrollbar(self, orient=VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        scrollbar.pack(side=RIGHT, fill=Y)
        self.tree.pack(fill=BOTH, expand=True, padx=5, pady=5)
        
    def browse_file(self):
        file_path = filedialog.askopenfilename(
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if file_path:
            self.file_path.set(file_path)
            self.load_errors(file_path)
            
    def load_errors(self, file_path):
        try:
            # Очищаем таблицу
            for item in self.tree.get_children():
                self.tree.delete(item)
                
            # Читаем CSV файл
            with open(file_path, 'r', encoding='utf-8') as f:
                # Пропускаем заголовок
                next(f)
                for line in f:
                    # Разбираем строку CSV
                    address, error = line.strip().split(',', 1)
                    # Убираем кавычки
                    address = address.strip('"')
                    error = error.strip('"')
                    # Добавляем в таблицу
                    self.tree.insert("", END, values=(address, error))
        except Exception as e:
            self.logger.write(f"Ошибка при загрузке файла: {str(e)}\n")


def make_gui(logger: LoggersCollection, excel_handler, db_handler, make_engine, check_connection_handler) -> MainWindow:
    window = MainWindow(logger, excel_handler, db_handler, make_engine, check_connection_handler)
    window.geometry("674x600")
    window.minsize(400, 275)
    return window


        