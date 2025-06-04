from abc import abstractmethod
import os
from datetime import datetime
import pandas as pd
import threading
from sqlalchemy import func, select, MetaData, Table, inspect, text
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

from .db_connection_manager import DBConnectionManager, ConnectionParams
from src.exceptions_manager import ExceptionsManager

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
                    # Проверяем доступность схемы перед созданием таблицы
                    engine = make_engine(**args)
                    schema = args.get('schema')
                    
                    # Проверка схемы
                    inspector = inspect(engine)
                    schemas = inspector.get_schema_names()
                    print(f"Доступные схемы в БД: {schemas}")
                    
                    if schema and schema not in schemas:
                        raise Exception(f"Схема '{schema}' не найдена в базе данных. Доступные схемы: {schemas}")
                    
                    # Используем схему при создании метаданных
                    metadata = MetaData(schema=schema)
                    input_table_name = args.get('input_table_name')
                    
                    # Проверяем существование таблицы
                    tables = inspector.get_table_names(schema=schema)
                    if input_table_name.lower() not in [t.lower() for t in tables]:
                        raise Exception(f"Таблица '{input_table_name}' не найдена в схеме '{schema}'. Доступные таблицы: {tables}")
                    
                    # Находим точное имя таблицы с учетом регистра
                    actual_table_name = None
                    for table in tables:
                        if table.lower() == input_table_name.lower():
                            actual_table_name = table
                            break
                    
                    table = Table(actual_table_name, metadata, autoload_with=engine)
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
            
            # Добавляем exceptions_manager в аргументы
            args['exceptions_manager'] = self.master.master.exceptions_manager
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
        if hasattr(self.master.master, 'exceptions_manager'):
            self.master.master.exceptions_manager._load_exceptions()

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
            if hasattr(self.master.master, 'exceptions_manager'):
                self.master.master.exceptions_manager._load_exceptions()


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
            'identity_column_name': self.id_entry.get() if self.id_entry.get().strip() != '' else None,
            'address_name': self.address_entry.get(),
            'output_path': self.output_path_entry.get(),
            'error_mode': ErrorHandlingMode(self.error_mode_var.get())
        }
        return args


class AutocompleteEntry(Entry):
    def __init__(self, parent, values=None, **kwargs):
        super().__init__(parent, **kwargs)
        self.values = values or []
        self._hits = []
        self._hit_index = 0
        self.position = 0
        
        self.bind('<KeyRelease>', self._on_keyrelease)
        self.bind('<KeyPress>', self._on_keypress)
        
        # Создаем всплывающее окно
        self.popup = Toplevel(self)
        self.popup.withdraw()
        self.popup.overrideredirect(True)
        
        # Создаем список для отображения подсказок
        self.listbox = Listbox(self.popup, width=40, height=5)
        self.listbox.pack()
        self.listbox.bind('<Button-1>', self._on_select)
        
    def _on_keyrelease(self, event):
        if event.keysym in ('Up', 'Down', 'Return', 'Escape'):
            return
            
        value = self.get()
        if value == '':
            self._hits = []
            self.popup.withdraw()
            return
            
        # Ищем совпадения
        self._hits = [x for x in self.values if x.lower().startswith(value.lower())]
        
        if self._hits:
            self._show_popup()
        else:
            self.popup.withdraw()
            
    def _on_keypress(self, event):
        if event.keysym == 'Up':
            if self._hits:
                self._hit_index = (self._hit_index - 1) % len(self._hits)
                self.listbox.selection_clear(0, END)
                self.listbox.selection_set(self._hit_index)
                self.listbox.see(self._hit_index)
        elif event.keysym == 'Down':
            if self._hits:
                self._hit_index = (self._hit_index + 1) % len(self._hits)
                self.listbox.selection_clear(0, END)
                self.listbox.selection_set(self._hit_index)
                self.listbox.see(self._hit_index)
        elif event.keysym == 'Return':
            if self._hits:
                self.delete(0, END)
                self.insert(0, self._hits[self._hit_index])
                self.popup.withdraw()
        elif event.keysym == 'Escape':
            self.popup.withdraw()
            
    def _show_popup(self):
        # Обновляем список
        self.listbox.delete(0, END)
        for item in self._hits:
            self.listbox.insert(END, item)
            
        # Позиционируем всплывающее окно
        x = self.winfo_rootx()
        y = self.winfo_rooty() + self.winfo_height()
        self.popup.geometry(f'+{x}+{y}')
        self.popup.deiconify()
        
    def _on_select(self, event):
        if self.listbox.curselection():
            index = self.listbox.curselection()[0]
            value = self.listbox.get(index)
            self.delete(0, END)
            self.insert(0, value)
            self.popup.withdraw()
            
    def set_values(self, values):
        """Обновляет список значений для автодополнения."""
        self.values = values


class DataBaseFrame(ProcessFrame):
    """ Форма заполнения для обработки адресов в БД """


    def __init__(self, parent, process_handler, logger):
        super().__init__(parent, process_handler, logger)
        self.connection_manager = DBConnectionManager()
        self.initUI()

    def _on_user_change(self, event=None):
        """Обработчик изменения поля пользователя."""
        user = self.user_entry.get()
        if user:
            # Получаем последние использованные параметры для пользователя
            params = self.connection_manager.get_connection_params(user)
            if params:
                # Обновляем списки автодополнения
                hosts = list(set(p.host for p in params))  # Убираем дубликаты
                db_names = list(set(p.db_name for p in params))  # Убираем дубликаты
                self.host_entry.set_values(hosts)
                self.db_name_entry.set_values(db_names)
                
                # Устанавливаем последние использованные параметры
                last_params = params[-1]
                self.host_entry.delete(0, END)
                self.host_entry.insert(0, last_params.host)
                self.port_combobox.set(last_params.port)
                self.db_name_entry.delete(0, END)
                self.db_name_entry.insert(0, last_params.db_name)
                self.schema_combobox.set(last_params.schema)

    def _on_host_change(self, event=None):
        """Обработчик изменения поля хоста."""
        host = self.host_entry.get()
        if host:
            # Получаем список портов для хоста
            ports = self.connection_manager.get_host_ports(host)
            if ports:
                self.port_combobox['values'] = list(set(ports))  # Убираем дубликаты
                self.port_combobox.set(ports[0])  # Устанавливаем первый порт

    def _on_db_name_change(self, event=None):
        """Обработчик изменения поля имени БД."""
        db_name = self.db_name_entry.get()
        if db_name:
            # Получаем список схем для БД
            schemas = self.connection_manager.get_db_schemas(db_name)
            if schemas:
                self.schema_combobox['values'] = list(set(schemas))  # Убираем дубликаты
                self.schema_combobox.set(schemas[0])  # Устанавливаем первую схему

    def _save_connection_params(self):
        """Сохраняет параметры подключения."""
        try:
            params = ConnectionParams(
                dbms=self.db_combobox.get(),
                user=self.user_entry.get(),
                password_hash="",  # Будет установлено в save_connection
                host=self.host_entry.get(),
                port=self.port_combobox.get(),
                db_name=self.db_name_entry.get(),
                schema=self.schema_combobox.get()
            )
            self.connection_manager.save_connection(params, self.password_entry.get())
            
            # Обновляем списки автодополнения
            user_params = self.connection_manager.get_connection_params(params.user)
            if user_params:
                hosts = list(set(p.host for p in user_params))
                db_names = list(set(p.db_name for p in user_params))
                self.host_entry.set_values(hosts)
                self.db_name_entry.set_values(db_names)
            
            messagebox.showinfo("Успех!", "Параметры подключения успешно сохранены")
            
        except Exception as e:
            messagebox.showerror("Ошибка!", f"Не удалось сохранить параметры подключения: {str(e)}")

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
            if field == 'port':
                value = self.port_combobox.get()
            elif field == 'schema':
                value = self.schema_combobox.get()
            else:
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
        
        # Проверка схемы БД
        schema = self.schema_combobox.get()
        if not schema:
            errors.append("Не указана схема БД")
        elif not re.match(r'^[a-zA-Z0-9_]+$', schema):
            errors.append("Некорректный формат имени схемы БД")
        
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
                    self.port_combobox.get(),
                    self.user_entry.get(),
                    self.password_entry.get(),
                    self.db_name_entry.get(),
                    self.schema_combobox.get()
                ]):
                    args = self._get_args()
                    engine = make_engine(**args)
                    
                    # Сначала проверим наличие схемы в БД
                    try:
                        inspector = inspect(engine)
                        schemas = inspector.get_schema_names()
                        print(f"Доступные схемы в БД: {schemas}")
                        
                        if schema not in schemas:
                            errors.append(f"Схема '{schema}' не найдена в базе данных. Доступные схемы: {schemas}")
                            return len(errors) == 0, errors
                    except Exception as e:
                        errors.append(f"Ошибка при проверке схемы: {str(e)}")
                        return len(errors) == 0, errors
                    
                    # Если схема существует, проверяем таблицы
                    metadata = MetaData(schema=schema)
                    
                    # Проверяем существование входной таблицы
                    try:
                        input_table_obj = Table(input_table, metadata, autoload_with=engine)
                        # Проверяем существование поля адреса
                        if address_field not in input_table_obj.columns:
                            errors.append(f"Поле '{address_field}' не найдено в таблице '{schema}.{input_table}'")
                    except Exception as e:
                        errors.append(f"Таблица '{schema}.{input_table}' не найдена в базе данных. Ошибка: {str(e)}")
                        return len(errors) == 0, errors
            except Exception as e:
                errors.append(f"Параметры подключения введены неверно, проверьте соответствие данных пользователя, сервера и БД: {str(e)}")
        
        return len(errors) == 0, errors


    def _real_conn_check(self):
        """Проверяет подключение к базе данных и параметры таблиц."""
        print("\n===== НАЧАЛО ПРОВЕРКИ ПОДКЛЮЧЕНИЯ =====")
        print(f"DBMS: {self.db_combobox.get()}")
        print(f"User: {self.user_entry.get()}")
        print(f"Host: {self.host_entry.get()}")
        print(f"Port: {self.port_combobox.get()}")
        print(f"DB Name: {self.db_name_entry.get()}")
        print(f"Schema: {self.schema_combobox.get()}")
        
        # Проверка параметров подключения
        conn_valid, conn_errors = self._validate_connection_params()
        if not conn_valid:
            print(f"Ошибки в параметрах подключения: {conn_errors}")
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
            print("\nПроверка подключения к БД...")
            args = self._get_args()
            print(f"Параметры подключения: {args}")
            
            engine = make_engine(**args)
            print("Движок БД создан успешно")
            
            # Проверяем подключение
            with engine.connect() as conn:
                print("Соединение установлено, выполняется тестовый запрос...")
                conn.execute(text("SELECT 1"))
                print("Тестовый запрос выполнен успешно")
            
            print("Подключение успешно установлено")
            messagebox.showinfo("Успех!", "Подключение к базе данных успешно установлено")
            self.process_button.config(state=NORMAL)
            
        except Exception as e:
            print(f"Ошибка при проверке подключения: {str(e)}")  
            print(f"Тип ошибки: {type(e)}")
            import traceback
            print(f"Трассировка: {traceback.format_exc()}")
            messagebox.showerror("Ошибка!", f"Не удалось подключиться к базе данных: {str(e)}")
            self.process_button.config(state=DISABLED)
        
        print("===== КОНЕЦ ПРОВЕРКИ ПОДКЛЮЧЕНИЯ =====\n")


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
        self.db_combobox.set("PostgreSQL")

        # Данные подключения
        connection_frame = LabelFrame(self, text="Данные подключения")
        connection_frame.pack(fill=X, padx=5, pady=5, anchor=N)

        # Пользователь
        user_frame = Frame(connection_frame)
        user_frame.pack(fill=X)
        Label(user_frame, text="Пользователь:", width=35).pack(side=LEFT, padx=5, pady=5)
        self.user_entry = AutocompleteEntry(user_frame)
        self.user_entry.pack(fill=X, padx=5, expand=True)
        self.user_entry.insert(0, 'postgres')
        self.user_entry.bind('<FocusOut>', self._on_user_change)

        # Пароль
        self.password_entry = make_field_frame(connection_frame, "Пароль:")
        self.password_entry.insert(0, '1234')

        # Хост
        host_frame = Frame(connection_frame)
        host_frame.pack(fill=X)
        Label(host_frame, text="Хост:", width=35).pack(side=LEFT, padx=5, pady=5)
        self.host_entry = AutocompleteEntry(host_frame)
        self.host_entry.pack(fill=X, padx=5, expand=True)
        self.host_entry.insert(0, 'localhost')
        self.host_entry.bind('<FocusOut>', self._on_host_change)

        # Порт
        port_frame = Frame(connection_frame)
        port_frame.pack(fill=X)
        Label(port_frame, text="Порт:", width=35).pack(side=LEFT, padx=5, pady=5)
        self.port_combobox = Combobox(port_frame)
        self.port_combobox.pack(fill=X, padx=5, expand=True)
        self.port_combobox.insert(0, '5432')

        # Имя БД
        db_name_frame = Frame(connection_frame)
        db_name_frame.pack(fill=X)
        Label(db_name_frame, text="Название БД:", width=35).pack(side=LEFT, padx=5, pady=5)
        self.db_name_entry = AutocompleteEntry(db_name_frame)
        self.db_name_entry.pack(fill=X, padx=5, expand=True)
        self.db_name_entry.insert(0, 'postgres')
        self.db_name_entry.bind('<FocusOut>', self._on_db_name_change)

        # Схема
        schema_frame = Frame(connection_frame)
        schema_frame.pack(fill=X)
        Label(schema_frame, text="Схема БД:", width=35).pack(side=LEFT, padx=5, pady=5)
        self.schema_combobox = Combobox(schema_frame)
        self.schema_combobox.pack(fill=X, padx=5, expand=True)
        self.schema_combobox.insert(0, 'public')

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
        self.output_table_entry.insert(0, 'log_address_recognition')

        # Кнопки действий
        actions_frame = Frame(self)
        actions_frame.pack(fill=X, padx=5, pady=5, anchor=S, side=BOTTOM)

        check_button = Button(actions_frame, text="Проверить подключение", command=self._real_conn_check)
        check_button.pack(side=LEFT, padx=5, pady=5)

        save_params_button = Button(actions_frame, text="Сохранить параметры", command=self._save_connection_params)
        save_params_button.pack(side=LEFT, padx=5, pady=5)

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
            'port': self.port_combobox.get(),
            'db_name': self.db_name_entry.get(),
            'schema': self.schema_combobox.get(),
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

class ExceptionsFrame(Frame):
    """Фрейм для работы с файлом исключений адресов."""
    
    def __init__(self, parent, logger):
        super().__init__(parent)
        self.logger = logger
        self.exceptions_file = "Exceptions.xlsx"
        self.exceptions = {}  # {неправильный_адрес: (правильный_адрес, ключ)}
        self.initUI()
        
    def initUI(self):
        """Инициализация интерфейса."""
        self.pack(fill=BOTH, expand=True)
        
        # Создаем таблицу для отображения исключений
        self.tree = ttk.Treeview(self, columns=("address", "correct_address", "key"), show="headings")
        self.tree.heading("address", text="Неправильный адрес")
        self.tree.heading("correct_address", text="Правильный адрес")
        self.tree.heading("key", text="Ключ")
        self.tree.column("address", width=300)
        self.tree.column("correct_address", width=300)
        self.tree.column("key", width=100)
        
        # Добавляем прокрутку
        scrollbar = Scrollbar(self, orient=VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        scrollbar.pack(side=RIGHT, fill=Y)
        self.tree.pack(fill=BOTH, expand=True, padx=5, pady=5)
        
        # Кнопки действий
        actions_frame = Frame(self)
        actions_frame.pack(fill=X, padx=5, pady=5)
        
        Button(actions_frame, text="Добавить", command=self._add_exception).pack(side=LEFT, padx=5)
        Button(actions_frame, text="Редактировать", command=self._edit_exception).pack(side=LEFT, padx=5)
        Button(actions_frame, text="Удалить", command=self._delete_exception).pack(side=LEFT, padx=5)
        Button(actions_frame, text="Сохранить", command=self._save_exceptions).pack(side=LEFT, padx=5)
        
        # Загружаем данные
        self._load_exceptions()
        
    def _load_exceptions(self):
        """Загружает исключения из файла."""
        try:
            if os.path.exists(self.exceptions_file):
                print(f"Загрузка исключений из файла: {self.exceptions_file}")
                df = pd.read_excel(self.exceptions_file)
                print(f"Содержимое файла исключений:\n{df}")
                
                # Очищаем таблицу
                for item in self.tree.get_children():
                    self.tree.delete(item)
                
                # Загружаем данные в таблицу и словарь
                self.exceptions = {}
                for _, row in df.iterrows():
                    address = row['address']
                    correct_address = row['correct_address']
                    key = row['key']
                    self.exceptions[address] = (correct_address, key)
                    self.tree.insert("", END, values=(address, correct_address, key))
                
                print(f"Загруженные исключения: {self.exceptions}")
            else:
                print(f"Файл исключений не найден: {self.exceptions_file}")
        except Exception as e:
            print(f"Ошибка при загрузке файла исключений: {str(e)}")
            
    def _save_exceptions(self):
        """Сохраняет исключения в файл."""
        try:
            # Обновляем словарь исключений из таблицы
            self.exceptions = {}
            for item in self.tree.get_children():
                values = self.tree.item(item)['values']
                address = values[0]
                correct_address = values[1]
                key = values[2]
                self.exceptions[address] = (correct_address, key)
            
            # Создаем DataFrame и сохраняем в файл
            data = []
            for address, (correct_address, key) in self.exceptions.items():
                data.append({
                    'address': address,
                    'correct_address': correct_address,
                    'key': key
                })
            
            df = pd.DataFrame(data)
            df.to_excel(self.exceptions_file, index=False)
            messagebox.showinfo("Успех!", "Файл исключений успешно сохранен")
            if hasattr(self.master.master, 'exceptions_manager'):
                self.master.master.exceptions_manager._load_exceptions()
        except Exception as e:
            messagebox.showerror("Ошибка!", f"Не удалось сохранить файл исключений: {str(e)}")
            
    def _add_exception(self):
        """Добавляет новое исключение."""
        dialog = ExceptionDialog(self, "Добавить исключение")
        if dialog.result:
            address, correct_address, key = dialog.result
            self.exceptions[address] = (correct_address, key)
            self.tree.insert("", END, values=dialog.result)
            
    def _edit_exception(self):
        """Редактирует выбранное исключение."""
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("Предупреждение", "Выберите исключение для редактирования")
            return
            
        item = selected[0]
        values = self.tree.item(item)['values']
        old_address = values[0]
        
        dialog = ExceptionDialog(self, "Редактировать исключение", values)
        if dialog.result:
            new_address, new_correct_address, new_key = dialog.result
            
            # Удаляем старое исключение и добавляем новое
            if old_address in self.exceptions:
                del self.exceptions[old_address]
            self.exceptions[new_address] = (new_correct_address, new_key)
            
            self.tree.item(item, values=dialog.result)
            
    def _delete_exception(self):
        """Удаляет выбранное исключение."""
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("Предупреждение", "Выберите исключение для удаления")
            return
            
        if messagebox.askyesno("Подтверждение", "Удалить выбранное исключение?"):
            item = selected[0]
            values = self.tree.item(item)['values']
            address = values[0]
            
            # Удаляем из словаря и таблицы
            if address in self.exceptions:
                del self.exceptions[address]
            self.tree.delete(item)


class ExceptionDialog(Toplevel):
    """Диалог для добавления/редактирования исключения."""
    
    def __init__(self, parent, title, values=None):
        super().__init__(parent)
        self.title(title)
        self.result = None
        
        # Создаем поля ввода
        frame = Frame(self)
        frame.pack(fill=X, padx=5, pady=5)
        
        Label(frame, text="Неправильный адрес:").pack(side=LEFT, padx=5)
        self.address_entry = Entry(frame, width=50)
        self.address_entry.pack(side=LEFT, padx=5)
        
        frame2 = Frame(self)
        frame2.pack(fill=X, padx=5, pady=5)
        
        Label(frame2, text="Правильный адрес:").pack(side=LEFT, padx=5)
        self.correct_address_entry = Entry(frame2, width=50)
        self.correct_address_entry.pack(side=LEFT, padx=5)
        
        frame3 = Frame(self)
        frame3.pack(fill=X, padx=5, pady=5)
        
        Label(frame3, text="Ключ:").pack(side=LEFT, padx=5)
        self.key_entry = Entry(frame3, width=10)
        self.key_entry.pack(side=LEFT, padx=5)
        
        # Заполняем поля, если редактируем
        if values:
            self.address_entry.insert(0, values[0])
            self.correct_address_entry.insert(0, values[1])
            self.key_entry.insert(0, values[2])
        
        # Кнопки
        buttons_frame = Frame(self)
        buttons_frame.pack(fill=X, padx=5, pady=5)
        
        Button(buttons_frame, text="OK", command=self._on_ok).pack(side=LEFT, padx=5)
        Button(buttons_frame, text="Отмена", command=self._on_cancel).pack(side=LEFT, padx=5)
        
        # Центрируем окно
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f'{width}x{height}+{x}+{y}')
        
        # Делаем окно модальным
        self.transient(parent)
        self.grab_set()
        self.wait_window()
        
    def _on_ok(self):
        """Обработчик нажатия кнопки OK."""
        address = self.address_entry.get().strip()
        correct_address = self.correct_address_entry.get().strip()
        key = self.key_entry.get().strip()
        
        if not address:
            messagebox.showwarning("Предупреждение", "Введите неправильный адрес")
            return
            
        if not correct_address:
            messagebox.showwarning("Предупреждение", "Введите правильный адрес")
            return
            
        if not key:
            messagebox.showwarning("Предупреждение", "Введите ключ")
            return
            
        try:
            key = int(key)
        except ValueError:
            messagebox.showwarning("Предупреждение", "Ключ должен быть числом")
            return
            
        self.result = (address, correct_address, key)
        self.destroy()
        
    def _on_cancel(self):
        """Обработчик нажатия кнопки Отмена."""
        self.destroy()

class ExceptionsManager:
    """Менеджер для работы с исключениями адресов."""
    
    def __init__(self, exceptions_file="Exceptions.xlsx"):
        self.exceptions_file = exceptions_file
        self.exceptions = {}  # {неправильный_адрес: (правильный_адрес, ключ)}
        self._load_exceptions()
        
    def _normalize_address(self, address: str) -> str:
        """Нормализует адрес для сравнения.
        
        Args:
            address (str): Исходный адрес.
            
        Returns:
            str: Нормализованный адрес.
        """
        if not address:
            return ""
        # Приводим к верхнему регистру
        address = address.upper()
        # Заменяем различные варианты написания на стандартные
        replacements = {
            'УЛИЦА': 'УЛ.',
            'УЛ ': 'УЛ.',
            'УЛ.': 'УЛ.',
            'ПРОСПЕКТ': 'ПР-КТ',
            'ПРОСП': 'ПР-КТ',
            'ПР-Т': 'ПР-КТ',
            'ПРОЕЗД': 'ПР-Д',
            'ПР.': 'ПР-Д',
            'БУЛЬВАР': 'Б-Р',
            'БУЛ': 'Б-Р',
            'ПЛОЩАДЬ': 'ПЛ.',
            'ПЛ ': 'ПЛ.',
        }
        for old, new in replacements.items():
            address = address.replace(old, new)
        # Убираем лишние пробелы
        address = ' '.join(address.split())
        return address
        
    def _load_exceptions(self):
        """Загружает исключения из файла."""
        try:
            if os.path.exists(self.exceptions_file):
                print(f"Загрузка исключений из файла: {self.exceptions_file}")
                df = pd.read_excel(self.exceptions_file)
                print(f"Содержимое файла исключений:\n{df}")
                
                # Загружаем данные в словарь с нормализацией адресов
                self.exceptions = {}
                for _, row in df.iterrows():
                    address = self._normalize_address(row['address'])
                    correct_address = row['correct_address']
                    key = row['key']
                    self.exceptions[address] = (correct_address, key)
                
                print(f"Загруженные исключения: {self.exceptions}")
            else:
                print(f"Файл исключений не найден: {self.exceptions_file}")
        except Exception as e:
            print(f"Ошибка при загрузке файла исключений: {str(e)}")
            
    def get_key(self, address: str) -> tuple[int | None, str]:
        """Получает ключ для адреса из исключений.
        
        Args:
            address (str): Адрес для поиска.
            
        Returns:
            tuple[int | None, str]: (ключ, сообщение)
                - Если адрес найден в исключениях, возвращает его ключ и пустое сообщение
                - Если адрес не найден, возвращает None и сообщение "адрес не найден"
                - Если адрес найден в исключениях с ключом None, возвращает None и сообщение "адрес не существует"
        """
        print(f"Поиск адреса в исключениях: {address}")  # Отладочная информация
        print(f"Доступные исключения: {self.exceptions}")  # Отладочная информация
        
        normalized_address = self._normalize_address(address)
        print(f"Нормализованный адрес: {normalized_address}")  # Отладочная информация
        
        if normalized_address in self.exceptions:
            correct_address, key = self.exceptions[normalized_address]
            print(f"Адрес найден в исключениях: correct_address={correct_address}, key={key}")  # Отладочная информация
            if key is None:
                return None, "адрес не существует"
            return key, ""
        print(f"Адрес не найден в исключениях")  # Отладочная информация
        return None, "адрес не найден"
        
    def get_correct_address(self, address: str) -> str | None:
        """Получает правильный адрес для адреса из исключений.
        
        Args:
            address (str): Адрес для поиска.
            
        Returns:
            str | None: Правильный адрес или None, если адрес не найден в исключениях
        """
        normalized_address = self._normalize_address(address)
        if normalized_address in self.exceptions:
            correct_address, _ = self.exceptions[normalized_address]
            return correct_address
        return None

class MainWindow(Tk):
    """ Главное окно программы. """


    def __init__(self, logger: LoggersCollection, process_excel, process_db, make_engine, check_connection_handler):
        super().__init__()
        self.logger = logger
        self.process_excel = process_excel
        self.process_db = process_db
        self.make_engine = make_engine
        self.check_connection_handler = check_connection_handler
        self.exceptions_manager = ExceptionsManager()
        
        self.title("Обработка адресов")
        self.geometry("800x600")
        
        # Создаем панель вкладок
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill=BOTH, expand=True, padx=5, pady=5)
        
        # Создаем фреймы для вкладок
        self.excel_frame = ExcelFileFrame(self.notebook, self.process_excel, self.logger)
        self.db_frame = DataBaseFrame(self.notebook, self.process_db, self.logger)
        self.errors_frame = ErrorsFrame(self.notebook, self.logger)
        self.exceptions_frame = ExceptionsFrame(self.notebook, self.logger)
        
        # Добавляем вкладки
        self.notebook.add(self.excel_frame, text="Excel")
        self.notebook.add(self.db_frame, text="База данных")
        self.notebook.add(self.errors_frame, text="Ошибки")
        self.notebook.add(self.exceptions_frame, text="Исключения")
        
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


        