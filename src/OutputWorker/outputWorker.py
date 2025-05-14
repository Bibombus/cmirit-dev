__all__ = [
    "OutputWorker",
    "SingleTableExcelOutputWorker",
    "AddressDTO",
    "LoggersCollection",
    "GUILogger",
    "DatabaseOutputWorker"
]

from abc import ABC, abstractmethod
from typing import Any, Iterable, Iterator
import sys
from tkinter import Listbox
import time

import pandas as pd
from sqlalchemy import Engine, Integer, create_engine, MetaData, Table, Column, String, text
from sqlalchemy import inspect  # Добавляем импорт для инспекции структуры БД

from ..AddresInfo import Address


class LoggersCollection(list):
    """Позволяет одновременно писать логи в несколько источников. Для добавления нового источника используется стандартный интерфейс списка."""

    def __getattr__(self, attr: str):
        """Получение атрибута по его имени. Используется для выполнения операций записи логов во все места, имеющие единый интерфейс сразу.

        Args:
            attr (str): Название аттрибута (метода).
        """

        def func(*args, **kwargs):
            for obj in self:
                getattr(obj, attr)(*args, **kwargs)

        return func


class GUILogger:

    def __init__(self, listbox: Listbox = None):
        self.widget = listbox


    @property
    def listbox(self) -> Listbox:
        return self.widget
    
    @listbox.setter
    def listbox(self, value):
        self.widget = value

    def write(self, message):
        if self.widget is not None:
            self.widget.insert(0, message)

    def flush(self):
        pass


class AddressDTO:
    """DTO для передачи данных об адресе."""

    def __init__(self, raw: str, address: Address | None = None, key: Any = None, **kwargs):
        """Конструктор.

        Args:
            raw (str): Сырой адрес.
            address (Address | None, optional): Обработанный адрес. Defaults to None.
            key (Any, optional): Ключ адреса. Defaults to None.
            **kwargs: Дополнительные поля, например id или идентификатор записи.
        """
        self.raw = raw
        self.address = address
        self.key = key
        self.note = kwargs.get('note')
        
        # Дополнительные поля для соответствия структуре справочника
        if address:
            self.Name = address.street.name if address.street else None
            self.Type = str(address.street.type) if address.street and address.street.type else None
            self.House = address.house
            self.Flat = address.flat
        else:
            self.Name = None
            self.Type = None
            self.House = None
            self.Flat = None
            
        # Добавляем все дополнительные поля
        for key, value in kwargs.items():
            if key != 'raw' and key != 'address' and key != 'key' and key != 'note':
                setattr(self, key, value)
                print(f"Установлен атрибут '{key}' = {value}")

    def dict(self) -> dict:
        """Преобразует DTO к словарю нужного для вывода формата.

        Returns:
            dict: Словарь в нужном для вывода формате.
        """
        # Начинаем с базовых полей
        data = {
            "Address": self.raw,
            "Name": self.Name,
            "Type": self.Type,
            "House": self.House,
            "Flat": self.Flat,
            "Key": self.key
                }
        
        # Добавляем все дополнительные поля, кроме служебных
        for key, value in self.__dict__.items():
            if key not in ['raw', 'address', 'key', 'note', 'Name', 'Type', 'House', 'Flat']:
                data[key] = value
                
        return data


    def __str__(self) -> str:
        """Строковое представление содердимого объекта.

        Returns:
            str: Строковое представление содердимого объекта.
        """

        result = f"Сырой адрес: {self.raw}"
        if self.address:
            result += f"; Разобранный: {self.address}"

        if self.key is not None:
            result += f"; Ключ: {self.key}"
        if len(self.__dict__) > 0:
            result += f"; Доп. данные: {self.__dict__}"
        
        return result


class OutputWorker(ABC):
    """Выполняет работу по выводу результатов. Абстрактный класс."""

    def __init__(self, logger: LoggersCollection = None):
        """Конструктор.

        Args:
            logger (LoggersCollection, optional): Объект для логирования. По умолчанию будет создан вывод в консоль.
        """

        self.logger = logger if logger is not None else sys.stdout

    @abstractmethod
    def save(self, addresses: Iterable[AddressDTO]):
        """Метод сохранения результатов. Является абстрактным.

        Args:
            addresses (terable[AddressDTO]): Коллекция с адресами и их ключами.
        """

        pass


class SingleTableExcelOutputWorker(OutputWorker):
    """Выводит все результаты в один лист excel."""

    def __init__(self, output_path: str, logger: LoggersCollection):
        """Конструктор.

        Args:
            output_path (str): Путь к файлу для записи.
            logger (LoggersCollection): Объект для логирования.
        """

        super().__init__(logger)
        self.output_path = output_path


    def save(self, addresses: Iterable[AddressDTO]):
        """Сохраняет данные в файл excel в одну таблицу.

        Args:
            addresses (Iterable[AddressDTO]): Коллекция с адресами и их ключами.
        """

        try:
            # Преобразуем данные в список словарей
            rows = []
            for item in addresses:
                row = {
                    'Address': item.raw,  # Сырой адрес
                    'Name': item.Name,    # Имя улицы
                    'Type': item.Type,    # Тип улицы
                    'House': item.House,  # Номер дома
                    'Flat': item.Flat,    # Номер квартиры
                    'Key': item.key,      # Ключ
                    'Note': item.note     # Примечание
                }
                # Добавляем дополнительные поля
                for key, value in item.__dict__.items():
                    if key not in ['raw', 'address', 'key', 'note', 'Name', 'Type', 'House', 'Flat']:
                        row[key] = value
                rows.append(row)
            
            # Создаем DataFrame и сохраняем в Excel
            df = pd.DataFrame(rows)
            df.to_excel(self.output_path, index=False)
        except Exception as e:
            self.logger.write(f"Ошибка при сохранении результатов: {str(e)}\n")
            raise e


class DatabaseOutputWorker(OutputWorker):
    """ Класс для записи результатов в БД. """

    def __init__(self, engine: Engine, input_table_name: str, output_table_name: str, schema: str, id_column: str | None, logger: LoggersCollection):
        super().__init__(logger)
        self.engine = engine
        self.input_table_name = input_table_name
        self.output_table_name = output_table_name
        self.schema = schema
        self.id_column = id_column

    def save(self, addresses: Iterable[AddressDTO]):
        """Сохраняет данные в базу данных.

        Args:
            addresses (Iterable[AddressDTO]): Коллекция с адресами и их ключами.
        """
        try:
            print(f"\n===== НАЧАЛО ПРОЦЕССА СОХРАНЕНИЯ =====")
            print(f"Схема: {self.schema}")
            print(f"Входная таблица: {self.input_table_name}")
            
            # ЭТАП 1: СОХРАНЕНИЕ ДАННЫХ В ВЫХОДНУЮ ТАБЛИЦУ
            # ------------------------------------------
            # Собираем данные для выходной таблицы и информацию для обновления ключей
            output_data = []
            update_data = []
            
            # Преобразуем входной поток в списки с проверкой типов
            print("Обработка входных данных...")
            for item in addresses:
                # Показываем для отладки значение ключа
                print(f"Обработка записи: raw={item.raw}, key={item.key}")
                
                # Данные для выходной таблицы
                output_data.append({
                    'raw_address': item.raw,
                    'street_name': item.Name,
                    'street_type': item.Type,
                    'house': item.House,
                    'flat': item.Flat,
                    'key': item.key
                })
                
                # Данные для обновления входной таблицы
                if item.key is not None and self.id_column is not None:
                    id_value = getattr(item, self.id_column, None)
                    print(f"ID для обновления: {id_value}, колонка: {self.id_column}")
                    print(f"Атрибуты объекта AddressDTO: {item.__dict__}")
                    
                    if id_value is not None:
                        try:
                            # Преобразуем к числовым типам для безопасности
                            update_data.append({
                                'id': int(id_value) if isinstance(id_value, (int, float, str)) else id_value,
                                'key': int(item.key) if isinstance(item.key, (int, float, str)) else item.key
                            })
                            print(f"Добавлены данные для обновления: ID={id_value}, key={item.key}")
                        except (ValueError, TypeError) as e:
                            print(f"Пропуск записи с невалидными данными: ID={id_value}, key={item.key}, ошибка: {str(e)}")
                    else:
                        print(f"ID не найден в объекте AddressDTO для ключа {item.key}")
                elif item.key is not None:
                    print(f"ID колонка не указана, но есть ключ: {item.key}")
                elif self.id_column is not None:
                    print(f"Ключ не определен для записи, id_column={self.id_column}")
            
            print(f"Подготовлено {len(output_data)} записей для выходной таблицы")
            print(f"Подготовлено {len(update_data)} записей для обновления ключей во входной таблице")
            
            # Загружаем метаданные
            print("\nЗагрузка метаданных...")
            metadata = MetaData(schema=self.schema)
            input_table_name = self.input_table_name
            
            # Создаем выходную таблицу
            print("Создание выходной таблицы...")
            output_table = Table(
                self.output_table_name,
                metadata,
                Column('id', Integer, primary_key=True),
                Column('raw_address', String),
                Column('street_name', String),
                Column('street_type', String),
                Column('house', String),
                Column('flat', String),
                Column('key', Integer),
                extend_existing=True
            )
            
            # Создаем/сохраняем таблицу в БД
            try:
                output_table.create(self.engine, checkfirst=True)
                print("Выходная таблица создана или уже существует")
                
                # Сохраняем данные в выходную таблицу
                if output_data:
                    with self.engine.begin() as conn:
                        print(f"Сохранение {len(output_data)} записей в выходную таблицу...")
                        conn.execute(output_table.insert(), output_data)
                    print("Данные успешно сохранены в выходную таблицу")
            except Exception as e:
                print(f"Ошибка при создании/сохранении выходной таблицы: {e}")
                # Продолжаем выполнение для обновления ключей в любом случае
            
            # ЭТАП 2: ОБНОВЛЕНИЕ КЛЮЧЕЙ ВО ВХОДНОЙ ТАБЛИЦЕ
            # ------------------------------------------
            if update_data:
                print("\n===== ЭТАП ОБНОВЛЕНИЯ КЛЮЧЕЙ =====")
                
                # Получаем информацию о структуре входной таблицы
                inspector = inspect(self.engine)
                try:
                    # Проверяем существование таблицы
                    tables = inspector.get_table_names(schema=self.schema)
                    print(f"Доступные таблицы: {tables}")
                    
                    # Проверяем регистр имени таблицы
                    actual_table_name = None
                    for table in tables:
                        if table.lower() == self.input_table_name.lower():
                            actual_table_name = table
                            print(f"Найдена входная таблица: {actual_table_name}")
                            break
                    
                    if not actual_table_name:
                        print(f"ОШИБКА: Таблица {self.input_table_name} не найдена в схеме {self.schema}!")
                        return
                    
                    input_table_name = actual_table_name
                    
                    # Проверяем колонки таблицы
                    columns = inspector.get_columns(input_table_name, schema=self.schema)
                    print(f"Колонки входной таблицы:")
                    column_names = []
                    for col in columns:
                        print(f"  - {col['name']} (тип: {col['type']})")
                        column_names.append(col['name'])
                    
                    # Находим колонку ID с учетом регистра
                    id_column_name = None
                    for col_name in column_names:
                        if col_name.lower() == self.id_column.lower():
                            id_column_name = col_name
                            print(f"Найдена ID колонка: {id_column_name}")
                            break
                    
                    if not id_column_name:
                        print(f"ОШИБКА: Колонка {self.id_column} не найдена в таблице!")
                        return
                    
                    # Проверяем существование колонки для ключей
                    key_column_name = None
                    for col_name in column_names:
                        if col_name.lower() == 'key_street_house':
                            key_column_name = col_name
                            print(f"Найдена колонка ключей: {key_column_name}")
                            break
                    
                    # Если колонки нет, создаем ее
                    if not key_column_name:
                        print("Колонка key_street_house не найдена, создаем...")
                        try:
                            with self.engine.begin() as conn:
                                # Используем регистр как в базе данных
                                sql = f'ALTER TABLE "{self.schema}"."{input_table_name}" ADD COLUMN "key_street_house" INTEGER'
                                conn.execute(text(sql))
                            print("Колонка key_street_house успешно создана")
                            key_column_name = "key_street_house"
                        except Exception as e:
                            print(f"Ошибка при создании колонки: {e}")
                            return
                    
                    # ПРЯМОЕ ОБНОВЛЕНИЕ С ЯВНЫМ УКАЗАНИЕМ СХЕМЫ И ИМЕН КОЛОНОК
                    print("\nНачинаем обновление ключей...")
                    
                    # Используем прямое подключение
                    conn = self.engine.raw_connection()
                    cursor = conn.cursor()
                    
                    # Проверка текущих значений
                    sql_check = f'SELECT COUNT(*) FROM "{self.schema}"."{input_table_name}" WHERE "{key_column_name}" IS NOT NULL'
                    cursor.execute(sql_check)
                    non_null_count = cursor.fetchone()[0]
                    print(f"Текущее количество записей с непустыми ключами: {non_null_count}")
                    
                    # Обновляем записи партиями
                    success_count = 0
                    error_count = 0
                    batch_size = 50
                    current_batch = []
                    
                    print(f"Всего записей для обновления: {len(update_data)}")
                    
                    for i, update in enumerate(update_data):
                        id_val = update['id']
                        key_val = update['key']
                        
                        # Добавляем в текущую партию
                        current_batch.append((key_val, id_val))
                        
                        # Когда партия заполнена или это последняя запись
                        if len(current_batch) >= batch_size or i == len(update_data) - 1:
                            try:
                                # Создаем временную таблицу для пакетного обновления
                                cursor.execute('DROP TABLE IF EXISTS temp_key_updates')
                                cursor.execute('CREATE TEMP TABLE temp_key_updates (key_val INTEGER, id_val INTEGER)')
                                
                                # Вставляем данные во временную таблицу
                                for key_val, id_val in current_batch:
                                    cursor.execute('INSERT INTO temp_key_updates VALUES (%s, %s)', (key_val, id_val))
                                
                                # Выполняем обновление через JOIN
                                sql_update = f'''
                                UPDATE "{self.schema}"."{input_table_name}" AS t
                                SET "{key_column_name}" = k.key_val
                                FROM temp_key_updates AS k
                                WHERE t."{id_column_name}" = k.id_val
                                '''
                                
                                cursor.execute(sql_update)
                                affected = cursor.rowcount
                                
                                # Подтверждаем изменения
                                conn.commit()
                                
                                success_count += affected
                                print(f"Пакетное обновление: {affected} записей обновлено из {len(current_batch)}")
                                
                                # Очищаем партию
                                current_batch = []
                            except Exception as e:
                                error_count += len(current_batch)
                                print(f"Ошибка пакетного обновления: {e}")
                                conn.rollback()
                                
                                # Попробуем обновить по одной записи
                                print("Пробуем обновлять записи по одной...")
                                for key_val, id_val in current_batch:
                                    try:
                                        sql = f'UPDATE "{self.schema}"."{input_table_name}" SET "{key_column_name}" = %s WHERE "{id_column_name}" = %s'
                                        cursor.execute(sql, (key_val, id_val))
                                        if cursor.rowcount > 0:
                                            success_count += 1
                                        conn.commit()
                                    except Exception as e2:
                                        error_count += 1
                                        conn.rollback()
                                        print(f"Ошибка индивидуального обновления для ID={id_val}: {e2}")
                                
                                # Очищаем партию
                                current_batch = []
                    
                    # Проверяем результаты
                    cursor.execute(sql_check)
                    new_non_null_count = cursor.fetchone()[0]
                    print(f"\nРезультаты обновления:")
                    print(f"Было записей с ключами до обновления: {non_null_count}")
                    print(f"Стало записей с ключами после обновления: {new_non_null_count}")
                    print(f"Добавлено новых записей с ключами: {new_non_null_count - non_null_count}")
                    print(f"Успешно обновлено: {success_count}, Ошибок: {error_count}")
                    
                    # Показываем примеры обновленных записей
                    try:
                        sql_examples = f'SELECT "{id_column_name}", "{key_column_name}" FROM "{self.schema}"."{input_table_name}" WHERE "{key_column_name}" IS NOT NULL LIMIT 5'
                        cursor.execute(sql_examples)
                        examples = cursor.fetchall()
                        
                        if examples:
                            print("\nПримеры обновленных записей:")
                            for row in examples:
                                print(f"ID: {row[0]}, Key: {row[1]}")
                    except Exception as e:
                        print(f"Ошибка при получении примеров: {e}")
                    
                    # Закрываем курсор и соединение
                    cursor.close()
                    conn.close()
                    
                    # Если ничего не обновилось, проведем дополнительную диагностику
                    if new_non_null_count == non_null_count:
                        print("\nВНИМАНИЕ! Обновление не повлияло на количество записей с ключами")
                        print("Возможные причины:")
                        print("1. Записи с указанными ID не существуют в таблице")
                        print("2. У записей уже были установлены ключи")
                        print("3. Проблемы с правами доступа или триггерами")
                        
                        # Попробуем крайний способ - прямое обновление одной записи
                        if len(update_data) > 0:
                            try:
                                test_id = update_data[0]['id']
                                test_key = 999999
                                
                                print(f"\nПроводим тестовое обновление для ID={test_id}, Key={test_key}...")
                                
                                with self.engine.begin() as conn:
                                    # Максимально простой запрос обновления
                                    sql = text(f'UPDATE "{self.schema}"."{input_table_name}" SET "{key_column_name}" = :key WHERE "{id_column_name}" = :id')
                                    conn.execute(sql, {"key": test_key, "id": test_id})
                                    
                                    # Проверяем результат
                                    check = text(f'SELECT COUNT(*) FROM "{self.schema}"."{input_table_name}" WHERE "{id_column_name}" = :id AND "{key_column_name}" = :key')
                                    result = conn.execute(check, {"id": test_id, "key": test_key}).scalar()
                                    
                                    if result > 0:
                                        print(f"ТЕСТ УСПЕШЕН: Запись с ID={test_id} обновлена до Key={test_key}")
                                        print("Если вы видите это сообщение, то проблема может быть в механизме обновления.")
                                    else:
                                        print(f"ТЕСТ ПРОВАЛЕН: Запись с ID={test_id} не была обновлена")
                                        
                                        # Проверяем существование записи
                                        exists = text(f'SELECT COUNT(*) FROM "{self.schema}"."{input_table_name}" WHERE "{id_column_name}" = :id')
                                        count = conn.execute(exists, {"id": test_id}).scalar()
                                        
                                        if count == 0:
                                            print(f"ПРИЧИНА: Запись с ID={test_id} не существует в таблице")
                                        else:
                                            print(f"ПРИЧИНА: Запись существует, но не может быть обновлена")
                            except Exception as e:
                                print(f"Ошибка при тестовом обновлении: {e}")
                
                except Exception as e:
                    print(f"Ошибка при анализе структуры БД: {e}")
            
            print("\n===== ЗАВЕРШЕНИЕ ПРОЦЕССА СОХРАНЕНИЯ =====")
                        
        except Exception as e:
            self.logger.write(f"Ошибка при сохранении результатов: {str(e)}\n")
            raise e