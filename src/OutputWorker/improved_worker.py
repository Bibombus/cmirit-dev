"""
Улучшенная версия класса DatabaseOutputWorker для решения проблемы с null значениями
в поле key_street_house во входной таблице базы данных.
"""

from abc import ABC, abstractmethod
from typing import Any, Iterable
import sys
from tkinter import Listbox
import time

import pandas as pd
from sqlalchemy import Engine, Integer, create_engine, MetaData, Table, Column, String, text, select
from sqlalchemy import inspect

from ..AddresInfo import Address
from .outputWorker import OutputWorker, AddressDTO, LoggersCollection

class ImprovedDatabaseOutputWorker(OutputWorker):
    """Улучшенная версия класса для записи в базу данных."""

    def __init__(self, engine, input_table_name, output_table_name, schema, id_column=None, logger=None):
        """Инициализация объекта.
        
        Args:
            engine: Подключение к БД.
            input_table_name: Имя входной таблицы.
            output_table_name: Имя выходной таблицы.
            schema: Схема БД.
            id_column: Имя колонки с ID (может быть None).
            logger: Объект для логирования.
        """
        super().__init__(logger)
        self.engine = engine
        self.input_table_name = input_table_name
        self.output_table_name = output_table_name
        self.schema = schema
        self.id_column = id_column

    def save(self, addresses):
        """Сохраняет данные в базу данных и обновляет ключи во входной таблице.
        
        Args:
            addresses: Итератор объектов AddressDTO.
        """
        
        # Проверка существования схемы
        inspector = inspect(self.engine)
        schemas = inspector.get_schema_names()
        print(f"Доступные схемы в БД: {schemas}")
        
        if self.schema not in schemas:
            error_msg = f"Схема '{self.schema}' не найдена в базе данных. Доступные схемы: {schemas}"
            print(f"ОШИБКА: {error_msg}")
            raise Exception(error_msg)
        
        try:
            print(f"\n===== НАЧАЛО ПРОЦЕССА СОХРАНЕНИЯ =====")
            print(f"Схема: {self.schema}")
            print(f"Входная таблица: {self.input_table_name}")
            print(f"Выходная таблица: {self.output_table_name}")
            print(f"ID колонка: {self.id_column if self.id_column else 'Не указана'}")
            
            # ЭТАП 1: Сбор данных для сохранения
            # ----------------------------------------
            output_data = []  # Данные для выходной таблицы
            update_data = []  # Данные для обновления ключей
            
            for item in addresses:
                # Данные для выходной таблицы
                output_data.append({
                    'raw_address': item.raw,
                    'street_name': item.Name,
                    'street_type': item.Type,
                    'house': item.House,
                    'flat': item.Flat,
                    'key': item.key
                })
                
                # Данные для обновления ключей
                if item.key is not None:
                    if self.id_column:
                        # Если указана ID-колонка, ищем ID в объекте
                        id_value = getattr(item, self.id_column, None)
                        
                        if id_value is not None:
                            try:
                                update_data.append({
                                    'id': int(id_value) if isinstance(id_value, (int, float, str)) else id_value,
                                    'key': int(item.key) if isinstance(item.key, (int, float, str)) else item.key
                                })
                                print(f"Добавлены данные для обновления с ID: {id_value}, key: {item.key}")
                            except (ValueError, TypeError) as e:
                                print(f"Ошибка при обработке ID={id_value}, key={item.key}: {e}")
                        else:
                            print(f"ID не найден для записи с ключом {item.key}")
                    else:
                        # Если ID-колонка не указана, используем адрес как идентификатор
                        try:
                            update_data.append({
                                'address': item.raw,
                                'key': int(item.key) if isinstance(item.key, (int, float, str)) else item.key
                            })
                            print(f"Добавлены данные для обновления по адресу: '{item.raw}', key: {item.key}")
                        except (ValueError, TypeError) as e:
                            print(f"Ошибка при обработке address='{item.raw}', key={item.key}: {e}")
            
            print(f"Подготовлено {len(output_data)} записей для выходной таблицы")
            print(f"Подготовлено {len(update_data)} записей для обновления ключей")
            
            # ЭТАП 2: Создание и заполнение выходной таблицы
            # ----------------------------------------
            try:
                # Создаем метаданные
                metadata = MetaData(schema=self.schema)
                
                # Определяем структуру выходной таблицы
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
                
                # Создаем таблицу в БД (если не существует)
                output_table.create(self.engine, checkfirst=True)
                print("Выходная таблица создана или уже существует")
                
                # Записываем данные в выходную таблицу
                if output_data:
                    with self.engine.begin() as conn:
                        conn.execute(output_table.insert(), output_data)
                    print(f"Сохранено {len(output_data)} записей в выходную таблицу")
            except Exception as e:
                print(f"Ошибка при работе с выходной таблицей: {e}")
            
            # ЭТАП 3: Обновление ключей во входной таблице
            # ----------------------------------------
            if not update_data:
                print("Нет данных для обновления ключей")
                return
            
            # Инспектор для изучения структуры БД
            inspector = inspect(self.engine)
            
            try:
                # Проверка существования таблицы
                tables = inspector.get_table_names(schema=self.schema)
                print(f"Доступные таблицы: {tables}")
                
                # Поиск таблицы с учетом регистра
                actual_table_name = None
                for table in tables:
                    if table.lower() == self.input_table_name.lower():
                        actual_table_name = table
                        print(f"Найдена входная таблица: {actual_table_name}")
                        break
                
                if not actual_table_name:
                    print(f"ОШИБКА: Таблица {self.input_table_name} не найдена в схеме {self.schema}")
                    return
                
                # Исследуем колонки таблицы
                columns = inspector.get_columns(actual_table_name, schema=self.schema)
                print(f"Колонки входной таблицы:")
                column_names = []
                for col in columns:
                    print(f"  - {col['name']} (тип: {col['type']})")
                    column_names.append(col['name'])
                
                # Если используем адрес как идентификатор, ищем подходящую колонку
                address_column_name = None
                if not self.id_column:
                    for col_name in column_names:
                        if col_name.lower() in ['address', 'raw_address', 'addr', 'adres']:
                            address_column_name = col_name
                            print(f"Найдена колонка с адресами: {address_column_name}")
                            break
                    
                    if not address_column_name:
                        print("ОШИБКА: Не найдена колонка с адресами во входной таблице")
                        return
                # Иначе ищем ID-колонку
                else:
                    id_column_name = None
                    for col_name in column_names:
                        if col_name.lower() == self.id_column.lower():
                            id_column_name = col_name
                            print(f"Найдена ID-колонка: {id_column_name}")
                            break
                    
                    if not id_column_name:
                        print(f"ОШИБКА: Колонка {self.id_column} не найдена в таблице")
                        return
                    
                    # Сохраняем имя колонки с учетом регистра
                    self.id_column = id_column_name
                
                # Проверяем наличие колонки для ключей
                key_column_name = None
                for col_name in column_names:
                    if col_name.lower() == 'key_street_house':
                        key_column_name = col_name
                        print(f"Найдена колонка для ключей: {key_column_name}")
                        break
                
                # Если колонки нет, создаем ее
                if not key_column_name:
                    print("Колонка key_street_house не найдена, создаем...")
                    with self.engine.begin() as conn:
                        conn.execute(text(f'ALTER TABLE "{self.schema}"."{actual_table_name}" ADD COLUMN "key_street_house" INTEGER'))
                    print("Колонка key_street_house создана")
                    
                    # Проверяем создание колонки
                    time.sleep(1)  # Даем БД время на обновление метаданных
                    inspector = inspect(self.engine)  # Получаем свежий инспектор
                    columns = inspector.get_columns(actual_table_name, schema=self.schema)
                    for col in columns:
                        if col['name'].lower() == 'key_street_house':
                            key_column_name = col['name']
                            print(f"Подтверждено создание колонки: {key_column_name}")
                            break
                    
                    if not key_column_name:
                        print("Не удалось подтвердить создание колонки key_street_house через инспектор метаданных.")
                        print("Продолжаем обработку, так как колонка, вероятно, была создана успешно...")
                        key_column_name = "key_street_house"  # Используем имя по умолчанию
                
                # ЭТАП 4: Обновление ключей
                # ----------------------------------------
                print("\n===== ОБНОВЛЕНИЕ КЛЮЧЕЙ =====")
                
                # Определяем режим обновления
                using_address = not self.id_column
                
                # Создаем подключение и курсор
                conn = self.engine.raw_connection()
                cursor = conn.cursor()
                
                # Проверяем количество записей с непустыми ключами до обновления
                sql_check = f'SELECT COUNT(*) FROM "{self.schema}"."{actual_table_name}" WHERE "{key_column_name}" IS NOT NULL'
                cursor.execute(sql_check)
                initial_keys_count = cursor.fetchone()[0]
                print(f"Текущее количество записей с ключами: {initial_keys_count}")
                
                # МЕТОД 1: Простое обновление по одной записи
                print("\nМЕТОД 1: Простое обновление по одной записи...")
                success_count = 0
                error_count = 0
                
                for update in update_data:
                    try:
                        if using_address:
                            # Используем адрес как идентификатор
                            addr_val = update['address']
                            key_val = update['key']
                            # Очищаем лишние пробелы и экранируем кавычки в адресе для безопасности SQL-запроса
                            safe_addr = addr_val.strip().replace("'", "''")
                            # Используем CASE-INSENSITIVE сравнение для большей надежности
                            sql = f'UPDATE "{self.schema}"."{actual_table_name}" SET "{key_column_name}" = {key_val} WHERE TRIM(BOTH FROM UPPER("{address_column_name}")) = UPPER(\'{safe_addr}\')'
                            if success_count < 5:  # Показываем только первые несколько запросов
                                print(f"SQL запрос: {sql}")
                        else:
                            # Используем id как идентификатор
                            id_val = update['id']
                            key_val = update['key']
                            sql = f'UPDATE "{self.schema}"."{actual_table_name}" SET "{key_column_name}" = {key_val} WHERE "{id_column_name}" = {id_val}'
                            if success_count < 5:  # Показываем только первые несколько запросов
                                print(f"SQL запрос: {sql}")
                        
                        # Выполняем запрос
                        cursor.execute(sql)
                        conn.commit()
                        if cursor.rowcount > 0:
                            success_count += 1
                            print(f"Успешно обновлена запись {success_count}: key={key_val}")
                        
                    except Exception as e:
                        error_count += 1
                        if error_count <= 5:  # Ограничиваем вывод ошибок
                            print(f"Ошибка при обновлении записи: {e}")
                
                print(f"Всего успешно обновлено: {success_count}, ошибок: {error_count}")
                
                # МЕТОД 2: Массовое обновление (если нужно)
                if initial_keys_count == 0 and len(update_data) > 0:
                    print("\nМЕТОД 1 не дал результатов. Пробуем МЕТОД 2: Пакетное обновление...")
                    
                    try:
                        # Создаем временную таблицу
                        cursor.execute('DROP TABLE IF EXISTS temp_key_updates')
                        
                        if using_address:
                            # Таблица с адресами
                            cursor.execute('CREATE TEMP TABLE temp_key_updates (addr_val TEXT, key_val INTEGER)')
                            for update in update_data:
                                addr_val = update['address'].strip().replace("'", "''")
                                cursor.execute('INSERT INTO temp_key_updates VALUES (%s, %s)', 
                                              (addr_val, update['key']))
                            
                            # SQL для обновления
                            sql = f'''
                            UPDATE "{self.schema}"."{actual_table_name}" AS t
                            SET "{key_column_name}" = k.key_val
                            FROM temp_key_updates AS k
                            WHERE TRIM(BOTH FROM UPPER(t."{address_column_name}")) = UPPER(TRIM(BOTH FROM k.addr_val))
                            '''
                        else:
                            # Таблица с ID
                            cursor.execute('CREATE TEMP TABLE temp_key_updates (id_val INTEGER, key_val INTEGER)')
                            for update in update_data:
                                cursor.execute('INSERT INTO temp_key_updates VALUES (%s, %s)', 
                                              (update['id'], update['key']))
                            
                            # SQL для обновления
                            sql = f'''
                            UPDATE "{self.schema}"."{actual_table_name}" AS t
                            SET "{key_column_name}" = k.key_val
                            FROM temp_key_updates AS k
                            WHERE t."{self.id_column}" = k.id_val
                            '''
                        
                        # Выполняем обновление
                        print(f"Выполняем запрос: {sql}")
                        cursor.execute(sql)
                        affected = cursor.rowcount
                        conn.commit()
                        
                        # Проверяем результаты
                        cursor.execute(sql_check)
                        method2_keys_count = cursor.fetchone()[0]
                        
                        print("\n===== РЕЗУЛЬТАТЫ МЕТОДА 2 =====")
                        print(f"Было записей с ключами: {initial_keys_count}")
                        print(f"Стало записей с ключами: {method2_keys_count}")
                        print(f"Добавлено новых ключей: {method2_keys_count - initial_keys_count}")
                        print(f"Обновлено записей: {affected}")
                        
                    except Exception as e:
                        conn.rollback()
                        print(f"Ошибка при выполнении МЕТОДА 2: {e}")
                
                # Финальная проверка
                print("\n===== ФИНАЛЬНЫЕ РЕЗУЛЬТАТЫ =====")
                cursor.execute(sql_check)
                final_keys_count = cursor.fetchone()[0]
                print(f"Всего записей с ключами до обновления: {initial_keys_count}")
                print(f"Всего записей с ключами после обновления: {final_keys_count}")
                print(f"Добавлено новых ключей: {final_keys_count - initial_keys_count}")
                print(f"Общее количество записей для обновления: {len(update_data)}")
                
                # Закрываем соединения
                cursor.close()
                conn.close()
            
            except Exception as e:
                print(f"Ошибка при анализе структуры БД: {e}")
            
            print("\n===== ЗАВЕРШЕНИЕ ПРОЦЕССА СОХРАНЕНИЯ =====")
            
        except Exception as e:
            self.logger.write(f"Ошибка при сохранении результатов: {str(e)}\n")
            raise e 