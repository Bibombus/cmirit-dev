"""
Улучшенная версия класса DatabaseOutputWorker для решения проблемы с null значениями
в поле key_street_house во входной таблице базы данных.
"""

from abc import ABC, abstractmethod
from typing import Any, Iterable
import sys
from tkinter import Listbox, messagebox
import time
import traceback
import re

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
        print(f"Инициализация ImprovedDatabaseOutputWorker:")
        print(f"  - ID колонка: {self.id_column}")
        print(f"  - Входная таблица: {self.input_table_name}")
        print(f"  - Выходная таблица: {self.output_table_name}")
        print(f"  - Схема: {self.schema}")

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
            
            # Проверяем существование выходной таблицы
            tables = inspector.get_table_names(schema=self.schema)
            table_exists = self.output_table_name.lower() in [t.lower() for t in tables]
            
            # Запрашиваем у пользователя режим работы с таблицей
            if table_exists:
                response = messagebox.askyesnocancel(
                    "Таблица уже существует",
                    f"Таблица {self.output_table_name} уже существует.\n\nВыберите действие:",
                    icon='question',
                    default='cancel',
                    detail="Нажмите 'Да' чтобы добавить новые данные к существующим\nНажмите 'Нет' чтобы удалить существующие данные и перезаписать новую таблицу"
                )
                
                if response is None:  # Пользователь нажал Cancel или закрыл окно
                    print("Операция отменена пользователем")
                    return
                
                # Сохраняем выбор пользователя
                append_mode = response  # True для дополнения, False для перезаписи
            
            # ЭТАП 1: Сбор данных для сохранения
            # ----------------------------------------
            output_data = []  # Данные для выходной таблицы
            update_data = []  # Данные для обновления ключей
            
            for item in addresses:
                try:
                    # Проверяем, что item.raw является строкой, а не числом
                    raw_address = str(item.raw) if item.raw is not None else None
                    
                    if raw_address is None:
                        print("Пропуск записи с пустым адресом")
                        continue
                    
                    # Нормализуем адрес для поиска
                    normalized_address = self._normalize_address(raw_address)
                    print(f"Обработка адреса: {raw_address}")
                    print(f"Нормализованный адрес: {normalized_address}")
                    
                    # Данные для выходной таблицы
                    output_data.append({
                        'raw_address': raw_address,
                        'street_name': item.Name if hasattr(item, 'Name') else None,
                        'street_type': item.Type if hasattr(item, 'Type') else None,
                        'house': item.House if hasattr(item, 'House') else None,
                        'flat': item.Flat if hasattr(item, 'Flat') else None,
                        'key': item.key,
                        'note': item.note if hasattr(item, 'note') else ("Адрес не распознан" if item.address is None or item.key is None else None)
                    })
                    
                    # Проверяем, был ли адрес успешно распознан для обновления ключей
                    if item.address is None or item.key is None:
                        print(f"Адрес не распознан: {raw_address}")
                        continue
                    
                    # Данные для обновления ключей
                    if item.key is not None:
                        if self.id_column:
                            # Если указана ID-колонка, ищем ID в объекте
                            id_value = getattr(item, 'ID', None)  # Используем 'ID' вместо self.id_column
                            
                            if id_value is not None:
                                try:
                                    # Преобразуем ID в число, если это возможно
                                    if isinstance(id_value, str):
                                        try:
                                            id_value = int(id_value)
                                        except ValueError:
                                            # Если не удалось преобразовать в число, оставляем как строку
                                            pass
                                    
                                    update_data.append({
                                        'id': id_value,
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
                                    'address': raw_address,
                                    'key': int(item.key) if isinstance(item.key, (int, float, str)) else item.key
                                })
                                print(f"Добавлены данные для обновления по адресу: '{raw_address}', key: {item.key}")
                            except (ValueError, TypeError) as e:
                                print(f"Ошибка при обработке address='{raw_address}', key={item.key}: {e}")
                except Exception as e:
                    print(f"Ошибка при обработке записи: {str(e)}")
                    print(f"Детали записи: {item.__dict__}")
                    continue
            
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
                    Column('note', String),
                    extend_existing=True
                )
                
                if table_exists:
                    if append_mode:  # Дополнение существующей таблицы
                        with self.engine.begin() as conn:
                            conn.execute(output_table.insert(), output_data)
                        print(f"Добавлено {len(output_data)} записей в существующую таблицу")
                    else:  # Перезапись таблицы
                        output_table.drop(self.engine, checkfirst=True)
                        output_table.create(self.engine)
                        with self.engine.begin() as conn:
                            conn.execute(output_table.insert(), output_data)
                        print(f"Таблица перезаписана, добавлено {len(output_data)} записей")
                else:
                    # Создаем новую таблицу
                    output_table.create(self.engine)
                    with self.engine.begin() as conn:
                        conn.execute(output_table.insert(), output_data)
                    print(f"Создана новая таблица с {len(output_data)} записями")
                
            except Exception as e:
                print(f"Ошибка при работе с выходной таблицей: {str(e)}")
                print(f"Трассировка: {traceback.format_exc()}")
                raise Exception(f"Ошибка при работе с выходной таблицей: {str(e)}")
            
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
                    key_column_name = "key_street_house"
                
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
                            sql = f'UPDATE "{self.schema}"."{actual_table_name}" SET "{key_column_name}" = {key_val} WHERE "{self.id_column}" = {id_val}'
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
                            print(f"Ошибка при обновлении записи: {str(e)}")
                            print(f"Детали обновления: {update}")
                
                print(f"Всего успешно обновлено: {success_count}, ошибок: {error_count}")
                
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
                print(f"Ошибка при анализе структуры БД: {str(e)}")
                print(f"Трассировка: {traceback.format_exc()}")
                raise Exception(f"Ошибка при анализе структуры БД: {str(e)}")
            
            print("\n===== ЗАВЕРШЕНИЕ ПРОЦЕССА СОХРАНЕНИЯ =====")
            
        except Exception as e:
            error_msg = f"Ошибка при сохранении результатов: {str(e)}\n{traceback.format_exc()}"
            print(error_msg)
            self.logger.write(error_msg)
            raise Exception(error_msg)
            
    def _normalize_address(self, address: str) -> str:
        """Нормализует адрес для поиска.
        
        Args:
            address (str): Исходный адрес.
            
        Returns:
            str: Нормализованный адрес.
        """
        if not address:
            return ""
            
        print(f"\nОбработка адреса: {address}")
            
        # Приводим к верхнему регистру
        address = address.upper()
        print(f"После приведения к верхнему регистру: {address}")
        
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
        
        # Применяем замены
        for old, new in replacements.items():
            address = address.replace(old, new)
        print(f"После замены сокращений: {address}")
            
        # Убираем лишние пробелы
        address = ' '.join(address.split())
        
        # Удаляем префиксы типа "УЛ." если они есть в начале
        prefixes = ['УЛ.', 'ПР-КТ', 'ПР-Д', 'Б-Р', 'ПЛ.']
        for prefix in prefixes:
            if address.startswith(prefix):
                address = address[len(prefix):].strip()
                break
        print(f"После удаления префиксов: {address}")
                
        # Специальные правила для составных имен
        special_rules = {
            'ОКИНИНА': 'ПАРТИЗАНА ОКИНИНА',
            'БЕЛЯЕВА': 'КОСМОНАВТА БЕЛЯЕВА',
            'ЛИБКНЕХТА': 'КАРЛА ЛИБКНЕХТА',
            'ЛЮКСЕМБУРГ': 'РОЗЫ ЛЮКСЕМБУРГ',
            'РОЗЫ': 'РОЗЫ ЛЮКСЕМБУРГ',
            'ГОРЬКОГО': 'МАКСИМА ГОРЬКОГО',
            'БЕЛОВА': 'ГЕНЕРАЛА БЕЛОВА',
            'ТРУБИЦЫНА': 'ПРОТОИЕРЕЯ ГЕОРГИЯ ТРУБИЦЫНА'
        }
        
        # Разбиваем адрес на части
        parts = address.split()
        print(f"Части адреса: {parts}")
        
        # Создаем новый список для результата
        result_parts = []
        
        # Обрабатываем каждую часть
        i = 0
        while i < len(parts):
            current_word = parts[i]
            print(f"\nОбработка слова: {current_word}")
            
            # Проверяем, есть ли это слово в правилах
            if current_word in special_rules:
                print(f"Найдено правило для слова {current_word}")
                # Проверяем, не является ли это частью уже замененного имени
                if not any(special_rules[current_word] in ' '.join(result_parts)):
                    print(f"Применяем правило: {current_word} -> {special_rules[current_word]}")
                    result_parts.append(special_rules[current_word])
                else:
                    print(f"Пропускаем замену, так как это часть уже замененного имени")
                    result_parts.append(current_word)
            else:
                print(f"Правило не найдено, оставляем как есть")
                result_parts.append(current_word)
            
            i += 1
        
        # Собираем адрес обратно
        normalized_address = ' '.join(result_parts)
        print(f"Итоговый нормализованный адрес: {normalized_address}")
        
        return normalized_address 