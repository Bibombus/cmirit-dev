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

    # Специальные правила для составных имен
    SPECIAL_RULES = {
        'ТРУБИЦЫНА': 'ИМЕНИ ПРОТОИЕРЕЯ ГЕОРГИЯ ТРУБИЦЫНА',
        'ОКИНИНА': 'ПАРТИЗАНА ОКИНИНА',
        'БЕЛЯЕВА': 'КОСМОНАВТА БЕЛЯЕВА',
        'ЛИБКНЕХТА': 'КАРЛА ЛИБКНЕХТА',
        'ЛЮКСЕМБУРГ': 'РОЗЫ ЛЮКСЕМБУРГ',
        'РОЗЫ': 'РОЗЫ ЛЮКСЕМБУРГ',
        'ГОРЬКОГО': 'МАКСИМА ГОРЬКОГО',
        'БЕЛОВА': 'КОМАНДАРМА БЕЛОВА',
        'ПЕРЦА': 'СЕРГЕЯ ПЕРЦА',
        'ПИТОМНИКА': 'ГОРОДСКОГО ПИТОМНИКА',
        'СЕРОВКИ': 'НАБЕРЕЖНАЯ СЕРОВКИ',
        'ЮНГ': 'СОЛОВЕЦКИХ ЮНГ',
        'ЮЖНАЯ': 'ПОДСТАНЦИИ ЮЖНАЯ'
    }

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
                        'note': item.note if hasattr(item, 'note') else ("Нет правильного адреса" if item.address is None or item.key is None else None)
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
            
    def _expand_address_with_rules(self, address: str) -> str:
        """Преобразует адрес в полную форму с помощью правил.
        
        Args:
            address (str): Исходный адрес.
            
        Returns:
            str: Адрес в полной форме.
        """
        if not address:
            return ""
            
        print(f"\n{'='*50}")
        print(f"РАСШИРЕНИЕ АДРЕСА С ПОМОЩЬЮ ПРАВИЛ")
        print(f"Исходный адрес: {address}")
        print(f"{'='*50}")
        
        # Приводим к верхнему регистру
        address = address.upper()
        print(f"\nПосле приведения к верхнему регистру:")
        print(f"  - Адрес: {address}")
        
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
            'ТЕРРИТОРИЯ': 'ТЕР.',
            'ТЕРРИТ': 'ТЕР.',
            'ТЕР ': 'ТЕР.',
            'ТЕР.': 'ТЕР.',
            'ПОДСТАНЦИЯ': 'ПОДСТ.',
            'ПОДСТ': 'ПОДСТ.',
            'ПОДСТ.': 'ПОДСТ.',
        }
        
        # Применяем замены
        for old, new in replacements.items():
            if old in address:
                print(f"  - Замена '{old}' на '{new}'")
                address = address.replace(old, new)
        print(f"После замены сокращений:")
        print(f"  - Адрес: {address}")
            
        # Убираем лишние пробелы
        address = ' '.join(address.split())
        
        # Разбиваем адрес на части
        parts = address.split()
        print(f"\nЧасти адреса:")
        print(f"  - {parts}")
        
        # Если адрес состоит только из номера дома, возвращаем как есть
        if len(parts) == 1 and parts[0].isdigit():
            print(f"Адрес состоит только из номера дома, возвращаем как есть")
            return address
            
        # Получаем номер дома (последнее число в адресе)
        house_number = None
        for part in reversed(parts):
            if part.isdigit():
                house_number = part
                break
                
        # Убираем номер дома из адреса для обработки
        if house_number:
            print(f"\nНомер дома: {house_number}")
            address = ' '.join(part for part in parts if part != house_number)
            print(f"Адрес без номера дома:")
            print(f"  - {address}")
        
        # Проверяем наличие префиксов территории
        territory_prefixes = ['ТЕР.']
        print(f"\nПроверка префиксов территории:")
        print(f"  - Доступные префиксы: {territory_prefixes}")
        print(f"  - Текущий адрес: '{address}'")
        
        has_territory_prefix = any(address.startswith(prefix) for prefix in territory_prefixes)
        print(f"  - Найден префикс территории: {has_territory_prefix}")
        
        # Если адрес начинается с префикса территории, сохраняем его
        territory_prefix = None
        if has_territory_prefix:
            print(f"\nОбнаружен префикс территории:")
            territory_prefix = next(prefix for prefix in territory_prefixes if address.startswith(prefix))
            print(f"  - Префикс: {territory_prefix}")
            # Убираем префикс для дальнейшей обработки
            address = address[len(territory_prefix):].strip()
            print(f"  - Адрес без префикса: {address}")
        
        # Проверяем каждое слово в адресе
        result_parts = []
        print(f"\nПрименение специальных правил:")
        for part in address.split():
            found_rule = False
            print(f"  - Проверка слова: '{part}'")
            for old, new in self.SPECIAL_RULES.items():
                if old in part:
                    print(f"    - Найдено правило для слова '{part}': '{new}'")
                    result_parts.append(new)
                    found_rule = True
                    break
            if not found_rule:
                print(f"    - Правило не найдено для слова '{part}', оставляем как есть")
                result_parts.append(part)
        
        # Собираем адрес обратно
        expanded_address = ' '.join(result_parts)
        
        # Если был префикс территории, добавляем его обратно
        if territory_prefix:
            expanded_address = f"{territory_prefix} {expanded_address}"
            print(f"\nДобавлен префикс территории обратно:")
            print(f"  - Адрес с префиксом: {expanded_address}")
        
        # Добавляем номер дома обратно, если он был
        if house_number:
            expanded_address = f"{expanded_address} {house_number}"
            
        print(f"\nИтоговый расширенный адрес:")
        print(f"  - {expanded_address}")
        print(f"{'='*50}")
        
        return expanded_address

    def _find_best_match(self, address: str, reference_addresses: list[str]) -> str:
        """Находит наиболее подходящий адрес из справочника с учетом частичных совпадений слов.
        
        Args:
            address (str): Исходный адрес для поиска.
            reference_addresses (list[str]): Список адресов из справочника.
            
        Returns:
            str: Наиболее подходящий адрес из справочника.
        """
        if not address or not reference_addresses:
            return None
            
        print(f"\nПоиск совпадения для адреса: {address}")
        print(f"Количество адресов в справочнике: {len(reference_addresses)}")
        
        # Сначала расширяем адрес с помощью правил
        expanded_address = self._expand_address_with_rules(address)
        print(f"Расширенный адрес: {expanded_address}")
        
        # Нормализуем расширенный адрес
        normalized_address = self._normalize_address(expanded_address)
        print(f"Нормализованный расширенный адрес: {normalized_address}")
        
        # Разбиваем нормализованный адрес на слова
        address_words = set(normalized_address.upper().split())
        print(f"Слова в нормализованном адресе: {address_words}")
        
        best_match = None
        best_score = 0
        
        for ref_addr in reference_addresses:
            # Нормализуем адрес из справочника
            ref_addr = self._normalize_address(ref_addr)
            print(f"\nПроверка адреса из справочника: {ref_addr}")
            
            # Разбиваем адрес из справочника на слова
            ref_words = set(ref_addr.upper().split())
            print(f"Слова в адресе из справочника: {ref_words}")
            
            # Находим общие слова
            common_words = address_words.intersection(ref_words)
            print(f"Общие слова: {common_words}")
            
            if common_words:
                # Вычисляем оценку совпадения
                # Базовый вес за каждое совпадающее слово
                score = len(common_words) * 2
                
                # Дополнительный вес за совпадение порядка слов
                address_list = normalized_address.upper().split()
                ref_list = ref_addr.upper().split()
                
                # Проверяем совпадение порядка слов
                for i in range(min(len(address_list), len(ref_list))):
                    if address_list[i] == ref_list[i]:
                        score += 1
                
                # Дополнительный вес за совпадение начала адреса
                if address_list and ref_list and address_list[0] == ref_list[0]:
                    score += 2
                
                # Дополнительный вес за совпадение конца адреса
                if address_list and ref_list and address_list[-1] == ref_list[-1]:
                    score += 2
                
                # Дополнительный вес за совпадение длины адреса
                if len(address_list) == len(ref_list):
                    score += 1
                
                print(f"Оценка совпадения: {score}")
                
                if score > best_score:
                    best_score = score
                    best_match = ref_addr
                    print(f"Новый лучший вариант: {best_match} (оценка: {best_score})")
        
        print(f"\nИтоговый результат: {best_match} (оценка: {best_score})")
        return best_match

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
        
        # Разбиваем адрес на части
        parts = address.split()
        print(f"Части адреса: {parts}")
        
        # Если адрес состоит только из номера дома, возвращаем как есть
        if len(parts) == 1 and parts[0].isdigit():
            return address
            
        # Получаем номер дома (последнее число в адресе)
        house_number = None
        for part in reversed(parts):
            if part.isdigit():
                house_number = part
                break
                
        # Убираем номер дома из адреса для обработки
        if house_number:
            address = ' '.join(part for part in parts if part != house_number)
            print(f"Адрес без номера дома: {address}")
        
        # Проверяем каждое слово в адресе
        result_parts = []
        for part in address.split():
            found_rule = False
            for old, new in self.SPECIAL_RULES.items():
                if old in part:
                    print(f"Найдено правило для слова {part}: {new}")
                    result_parts.append(new)
                    found_rule = True
                    break
            if not found_rule:
                print(f"Правило не найдено для слова {part}, оставляем как есть")
                result_parts.append(part)
        
        # Собираем адрес обратно
        normalized_address = ' '.join(result_parts)
        
        # Добавляем номер дома обратно, если он был
        if house_number:
            normalized_address = f"{normalized_address} {house_number}"
            
        print(f"Итоговый нормализованный адрес: {normalized_address}")
        
        return normalized_address 