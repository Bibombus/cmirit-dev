import argparse
import datetime
import sys
import os
from typing import Any
from enum import Enum
import logging
import time
from os import path

import pandas as pd
from sqlalchemy import create_engine, select, MetaData, Table, text, inspect

from src import AbbrsInfo
from src.AddresInfo import Address
from src.AddresInfo.address import StreetType
from src.Linker import Linker
from src.OutputWorker import SingleTableExcelOutputWorker, AddressDTO, LoggersCollection, ImprovedDatabaseOutputWorker
from src.OutputWorker.outputWorker import DatabaseOutputWorker
from src.args import make_args_parser
from src import gui
from src.exceptions_manager import ExceptionsManager

linker: Linker | None = None
logger: LoggersCollection | None
args: argparse.Namespace | None = None


class ProcessingStats:
    def __init__(self):
        self.successful = 0
        self.unprocessed = 0  # адреса, которые разобраны, но не найдены в справочнике
        self.unparsed = 0     # адреса, которые не удалось разобрать
        self.unprocessed_addresses = []  # список необработанных адресов
        self.unparsed_addresses = []     # список неразобранных адресов

    def add_success(self):
        self.successful += 1

    def add_unprocessed(self, address: str, error: Exception):
        self.unprocessed += 1
        self.unprocessed_addresses.append((address, str(error)))

    def add_unparsed(self, address: str, error: Exception):
        self.unparsed += 1
        self.unparsed_addresses.append((address, str(error)))

    def get_summary(self) -> str:
        return f"Результаты обработки:\n✓ Успешно: {self.successful} адресов\n✗ Необработано: {self.unprocessed} адресов\n✗ Неразобрано: {self.unparsed} адресов"

    def export_errors(self, output_path: str):
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("Адрес,Тип ошибки,Ошибка\n")
            for addr, error in self.unprocessed_addresses:
                f.write(f'"{addr}","Необработан","{error}"\n')
            for addr, error in self.unparsed_addresses:
                f.write(f'"{addr}","Неразобран","{error}"\n')


def make_logger():
    """ Создает объект логера, который делает записи в различные источники. """

    logsdir = "./logs/"
    logfilename = f'{datetime.datetime.now().strftime("%Y.%m.%d_%H-%M-%S")}.log'
    if not os.path.exists(logsdir):
        os.makedirs(logsdir)
    logfilename = os.path.join(logsdir, logfilename)
    global logger
    log_file = open(logfilename, 'w', encoding="utf-8")
    logger = LoggersCollection([log_file])
    logger.write(f"=== Начало работы программы {datetime.datetime.now()} ===\n")

    if args.verbose:
        logger.append(sys.stdout)


def init_linker():
    """ Инициализация объекта, который ищет ключ.

    Raises:
        Exception: Не удалось открыть файл выгрузки БД.
    """

    try:
        db = pd.read_excel(args.db_export_file, args.db_export_sheet_name)
        global linker
        linker = Linker.load(db)
    except Exception as e:
        logger.write("Не удалось открыть файл выгрузки БД.\n")
        logger.write(f"{e}\n")
        raise e


def process(addres: str, exceptions_manager=None) -> tuple[Address, Any | None, str]:
    """ Обработка сырого адреса и получение его ключа.

    Args:
        addres (str): Сырой адрес.
        exceptions_manager (ExceptionsManager, optional): Менеджер исключений.

    Returns:
        tuple[Address, Any | None, str]: (адрес, ключ, сообщение)
    """
    raw = str(addres) if str(addres) !='nan' else None
    
    # Сначала проверяем в исключениях
    if exceptions_manager:
        key, message = exceptions_manager.get_key(raw)
        
        if key is not None:
            # Если адрес найден в исключениях, получаем правильный адрес
            correct_address = exceptions_manager.get_correct_address(raw)
            
            if correct_address:
                try:
                    # Создаем новый адрес из правильного варианта
                    addr = Address.fromStr(correct_address)
                    
                    # Сохраняем номер квартиры из исходного адреса, если возможно
                    try:
                        original_addr = Address.fromStr(raw)
                        addr.flat = original_addr.flat
                    except Exception:
                        pass
                    
                    logger.write(f'Сырой адрес: "{raw}" - Обработанный адрес: "{addr}"\n')
                    return addr, key, "Найден в исключениях"
                except Exception:
                    logger.write(f'Сырой адрес: "{raw}" - Адрес не существует\n')
                    return None, None, "Адрес не существует"
        elif message == "адрес не существует":
            logger.write(f'Сырой адрес: "{raw}" - Адрес не существует\n')
            return None, None, "Адрес не существует"
    
    # Если адрес не найден в исключениях или произошла ошибка, ищем в справочнике
    try:
        # Создаем временный объект ImprovedDatabaseOutputWorker для расширения адреса
        temp_worker = ImprovedDatabaseOutputWorker(None, None, None, None)
        expanded_address = temp_worker._expand_address_with_rules(raw)
        
        addr = Address.fromStr(expanded_address)
        key = linker.link(addr, require_flat_check=True)
        
        if key is None:
            logger.write(f'Сырой адрес: "{raw}" - Адрес не существует\n')
            return addr, None, "Адрес не существует"
            
        addr1 = linker.getvalue(key)
        addr1.flat = addr.flat
        
        logger.write(f'Сырой адрес: "{raw}" - Обработанный адрес: "{addr1}"\n')
        return addr1, key, ""
    except Exception:
        try:
            # Пытаемся разобрать адрес даже если не нашли соответствие
            addr = Address.fromStr(raw)
            logger.write(f'Сырой адрес: "{raw}" - Адрес не существует\n')
            return addr, None, "Адрес не существует"
        except Exception:
            logger.write(f'Сырой адрес: "{raw}" - Адрес не разобран\n')
            return None, None, "Адрес не разобран"


def process_excel(input_path: str, input_sheet: str, address_name: str, output_path: str, identity_column_name: str | None = None, progress_callback=None, exceptions_manager=None) -> ProcessingStats:
    stats = ProcessingStats()
    
    try:
        # Читаем только нужные колонки
        usecols = [address_name]
        if identity_column_name is not None:
            usecols.append(identity_column_name)
        input_file = pd.read_excel(input_path, input_sheet, usecols=usecols)
    except Exception as e:
        logger.write("Работа с файлом адресов не удалась.\n")
        logger.write(f"{e}\n")
        raise e
    
    try:
        outputWorker = SingleTableExcelOutputWorker(output_path, logger)
        total_processed = 0
        batch_size = 100
        
        def parse():
            nonlocal total_processed
            # Преобразуем DataFrame в список для более быстрой обработки
            data_list = input_file.to_dict('records')
            total_rows = len(data_list)
            
            for i, row in enumerate(data_list, 1):
                if progress_callback and i % batch_size == 0:
                    progress_callback(i)
                
                data = {
                    'address': None,
                    'key': None,
                    'raw': None,
                    'note': None
                }

                if identity_column_name is not None:
                    data[identity_column_name] = row[identity_column_name]
                
                data['raw'] = row[address_name]
                try:
                    data['address'], data['key'], message = process(data['raw'], exceptions_manager)
                    if message == "Адрес не существует":
                        data['note'] = message
                        stats.add_unprocessed(data['raw'], Exception(message))
                    elif message == "Адрес не разобран":
                        data['note'] = message
                        stats.add_unparsed(data['raw'], Exception(message))
                    elif message == "Найден в исключениях":
                        data['note'] = message
                        stats.add_success()
                    elif data['address'] is None or data['key'] is None:
                        data['note'] = "Адрес не существует"
                        stats.add_unprocessed(data['raw'], Exception("Адрес не был распознан"))
                    else:
                        stats.add_success()
                except Exception as e:
                    data['note'] = "Адрес не разобран"
                    stats.add_unparsed(data['raw'], e)
                
                # Всегда возвращаем DTO, даже если адрес не распознан
                yield AddressDTO(**data)
        
        outputWorker.save(parse())
    except Exception as e:
        logger.write("Не удалось выполнить обработку адресов.\n")
        logger.write(f"{e}\n")
        raise e
    
    return stats


def make_engine(dbms: str, user: str, password: str, host: str, port: str, db_name: str, **kwargs):
    dbms_cases = {
        "Oracle": 'oracle',
        "PostgreSQL": 'postgresql', 
        "MSSQL Server": 'mssql'
    }
    
    url = "{0}://{1}:{2}@{3}:{4}/{5}".format(dbms_cases[dbms], user, password, host, port, db_name)
    engine = create_engine(url, pool_pre_ping=True)
    with engine.connect():
        return engine


def process_db(dbms: str, user: str, password: str, host: str, port: str, db_name: str, schema: str, input_table_name: str, id_column: str, address_column: str, output_table_name: str, progress_callback=None, exceptions_manager=None) -> ProcessingStats:
    stats = ProcessingStats()
    
    try:
        engine = make_engine(dbms, user, password, host, port, db_name)
        
        # Проверяем доступность схемы
        inspector = inspect(engine)
        schemas = inspector.get_schema_names()
        print(f"Доступные схемы в БД: {schemas}")
        
        if schema not in schemas:
            raise Exception(f"Схема '{schema}' не найдена в базе данных. Доступные схемы: {schemas}")
        
        # Используем схему при создании метаданных
        metadata = MetaData(schema=schema)
        
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
        
        # Проверяем наличие нужных колонок
        if address_column not in [c.name for c in table.columns]:
            raise Exception(f"Колонка '{address_column}' не найдена в таблице. Доступные колонки: {[c.name for c in table.columns]}")
        
        if id_column is not None and id_column not in [c.name for c in table.columns]:
            raise Exception(f"Колонка '{id_column}' не найдена в таблице. Доступные колонки: {[c.name for c in table.columns]}")
        
        print(f"\nПараметры обработки:")
        print(f"  - Таблица: {actual_table_name}")
        print(f"  - Колонка адреса: {address_column}")
        print(f"  - Колонка ID: {id_column if id_column else 'не используется'}")
        print(f"  - Схема: {schema}")
        
        def generator():
            try:
                total_processed = 0
                batch_size = 100
                
                # Формируем запрос в зависимости от наличия ID-колонки
                if id_column is not None:
                    query = select(table.c[address_column], table.c[id_column])
                else:
                    query = select(table.c[address_column])
                
                # Выполняем запрос
                with engine.connect() as conn:
                    result = conn.execute(query)
                    
                    for row in result:
                        total_processed += 1
                        if progress_callback and total_processed % batch_size == 0:
                            progress_callback(total_processed)
                            
                        try:
                            if id_column is not None:
                                # Если есть колонка ID
                                address, id_val = row  # Меняем порядок - сначала адрес, потом ID
                                print(f"Обработка строки с адресом={address}, ID={id_val}")
                                
                                # Проверяем, что адрес не пустой
                                if address is None or str(address).strip() == '':
                                    print(f"Пропуск строки с пустым адресом (ID={id_val})")
                                    continue
                                    
                                addr, key, message = process(str(address), exceptions_manager)
                                
                                if addr is None:
                                    print(f"Не удалось обработать адрес: {address}")
                                    continue
                                    
                                # Создаем словарь с параметрами для AddressDTO
                                data = {
                                    'raw': str(address),  # Сохраняем исходный адрес
                                    'address': addr,
                                    'key': key,
                                    'ID': id_val  # Сохраняем ID
                                }
                                
                                if message == "Адрес не существует":
                                    data['note'] = message
                                    stats.add_unprocessed(str(address), Exception(message))
                                elif message == "Адрес не разобран":
                                    data['note'] = message
                                    stats.add_unparsed(str(address), Exception(message))
                                elif message == "Найден в исключениях":
                                    data['note'] = message
                                    stats.add_success()
                                elif addr is None or key is None:
                                    data['note'] = "Адрес не существует"
                                    stats.add_unprocessed(str(address), Exception("Адрес не был распознан"))
                                else:
                                    stats.add_success()
                                    
                                print(f"Данные для AddressDTO: {data}")
                                dto = AddressDTO(**data)
                                print(f"Созданный объект DTO: {dto.__dict__}")
                            else:
                                # Если колонки ID нет
                                address = row[0]
                                print(f"Обработка строки с адресом: {address}")
                                
                                # Проверяем, что адрес не пустой
                                if address is None or str(address).strip() == '':
                                    print(f"Пропуск строки с пустым адресом")
                                    continue
                                    
                                addr, key, message = process(str(address), exceptions_manager)
                                
                                data = {
                                    'raw': str(address),  # Сохраняем исходный адрес
                                    'address': addr,
                                    'key': key
                                }
                                
                                if message == "Адрес не существует":
                                    data['note'] = message
                                    stats.add_unprocessed(str(address), Exception(message))
                                elif message == "Адрес не разобран":
                                    data['note'] = message
                                    stats.add_unparsed(str(address), Exception(message))
                                elif message == "Найден в исключениях":
                                    data['note'] = message
                                    stats.add_success()
                                elif addr is None or key is None:
                                    data['note'] = "Адрес не существует"
                                    stats.add_unprocessed(str(address), Exception("Адрес не был распознан"))
                                else:
                                    stats.add_success()
                                    
                                dto = AddressDTO(**data)
                            
                            print(f"Обработка записи: raw={dto.raw}, key={dto.key}")
                            yield dto
                        except Exception as ex:
                            stats.add_unparsed(str(address) if address is not None else "неизвестный адрес", ex)
                            print(f"Ошибка при обработке строки: {ex}")
                            continue
            except Exception as e:
                logger.write("Не удалось прочитать данные из БД.")
                logger.write(str(e))
                raise e
            
        print(f"Создание ImprovedDatabaseOutputWorker с параметрами:")
        print(f"- input_table_name: {actual_table_name} (исходное: {input_table_name})")
        print(f"- output_table_name: {output_table_name}")
        print(f"- schema: {schema}")
        print(f"- id_column: {id_column}")
        
        # Используем улучшенную версию класса для записи в базу данных
        outputWorker = ImprovedDatabaseOutputWorker(
            engine=engine,
            input_table_name=actual_table_name,  # Используем актуальное имя таблицы
            output_table_name=output_table_name,
            schema=schema,
            id_column=id_column,
            logger=logger
        )
        outputWorker.save(generator())
    except Exception as e:
        logger.write(f'Ошибка сохранения данных в БД. Подробнее :{e}')
        raise e
    
    return stats

def check_connection(dbms: str, user: str, password: str, host: str, port: str, db_name: str, schema: str = None, **kwargs):
    """Проверяет подключение к базе данных и наличие указанной схемы."""
    try:
        engine = make_engine(dbms, user, password, host, port, db_name)
        with engine.connect() as conn:
            # Проверяем базовое подключение
            conn.execute(text("SELECT 1"))
            
            # Проверяем схему, если она указана
            if schema and schema.lower() != 'public':
                inspector = inspect(engine)
                schemas = inspector.get_schema_names()
                print(f"Доступные схемы в БД: {schemas}")
                
                if schema not in schemas:
                    raise Exception(f"Схема '{schema}' не найдена в базе данных. Доступные схемы: {schemas}")
    except Exception as e:
        raise Exception(f"Не удалось подключиться к базе данных: {str(e)}")


if __name__ == "__main__":
    p = make_args_parser()
    args = p.parse_args()

    make_logger()
    logger.write("Инициализация программы...\n")
    
    try:
        init_linker()
        logger.write("Linker успешно инициализирован\n")
        
        # Инициализируем менеджер исключений
        exceptions_manager = ExceptionsManager()
        
        if args.gui:
            logger.write("Запуск в режиме GUI\n")
            app = gui.make_gui(logger, process_excel, process_db, make_engine, check_connection)
            app.mainloop()
        else:
            logger.write("Запуск в консольном режиме\n")
            # Восстанавливаем консольный режим
            if not args.input_file or not args.output_file:
                logger.write("ОШИБКА: Для консольного режима необходимо указать входной и выходной файлы\n")
                sys.exit(1)
                
            try:
                logger.write(f"Обработка файла {args.input_file}...\n")
                stats = process_excel(
                    args.input_file,
                    args.input_sheet_name,
                    args.input_column_name,
                    args.output_file,
                    args.identity_column_name,
                    exceptions_manager=exceptions_manager
                )
                logger.write(stats.get_summary() + "\n")
                if stats.unprocessed > 0 or stats.unparsed > 0:
                    error_file = f"errors_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    stats.export_errors(error_file)
                    logger.write(f"Ошибки сохранены в файл: {error_file}\n")
            except Exception as e:
                logger.write(f"ОШИБКА при обработке файла: {str(e)}\n")
                sys.exit(1)
    except Exception as e:
        logger.write(f"КРИТИЧЕСКАЯ ОШИБКА: {str(e)}\n")
        sys.exit(1)
