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

linker: Linker | None = None
logger: LoggersCollection | None
args: argparse.Namespace | None = None


class ErrorHandlingMode(Enum):
    SKIP = "skip"  # Пропускать ошибки
    STOP = "stop"  # Останавливаться при ошибке

class ProcessingStats:
    def __init__(self):
        self.successful = 0
        self.failed = 0
        self.failed_addresses = []

    def add_success(self):
        self.successful += 1

    def add_failure(self, address: str, error: Exception):
        self.failed += 1
        self.failed_addresses.append((address, str(error)))

    def get_summary(self) -> str:
        return f"Результаты обработки:\n✓ Успешно: {self.successful} адресов\n✗ С ошибками: {self.failed} адресов"

    def export_errors(self, output_path: str):
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("Адрес,Ошибка\n")
            for addr, error in self.failed_addresses:
                f.write(f'"{addr}","{error}"\n')


def make_logger():
    """ Создает объект логера, который делает записи в различные источники. """

    logsdir = "./logs/"
    logfilename = f'{datetime.datetime.now().strftime("%Y.%m.%d_%H-%M-%S")}.log'
    if not os.path.exists(logsdir):
        os.makedirs(logsdir)
    logfilename = os.path.join(logsdir, logfilename)
    global logger
    logger = LoggersCollection([open(logfilename, 'w', encoding="utf-8")])

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
    print(f"Обработка адреса: {raw}")  # Отладочная информация
    
    # Сначала проверяем в исключениях
    if exceptions_manager:
        key, message = exceptions_manager.get_key(raw)
        print(f"Проверка в исключениях: key={key}, message={message}")  # Отладочная информация
        if key is not None:
            # Если адрес найден в исключениях, получаем правильный адрес
            correct_address = exceptions_manager.get_correct_address(raw)
            print(f"Правильный адрес из исключений: {correct_address}")  # Отладочная информация
            if correct_address:
                try:
                    # Создаем новый адрес из правильного варианта
                    addr = Address.fromStr(correct_address)
                    print(f"Создан адрес из правильного варианта: {addr}")  # Отладочная информация
                    
                    # Сохраняем номер квартиры из исходного адреса, если возможно
                    try:
                        original_addr = Address.fromStr(raw)
                        addr.flat = original_addr.flat
                    except Exception as e:
                        print(f"Не удалось получить квартиру из исходного адреса: {str(e)}")  # Отладочная информация
                        # Продолжаем работу без установки квартиры
                    
                    print(f"Итоговый адрес: {addr}")  # Отладочная информация
                    return addr, key, ""
                except Exception as e:
                    print(f"Ошибка при обработке правильного адреса: {str(e)}")  # Отладочная информация
                    return None, None, f"Ошибка при обработке правильного адреса: {str(e)}"
        elif message == "адрес не существует":
            # Если адрес помечен как несуществующий
            return None, None, message
    
    # Если адрес не найден в исключениях или произошла ошибка, ищем в справочнике
    try:
        addr = Address.fromStr(raw)
        print(f"Создан адрес из справочника: {addr}")  # Отладочная информация
        key = linker.link(addr, require_flat_check=True)
        addr1 = linker.getvalue(key)
        addr1.flat = addr.flat
        print(f"Итоговый адрес из справочника: {addr1}")  # Отладочная информация
        return addr1, key, ""
    except Exception as e:
        print(f"Ошибка при обработке адреса из справочника: {str(e)}")  # Отладочная информация
        # Если адрес не найден ни в справочнике, ни в исключениях
        return None, None, "адрес не найден"


def process_excel(input_path: str, input_sheet: str, address_name: str, output_path: str, id_name: str | None = None, error_mode: ErrorHandlingMode = ErrorHandlingMode.STOP, progress_callback=None, exceptions_manager=None) -> ProcessingStats:
    stats = ProcessingStats()
    
    try:
        # Читаем только нужные колонки
        usecols = [address_name]
        if id_name is not None:
            usecols.append(id_name)
        input_file = pd.read_excel(input_path, input_sheet, usecols=usecols)
    except Exception as e:
        logger.write("Работа с файлом адресов не удалась.\n")
        logger.write(f"{e}\n")
        raise e
    
    try:
        outputWorker = SingleTableExcelOutputWorker(output_path, logger)
        total_processed = 0
        batch_size = 100  # Размер пакета для обновления прогресса
        
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

                if id_name is not None:
                    data[id_name] = row[id_name]
                
                data['raw'] = row[address_name]
                try:
                    data['address'], data['key'], message = process(data['raw'], exceptions_manager)
                    if message:
                        data['note'] = message
                    stats.add_success()
                    yield AddressDTO(**data)
                except Exception as e:
                    stats.add_failure(data['raw'], e)
                    if error_mode == ErrorHandlingMode.STOP:
                        raise e
                    continue
        
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


def process_db(dbms: str, user: str, password: str, host: str, port: str, db_name: str, schema: str, input_table_name: str, id_column: str, address_column: str, output_table_name: str, error_mode: ErrorHandlingMode = ErrorHandlingMode.STOP, progress_callback=None, exceptions_manager=None) -> ProcessingStats:
    stats = ProcessingStats()
    
    try:
        engine = make_engine(dbms, user, password, host, port, db_name)
    except Exception as e:
        raise Exception(f'Ошибка при установлении подключения к БД. Подробнее: {e}')
    
    # Проверка схемы
    inspector = inspect(engine)
    schemas = inspector.get_schema_names()
    print(f"Доступные схемы в БД: {schemas}")
    
    if schema not in schemas:
        raise Exception(f"Схема '{schema}' не найдена в базе данных. Доступные схемы: {schemas}")
    
    # Проверка таблицы с учетом регистра
    tables = inspector.get_table_names(schema=schema)
    print(f"Доступные таблицы в схеме {schema}: {tables}")
    
    if input_table_name.lower() not in [t.lower() for t in tables]:
        raise Exception(f"Таблица '{input_table_name}' не найдена в схеме '{schema}'. Доступные таблицы: {tables}")
    
    # Находим точное имя таблицы с учетом регистра
    actual_table_name = None
    for table in tables:
        if table.lower() == input_table_name.lower():
            actual_table_name = table
            break
    
    metadata = MetaData(schema=schema)
    table = Table(actual_table_name, metadata, autoload_with=engine)

    def generator():
        try:
            with engine.connect() as conn:
                # Выбираем только нужные колонки
                columns = [table.c[address_column]]
                if id_column is not None:
                    columns.append(table.c[id_column])
                
                # Используем серверный курсор для потоковой обработки
                result = conn.execution_options(stream_results=True).execute(select(*columns))
                total_processed = 0
                batch_size = 100
                
                for row in result:
                    total_processed += 1
                    if progress_callback and total_processed % batch_size == 0:
                        progress_callback(total_processed)
                        
                    try:
                        if id_column is not None:
                            # Если есть колонка ID
                            id_val, address = row
                            print(f"Обработка строки с ID={id_val}, адрес={address}")
                            addr, key, message = process(address, exceptions_manager)
                            # Создаем словарь с параметрами для AddressDTO
                            # Важно: используем именно id_column как имя параметра
                            data = {}
                            data[id_column] = id_val  # Явно указываем имя колонки как ключ
                            if message:
                                data['note'] = message
                            print(f"Данные для AddressDTO: {data}")
                            dto = AddressDTO(address, addr, key, **data)
                            print(f"Созданный объект DTO: {dto.__dict__}")
                        else:
                            # Если колонки ID нет
                            address = row[0]
                            print(f"Обработка строки с адресом: {address}")
                            addr, key, message = process(address, exceptions_manager)
                            data = {}
                            if message:
                                data['note'] = message
                            dto = AddressDTO(address, addr, key, **data)
                        
                        print(f"Обработка записи: raw={dto.raw}, key={dto.key}")
                        stats.add_success()
                        yield dto
                    except Exception as ex:
                        stats.add_failure(address, ex)
                        print(f"Ошибка при обработке строки: {ex}")
                        if error_mode == ErrorHandlingMode.STOP:
                            raise ex
                        continue
        except Exception as e:
            logger.write("Не удалось прочитать данные из БД.")
            logger.write(str(e))
            raise e
            
    try:
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
        
        # Если по какой-то причине ImprovedDatabaseOutputWorker не сработал, можно раскомментировать старый вариант:
        # outputWorker = DatabaseOutputWorker(engine, input_table_name, output_table_name, schema, id_column, logger)
        # outputWorker.save(generator())
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
    init_linker()
    if args.gui:
        app = gui.make_gui(logger, process_excel, process_db, make_engine, check_connection)
        app.mainloop()
    else:
        print('Консольный режим временно отключен.')
