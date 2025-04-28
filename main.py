import argparse
import datetime
import sys
import os
from typing import Any
from enum import Enum

import pandas as pd
from sqlalchemy import create_engine, select, MetaData, Table, text

from src.AddresInfo import Address
from src.Linker import Linker
from src.OutputWorker import SingleTableExcelOutputWorker, AddressDTO, LoggersCollection
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


def process(addres: str) -> tuple[Address, Any | None]:
    """ Обработка сырого адреса и получение его ключа. Сама суть - распознали адрес, получили ключ, взяли адрес из выгрузки по ключу, отдали адрес и ключ.

    Args:
        addres (str): Сырой адрес.

    Returns:
        tuple[Address, Any | None]: Пара адрес и ключ
    """

    raw = str(addres) if str(addres) !='nan' else None
    addr = Address.fromStr(raw)
    key = linker.link(addr, require_flat_check=True)
    addr1 = linker.getvalue(key)
    addr1.flat = addr.flat
    return addr1, key


def process_excel(input_path: str, input_sheet: str, address_name: str, output_path: str, id_name: str | None = None, error_mode: ErrorHandlingMode = ErrorHandlingMode.STOP, progress_callback=None) -> ProcessingStats:
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
                }

                if id_name is not None:
                    data[id_name] = row[id_name]
                
                data['raw'] = row[address_name]
                try:
                    data['address'], data['key'] = process(data['raw'])
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


def process_db(dbms: str, user: str, password: str, host: str, port: str, db_name: str, input_table_name: str, id_column: str, address_column: str, output_table_name: str, error_mode: ErrorHandlingMode = ErrorHandlingMode.STOP, progress_callback=None) -> ProcessingStats:
    stats = ProcessingStats()
    
    try:
        engine = make_engine(dbms, user, password, host, port, db_name)
    except Exception as e:
        raise Exception(f'Ошибка при установлении подключения к БД. Подробнее: {e}')
    
    metadata = MetaData()
    table = Table(input_table_name, metadata, autoload_with=engine)

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
                batch_size = 100  # Размер пакета для обновления прогресса
                
                for row in result:
                    total_processed += 1
                    if progress_callback and total_processed % batch_size == 0:
                        progress_callback(total_processed)
                        
                    try:
                        if id_column is not None:
                            id_, address = row
                            addr, key = process(address)
                            data = {id_column: id_}
                            stats.add_success()
                            yield AddressDTO(address, addr, key, **data)
                        else:
                            address = row[0]
                            addr, key = process(address)
                            stats.add_success()
                            yield AddressDTO(address, addr, key)
                    except Exception as ex:
                        stats.add_failure(address, ex)
                        if error_mode == ErrorHandlingMode.STOP:
                            raise ex
                        continue
        except Exception as e:
            logger.write("Не удалось прочитать данные из БД.")
            logger.write(str(e))
            raise e
            
    try:
        outputWorker = DatabaseOutputWorker(engine, output_table_name, id_column, logger)
        outputWorker.save(generator())
    except Exception as e:
        logger.write(f'Ошибка сохранения данных в БД. Подробнее :{e}')
        raise e
    
    return stats

def check_connection(dbms: str, user: str, password: str, host: str, port: str, db_name: str, **kwargs):
    """Проверяет подключение к базе данных."""
    try:
        engine = make_engine(dbms, user, password, host, port, db_name)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
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
