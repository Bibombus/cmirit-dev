__all__ = [
    "OutputWorker",
    "SingleTableExcelOutputWorker",
    "AddressDTO",
    "LoggersCollection",
    "GUILogger",
    "DatabaseOutputWorker"
]

from abc import ABC, abstractmethod
from typing import Any, Iterable
import sys
from tkinter import Listbox

import pandas as pd
from sqlalchemy import Engine, Integer, create_engine, MetaData, Table, Column, String

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
    """DTO (Data Transfer Object) для обмена main и OutputWorker'a инфой о результатах парсинга и привязки"""

    def __init__(self, raw: str, address: Address = None, key: Any = None, **kwargs):
        """Конструктор.

        Args:
            raw (str): Сырой адрес.
            address (Address, optional): Разобранный адрес. По умолчанию = None.
            key (Any, optional): Ключ. По умолчанию = None.
            **kwargs(dict, optional): Дополнительные данные для записи с адресом. !!!При совпадениях с названиями имеющихся столбцов данные не будут записаны.
        """

        self.raw = raw if raw else None
        self.address = address if address else None
        self.key = key if address else None
        self.kwargs = kwargs

    def dict(self) -> dict:
        """Преобразует DTO к словарю нужного для вывода формата.

        Returns:
            dict: Словарь в нужном для вывода формате.
        """

        data = dict(self.kwargs)
        data["Address"] = self.raw
        if self.address:
            data.update(
                {
                    "Name": self.address.street.name,
                    "Type": str(self.address.street.type),
                    "House": self.address.house,
                    "Flat": self.address.flat,
                    "Key": self.key,
                }
            )
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
        if len(self.kwargs) > 0:
            result += f"; Доп. данные: {self.kwargs}"
        
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

    def __init__(self, path: str = "./output.xslx", logger: LoggersCollection = None):
        """Конструктор.

        Args:
            path (str, optional): Путь к файлу для записи. По умолчанию = "./output.xslx".
            logger (LoggersCollection, optional): Объект для логирования. По умолчанию будет создан вывод в консоль.
        """

        super().__init__(logger)
        self.path = path


    def save(self, addresses: Iterable[AddressDTO]):
        """Сохраняет данные в файл excel в одну таблицу.

        Args:
            addresses (Iterable[AddressDTO]): Коллекция с адресами и их ключами.
        """

        data = []
        for i in addresses:
            self.logger.write(f"{i}\n")
            data.append(i.dict())
        df = pd.DataFrame(data)
        try:
            with pd.ExcelWriter(self.path) as writer:
                df.to_excel(writer, index=False)
        except Exception as e:
            self.logger.write("Не удалось сохранить результаты в файл.\n")
            self.logger.write(f"{e}\n")
        finally:
            self.logger.flush()


class DatabaseOutputWorker(OutputWorker):
    """ Класс для записи результатов в БД. """


    def __init__(self, engine: Engine, output_table_name, id_col = None, logger: LoggersCollection = None):
        super().__init__(logger)
        self.engine = engine
        metadata = MetaData()

        # Определение новой таблицы
        columns = [
            Column('Address', String(50)),
            Column('Name', String(50)),
            Column('Type', String(50)),
            Column('House', String(50)),
            Column('Flat', String(50)),
            Column('Key', Integer)]

        if id_col is not None:
            columns.append(Column(id_col, String(50)))

        self.new_table = Table(
            output_table_name,
            metadata,
            *columns
        )

        # Создание таблицы
        metadata.create_all(engine)


    def save(self, addresses: Iterable[AddressDTO]):
        data = [i.dict() for i in addresses]
        with self.engine.connect() as connection:
            for record in data:
                try:    
                    command = self.new_table.insert().values(**record)
                    connection.execute(command)
                    connection.commit()
                except Exception as e:
                    self.logger.write("Не удалось сохранить результаты в базу данных.\n")
                    self.logger.write(f"{e}\n")
                finally:
                    self.logger.flush()