"""
    Модуль вывода данных. Предоставляет возможности вывода результатов и логов в различные хранилища: 
    Excel, log-files, comsole, database(PostgreSQL, Oracle, MSSQL Server)
"""

from .outputWorker import *
from .improved_worker import ImprovedDatabaseOutputWorker

__all__ = [
    "OutputWorker",
    "SingleTableExcelOutputWorker",
    "AddressDTO",
    "LoggersCollection",
    "GUILogger",
    "DatabaseOutputWorker",
    "ImprovedDatabaseOutputWorker"
]