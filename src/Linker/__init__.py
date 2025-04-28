"""
    Модуль 'привязки' (получения ключа (id) адреса из БД).
    Также содержит функционал приведения адреса к 'нормальному' (стандартному) виду.
    Нормализация организована на основе difflib, в частности расстояние Левенштейна.
"""

from .streetsFinder import StreetsFinder
from .exceptions import *
from .linker import Linker