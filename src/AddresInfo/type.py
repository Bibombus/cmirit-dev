__all__ = ['StreetType']

from typing import Generator
import enum
import yargy

from ..Rules import Tokenizer
from ..Rules.type_rules import TYPE_RULE


_parser = yargy.Parser(TYPE_RULE, Tokenizer()) 


class _StreetType:
    """
    Внутренний класс, не для использования вне этого файла.
    Предоставляет интерфейс взаимодействия с типами улиц:
    получения их имени и короткого написания.
    """


    def __init__(self, full_name: str, short_name: str):
        """Коструктор типа улицы.

        Args:
            full_name (str): Полное написание типа улицы.
            short_name (str): Короткое написание типа улицы.
        """

        self._name = full_name
        self._short = short_name


    @property
    def name(self) -> str:
        """Полное написание типа улицы

        Returns:
            str: Полное написание типа улицы
        """

        return self._name


    @property
    def short_name(self) -> str:
        """Короткое написание типа улицы

        Returns:
            str: Короткое написание типа улицы
        """

        return self._short


    def __eq__(self, __value: object) -> bool:
        """Оператор равенства. Сравнивае по типу объектов и полному написанию.

        Args:
            __value (object): Сравниваемый объект.

        Returns:
            bool: Результат сравнения на равенство.
        """

        return type(__value) == _StreetType and __value._name == self._name


    def __ne__(self, __value: object) -> bool:
        """Опертор неравенства. Возвращает инвертированный результат оператора равенства.

        Args:
            __value (object): Сравниваемый объект.

        Returns:
            bool: Результат сравнения.
        """

        return not self == __value


class StreetType(enum.Enum):
    """ Типы улиц в городе Череповец """
    
    
    BULVAR = _StreetType("бульвар", "Б-Р")
    """ Бульвар """
    STREET = _StreetType("улица", "УЛ.")
    """ Улица """
    LINIYA = _StreetType("линия", "ЛН.")
    """ Линия """
    PEREULOK = _StreetType("переулок", "ПЕР.")
    """ Переулок """
    PLOSHAD = _StreetType("площадь", "ПЛ.")
    """ Площадь """
    PROEZD = _StreetType("проезд", "ПР-Д")
    """ Проезд """
    PROSPECT = _StreetType("проспект", "ПР-КТ")
    """ Проспект """
    SHOSSE = _StreetType("шоссе", "Ш.")
    """ Шоссе """
    TERRITORIA = _StreetType("территория", "ТЕР.")
    """ Территория """


    def short_names() -> Generator[str, None, "StreetType"]:
        """Генератор коротких написаний типов улиц.

        Yields:
            Generator[str, None, StreetType]: Содердит короткие названия типов улиц.
        """

        for elem in StreetType:
            yield elem.value.short_name
    

    def full_names() -> Generator[str, None, "StreetType"]:
        """Генератор полных написаний типов улиц.

        Yields:
            Generator[str, None, StreetType]: Содердит полные названия типов улиц.
        """
        
        for elem in StreetType:
            yield elem.value.name


    @classmethod
    def fromStr(cls, value: str) -> "StreetType":
        """Создает объект типа улицы из строки.

        Args:
            value (str): Строковое написание типа улицы.

        Raises:
            ValueError: Тип улицы не распознан.
            ValueError: Неизвестный тип улицы.

        Returns:
            StreetType: Объект-перечисление типа улицы.
        """

        match = _parser.match(value)
        if not match:
            raise ValueError("Тип улицы не распознан.")
        
        match match.fact.value:
            case "УЛ.":
                return StreetType.STREET
            case "Ш.":
                return StreetType.SHOSSE
            case "Б-Р":
                return StreetType.BULVAR
            case "ЛН.":
                return StreetType.LINIYA
            case "ПЕР.":
                return StreetType.PEREULOK
            case "ПР-КТ":
                return StreetType.PROSPECT
            case "ПР-Д":
                return StreetType.PROEZD
            case "ПЛ.":
                return StreetType.PLOSHAD
            case "ТЕР.":
                return StreetType.TERRITORIA
            case _:
                raise ValueError("Неизвестный тип улицы.")
            

    def __str__(self) -> str:
        """Строковое представление типа улицы (короткое написание).

        Returns:
            str: Короткое написание типа улицы.
        """

        return self.value.short_name