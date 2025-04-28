__all__ = ["Street"]

from types import NoneType

from .type import StreetType


class Street:
    """ Информация об улице. """


    def __init__(self, full_name: str, type_: StreetType):
        """Конструктор улицы.

        Args:
            full_name (str): Полное написание улицы
            type_ (StreetType): Тип элемента.
        """

        assert type(type_) in (StreetType, NoneType), "Передан неверный тип улицы."

        self.__name = full_name
        self.__type = type_


    @property
    def name(self) -> str:
        """Полное название улицы.

        Returns:
            str: Полное название улицы.
        """
        return self.__name


    @property
    def type(self) -> StreetType:
        """Тип улицы.

        Returns:
            StreetType: Тип улицы.
        """
        return self.__type


    def __eq__(self, __value: object) -> bool:
        """Оператор сравнения на равенство. Производит полное сравнение.

        Args:
            __value (object): Сравниваемый объект.

        Returns:
            bool: Результат сравнения.
        """

        return (
            isinstance(__value, Street)
            and __value.__name == self.__name
            and __value.__type == self.__type
        )


    def __ne__(self, __value: object) -> bool:
        """Оператор неравенства. Возвращает инвертированный результат оператора равенства.

        Args:
            __value (object): Сравниваемый объект.

        Returns:
            bool: Результат сравнения.
        """

        return not self == __value
    

    def __bool__(self) -> bool:
        """Оператор преобразования к булеву типу.

        Returns:
            bool: Результат преобразования.
        """

        return self.__name is not None
    

    def copy(self) -> "Street":
        """Создает копию объекта.

        Returns:
            Street: Копия объекта.
        """

        return Street(self.__name, self.__type)
    

    def __str__(self) -> str:
        """Строковое представление объекта.

        Returns:
            str: Строковое представление объекта.
        """
        
        items = [str(item) for item in [self.type, self.name] if item is not None]
        return str.join(", ", items)
