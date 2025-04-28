class LinkerException(Exception):
    """
    Базовый класс для исключений, связанных с привязкой адреса к выгрузке из БД.
    Сообщение ошибки для дочерних классов подтягивается из doc-строки.
    """

    def __init__(self):
        super().__init__(
            "Ошибка привязки адреса"
            if type(self) == LinkerException
            else self.__doc__.strip()
        )


class NotInDBException(LinkerException):
    """
    Адрес не был найден в выгрузке из БД.
    """

    pass


class NoSuitableFlatRange(NotInDBException):
    """
    Адрес был найден, но квартира не соответствовала данным в выгрузке.
    """

    pass


class UnresolvedAmbigiuty(LinkerException):
    """
    Невозможно устранить неоднозначность в адресе.
    """

    pass


class NormalizationException(LinkerException):
    """
    Невозможно привести переданный адрес к нормальной форме.
    """

    pass
