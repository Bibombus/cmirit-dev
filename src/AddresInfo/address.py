from .street import Street
from .type import StreetType
from ..Rules import Parser


class Address:
    """Класс содержащий информацию об адресе."""


    def __init__(self, street: Street, house: str, flat: int | None = None):
        """Конструктор адреса.

        Args:
            street (Street): Улица
            house (str): Номер дома
            flat (int | None, не обязательный): Квартира. По умолчанию None.
        """

        self.street = street
        self.house = house
        self.flat = flat


    @classmethod
    def fromStr(cls, address: str) -> "Address":
        """Создание нового объекта адреса из сырой строки. Используется парсер по умолчанию.
        
        Args:
            address (str): Сырой адрес.

        Raises:
            ValueError: Парсер не распознал адрес в строке.

        Returns:
            Address: Объект адреса, полученный из строки.
        """

        assert type(address) == str and len(address.strip()) != 0, "Адрес должен быть не пустой строкой."
        match = Address.__parser.match(address)
        
        if not match:
            raise ValueError("Не удалось разобрать входную строку на адрес.")

        addr = match.fact.__dict__
        street_ = addr["Street"]
        type_ = (
            StreetType.fromStr(street_.Type)
            if street_ and street_.Type is not None
            else None
        )
        street = Street(street_.Name, type_)
        
        flat = int(addr["Flat"]) if addr.get("Flat", None) else None
        house = addr["House"]
        if addr.get("Corpus", None):
            corpus = "К. " + str(addr["Corpus"])
            house = " ".join([house, corpus])

        if addr.get("Stroenie", None):
            stroenie = "СТР. " + str(addr["Stroenie"])
            house = " ".join([house, stroenie])

        return Address(street, house, flat)


    def copy(self) -> "Address":
        """Создает глубокую копию объекта.
        
        Returns:
            Address: Глубокая копия данного экземпляра.
        """

        return Address(self.street.copy(), self.house, self.flat)
    

    def __str__(self) -> str:
        """Строковое представление объекта адреса.

        Returns:
            str: Строковое представление объекта адреса.
        """
        
        items = [
            str(item)
            for item in [self.street, self.house, self.flat]
            if item is not None
        ]
        return str.join(", ", items)

    __parser = Parser()
