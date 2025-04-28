__all__ = ["Linker"]

from typing import Any, Literal
import pandas as pd

from ..AddresInfo import Street, StreetType, Address

from . import *


class Linker:
    """
    Производит привязку адреса к выгрузке из БД.
    """


    @classmethod
    def load(cls, db_dataframe: pd.DataFrame) -> "Linker":
        """Инициализирует новый линкер на основе выгрузки из БД.

        Args:
            db_dataframe (pd.DataFrame): Таблица выгрузки из БД в следующем формате: регистр=капс, названия полей(Key, Name, Type, House, Flat_start, Flat_end)

        Returns:
            Linker: Готовый к работе объект линковщика.
        """

        assert (
            "Type" in db_dataframe.columns
            and "Name" in db_dataframe.columns
            and "House" in db_dataframe.columns
            and "Flat_start" in db_dataframe.columns
            and "Flat_end" in db_dataframe.columns
            and "Key" in db_dataframe.columns
        ), "Файл выгрузки из БД неверного оформления. Требуемые названия полей: Key, Name, Type, House, Flat_start, Flat_end"
        
        instance = Linker()
        instance.df = db_dataframe
        instance.finder = Linker.__parse_metadata(db_dataframe)
        return instance


    @classmethod
    def __parse_metadata(cls, df: pd.DataFrame) -> StreetsFinder:
        """Извлекает данные из выгрузки с БД + перебирает различные варианты написания и добавляет их в банк finder`а. 

        Args:
            df (pd.DataFrame): Дата-фрейм с выгрузкой из БД.

        Returns:
            StreetsFinder: Готовый к использованию StreetsFinder.
        """
        
        finder = StreetsFinder()
        for _, row in df.iterrows():
            name, type_,  = row.Name, row.Type
            street = Street(name, StreetType.fromStr(type_))
            finder.append(street)
        return finder


    def getkey(self, address: Address, default_value: Any = None, require_flat_check: bool = True) -> Any:
        """Обертка над link методом. Не выбрасывает исключения, возвращая вместо этого значение по умолчанию.

        Args:
            address (Address): Адрес.
            default_value (Any, optional): Значение по умолчанию. По умолчанию = None.
            require_flat_check (bool, optional): Обязательна ли проверка на вхождение квартиры в диапазон. По умолчанию = True.

        Returns:
            Any: Ключ текущего адреса.
        """
        
        try:
            return self.link(address, require_flat_check)
        except LinkerException:
            return default_value
        

    def link(self, address: Address, require_flat_check: bool = True) -> int:
        """Добавляет к `адресу` `его` ID согласно выгрузке из БД.

        Args:
            address (Address): Адрес, чей ключ пытаемся получить.
            require_flat_check (bool, optional): Является ли проверка квартиры по диапазону в доме обязательной. По умолчанию = True.

        Raises:
            NormalizationException: Не удалось привести адрес к нормальной форме.
            NotInDBException: Адрес не был найден в выгрузке из БД.
            UnresolvedAmbigiuty: Решить неоднозначность не удалось.
        Returns:
            int: _description_
        """

        # Адреса без дома или названия улицы бесполезно обрабатывать
        assert address is not None and address.street is not None and address.house is not None, "Адрес, или его улица или дом не могут быть None."

        variants = self.finder.find(address.street)

        # Нормализовать не удалось, грустим
        if not variants:
            raise NormalizationException()

        # Всего 1 вариант, надо искать ключ в БД
        if len(variants) == 1:
            tmp_addr = address.copy()
            tmp_addr.street = variants[0]
            matches = self.__match_with_db(tmp_addr, require_flat_check)
            match len(matches):
                # Ключа нет 
                case 0:
                    raise NotInDBException()
                # Ключ один
                case 1:
                    return matches[0]
                # Несколько кандидатов, ситуация нерешаема
                case _: 
                    raise UnresolvedAmbigiuty()
                
        # Вариантов несколько, надо смотреть, может быть с ключом всего один
        res = []
        for v in variants:
            tmp_addr = address.copy()
            tmp_addr.street = v
            m = self.__match_with_db(tmp_addr, require_flat_check)
            if len(m) == 1:
                res.append(m[0])
        
        match len(res):
            case 0:
                raise NotInDBException()
            case 1:
                return res[0]
            case _: 
                raise UnresolvedAmbigiuty()


    def __match_with_db(self, address: Address, require_flat_check: bool = True, CaseType: Literal["upper", 'lower', 'title'] = "upper") -> list[int]:
        """ Находит `все возможные` варианты в выгрузке из БД.

        Args:
            address (Address):  Адрес, полученный из парсера.
            require_flat_check (bool, optional): Обязательна ли проверка диапазонов квартир. В случае отсутствия квартиры в адресе не влияет на результат. По умолчанию = True.
            CaseType (Literal['upper', 'lower', 'title'], optional): Вид записи данных в выгрузке БД. По умолчанию = "upper".

        Returns:
            list[int]: Список ключей всех возможных вариантов адресов.
        """

        match CaseType:
            case "upper":
                caseModifier = str.upper
            case "title":
                caseModifier = str.title
            case "lower":
                caseModifier = str.lower
        # Выбираем варики по названию и номеру дома - 100% они есть на данном этапе
        variants = self.df.loc[(self.df.Name == caseModifier(address.street.name)) & (self.df.House == caseModifier(address.house))]

        # Если есть тип улицы, лишние варианты откинем
        if address.street.type is not None:
            variants = variants.loc[variants.Type == caseModifier(address.street.type.value.short_name)]

        # Если квартира есть и просят проверить соответствие диапазонам
        if address.flat is not None and require_flat_check and len(variants) > 0:
            return Linker.__filter_by_flat_range(address.flat, variants)

        # Если квартиры нет или ее не просят проверять
        return [v.Key for _, v in variants.iterrows()]


    def __filter_by_flat_range(flat: int, variants: pd.DataFrame | pd.Series) -> list[int]:
        """Производит выборку из вариантов в соответствии с диапазонами квартир.

        Args:
            flat (int): Квартира в текущем адресе.
            variants (pd.DataFrame | pd.Series): Варианты адресов взятые из выгрузки БД. (тип указан не точно, но работает на выборку по тому же интерфейсу как и указанные)

        Returns:
            list[int]: Список ключей адресов, чьи диапазоны квартир удовлетворяют переданной квартире.
        """

        result = []
        for _, v in variants.iterrows():
            start = v.Flat_start
            end = v.Flat_end
            if start <= flat and flat <= end:
                result.append(v.Key)
        return result
    

    def getvalue(self, key: Any, default_value: Address = None) -> Address:
        """Возвращает адрес по заданному ключу. В случае отстутствия адреса вернет 'default_value'.

        Args:
            key (Any): Значение ключевого поля.
            default_value (Address, optional): Значение по умолчанию. По умолчанию = None.

        Returns:
            Address: Адрес, который имеет данный ключ.
        """
        
        self.df: pd.DataFrame
        rows = self.df.loc[self.df.Key == key]
        if len(rows) != 1:
            return default_value
        type_ ,name, house, _, _, _ = rows.iloc[0] 
        street = Street(name, StreetType.fromStr(type_))
        return Address(street=street, house=house)