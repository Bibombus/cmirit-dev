__all__ = ["StreetsFinder"]

from typing import Literal
import difflib

from ..AddresInfo import Street


class StreetsFinder:
    """
    Класс, позволяющий найти полное название улицы
    по ее различному написанию.
    """


    def __init__(self):
        """ Конструктор """
        
        from ..AbbrsInfo import Abbreviations
        self._abbrs = Abbreviations
        self.data = dict()


    def append(self, street: Street):
        """Добавляет в банк улицу (в ней также хранятся варианты ее написания).

        Args:
            street (Street): Улица, которую необходимо добавить в банк.
        """

        def _add(key, val):
            if key in self.data:
                if val not in self.data[key]:
                    self.data[key].append(val)
            else:
                self.data[key] = [val]


        _add(street.name, street)

        words = street.name.split()
        for word in words:
            if word in self._abbrs:
                for variant in self._abbrs[word]:
                    # Чтобы прообелы между словами лишние не появлялись. ДУмаю надо будет найти лучше вариант
                    tmp = str.join(" ", street.name.replace(word, variant).split())
                    _add(tmp, street)


    def remove(self, street: Street):
        """Удаляет улицу и все варианты ее написания из банка.

        Args:
            street (Street): Улица, которую необзодимо удалить из банка.
        """

        tmp = {}
        for k, v in self.data.items():
            # Если удаляемой улицы в этой паре нет, не изменяем
            if street not in v:
                tmp[k] = v
            else:
                # Если там не одна (а если одна то эта наша удалемая, просто не добавим пару ключ значение), 
                # то удалим из списка и вернем
                if len(v) != 1:
                    del v[v.index(street)]
                    tmp[k] = v
        self.data = tmp


    def get_variants(self, key: Street, CaseType: Literal["upper", 'lower', 'title'] = "upper") -> list[Street]:
        """Возвращает все возможные варианты написания улицы.

        Args:
            key (Street): Улица, для которой возвращаются варианты написания из банка.
            CaseType (Literal["upper", 'lower', 'title'], optional): Вид написания данных в БД. По умолчанию = "upper".

        Raises:
            TypeError: Улица имела неверный тип объекта (не Street).

        Returns:
            list[Street]: Список возможных написаний улицы.
        """

        if type(key) != Street:
            raise TypeError()
        
        name = key.name.strip()
        
        match CaseType:
            case "lower":
                name = name.lower()
            case "title":
                name = name.title()
            case "upper":
                name = name.upper()

        match_ = difflib.get_close_matches(name, self.data.keys(), 5, 0.55)

        if not match_:
            return []

        return self.data[match_[0]]


    def find(self, key: Street, CaseType: Literal["upper", 'lower', 'title'] = "upper") -> Street:
        """Возвращает полное написание переданной улицы.

        Args:
            key (Street): Улица, для которой ищется полное написание.
            CaseType (Literal["upper", 'lower', 'title'], optional): Вид записи данных в БД. По умолчанию = "upper".

        Raises:
            TypeError: Улица имела неверный тип объекта (не Street).

        Returns:
            Street: Полное написание улицы.
        """

        if type(key) != Street:
            raise TypeError()
        
        name = key.name.strip()
        
        match CaseType:
            case "lower":
                name = name.lower()
            case "title":
                name = name.title()
            case "upper":
                name = name.upper()

        match_ = difflib.get_close_matches(name, self.data.keys(), 5, 0.55)

        if not match_:
            return None 

        variants = self.data[match_[0]]

        # Если всего один вариант или key не имеет типа
        # отдаем все что нашли.
        # В случае нескольких вариантов сможем разрулить на некст шаге в link
        if len(variants) == 1 or key.type is None:
            return variants
        
        # А тут мы имеем несколько вариков и тип у key
        # значит можем выбрать подходящий
        for v in variants:
            if v.type == key.type:
                return [v]


    def __repr__(self) -> str:
        """Строковое представление содержимого банка написаний улиц.

        Returns:
            str: Строковое представление содержимого банка написаний улиц.
        """
        return self.data.__repr__()