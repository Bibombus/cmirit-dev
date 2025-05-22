__all__ = ["AddressFact", "ADDRESS"]

from yargy import rule, or_, and_, not_
from yargy.interpretation import fact
from yargy.pipelines import morph_pipeline
from yargy.predicates import (
    gte,
    lte,
    eq,
    type,
    gram,
    is_title,
    normalized,
    caseless,
    in_caseless,
    dictionary,
)

from yargy.pipelines import caseless_pipeline


AddressFact = fact(
    "AddressFact",
    [
        "Index",
        "Country",
        "Region",
        "City",
        "Street",
        "House",
        "Corpus",
        "Stroenie",
        "Flat"
    ],
)

StreetFact = fact("Street", ["Name", "Type"])

LETTER = in_caseless(set("абвгдеёжзиклмнопрстуфхшщэюя"))
DASH = eq("-")
COMMA = eq(",")
SLASH = eq("/")
SPACE = eq(" ")
INT = type("INT")
NON_NEGATIVE_INT = and_(INT, gte(0), lte(1001))

ADJF = gram("ADJF")
NOUN = gram("NOUN")
TITLE = is_title()

# 1-ая, 2й, 3 ий и тд (до сотого) и 50-летия и тп
ANUM = rule(type("AlphaNumericWord"))

# пр-кт и прочие через дефис
DASHED_WORD = rule(type("DashedWord"))


######################
#
#   Начало правил
#
######################


# Индекс
INDEX = rule(and_(INT, gte(100000), lte(999999)))

# Страна
COUNTRY = rule(or_(dictionary({"россия"}), in_caseless({"рф"}))).interpretation(
    AddressFact.Country.const("Россия")
)

# Вспомогательное слово у названия региона
REGION_WORD = morph_pipeline(["область", "обл"])


# Название региона
REGION_NAME = dictionary({"Вологодская", "во"}).interpretation(AddressFact.Region)

# Регион полностью
REGION = rule(
    or_(
        rule(REGION_NAME),
        rule(REGION_NAME, REGION_WORD),
        rule(REGION_WORD, REGION_NAME),
    )
)

# Вспомогательное слово у названия города
CITY_WORD = or_(normalized("город"), normalized("гор"), caseless("г"))

# Название города
CITY_NAME_PREDICATE = normalized("череповец")
CITY_NAME = CITY_NAME_PREDICATE.interpretation(AddressFact.City.const("Череповец"))

# Город полностью
CITY = rule(
    or_(
        rule(CITY_NAME, CITY_WORD),
        rule(CITY_WORD, CITY_NAME),
        rule(CITY_NAME),
    )
)

# дом, д, д.
DOM_WORD = or_(normalized("дом"), caseless("д"))

HOUSE_LETTER = in_caseless(set("абвгдеёжзий"))

# номер (10), номер + буква (10А), номер + [буква] / + номер + [буква] (10А/5Б, 10/5, 10А/5, 10/5А и тд.)
DOM_NUMBER = or_(
    rule(
        NON_NEGATIVE_INT,
        HOUSE_LETTER.optional(),
        SLASH,
        NON_NEGATIVE_INT,
        HOUSE_LETTER.optional(),
    ),
    rule(NON_NEGATIVE_INT, HOUSE_LETTER.optional()),
).interpretation(AddressFact.House)


# Номер дома + вспомогательное слово
DOM = or_(rule(DOM_NUMBER), rule(DOM_WORD, DOM_NUMBER), rule(DOM_NUMBER, DOM_WORD))


# Вспомогательное слово корпуса
KORPUS_WORD = caseless_pipeline(["корпус", "корп", "кор", "к"])

# Номер корпуса
KORPUS_NUMBER = or_(NON_NEGATIVE_INT).interpretation(AddressFact.Corpus)

# Корпус
KORPUS = rule(KORPUS_WORD, KORPUS_NUMBER)


# Вспомогательное слово строения
STROENIE_WORD = caseless_pipeline(["строение", "стр", "с"])

# Номер строения
STROENIE_NOMBER = or_(NON_NEGATIVE_INT).interpretation(AddressFact.Stroenie)

# Строение
STROENIE = rule(STROENIE_WORD, STROENIE_NOMBER)


# квартира, кв, кв.,
KVARTIRA_WORD = or_(normalized("квартира"), caseless("кв"))

# Номер квартиры
KVARTIRA_NUMBER = rule(NON_NEGATIVE_INT).interpretation(AddressFact.Flat)

# Квартира + вспомогательное слово
KVARTIRA = or_(
    rule(KVARTIRA_NUMBER),
    rule(KVARTIRA_WORD, KVARTIRA_NUMBER),
    rule(KVARTIRA_NUMBER, KVARTIRA_WORD),
)

FULL_HOUSE_VARIANT = or_(
    rule(DOM, STROENIE.optional(), KORPUS.optional()),
    rule(DOM, KORPUS, STROENIE),
)

# дом кв, дом-кв, дом, кв дом
# (под дом и кв подразумеваются пары (в максимально полном случае) 
# токенов номера и ключевого слова (дом 10 и тд))
# UPD:  после добавления строений и корпусов добавилось промежуточное правило FULL_HOUSE_VARIANT
BUILDING = rule(
    or_(
        rule(FULL_HOUSE_VARIANT),
        rule(FULL_HOUSE_VARIANT, KVARTIRA),
        rule(DOM, or_(DASH, COMMA), KVARTIRA),
        rule(KVARTIRA, DOM),
    )
)

from .type_rules import *

# Имени кого-то
IMENI = rule(dictionary({"имени", "им"}))

# Аббревиатура имени
ABBR = rule(LETTER)

# Имя
NAME = rule(gram("Name"))

# Фамилия
SURNAME = rule(or_(gram("Surn")))

# Персона полностью
PERSON = rule(
    or_(
        rule(ABBR, SURNAME),
        rule(NAME, SURNAME),
        rule(SURNAME, NAME),
        SURNAME,
    )
)

# Специальные префиксы
SPECIAL_PREFIX = dictionary({
    "имени", "им", "протоиерея", "партизана", "космонавта", 
    "карла", "розы", "максима", "командарма", "сергея",
    "городского", "набережная", "соловецких", "подстанции"
})

# Специальные префиксы территории
TERRITORY_PREFIX = dictionary({
    "тер.", "территория", "территор", "тер"
})

# Предикат. Не является каким то ключевым словом (типом улицы, др вспомогательным словом)
NOT_STREET_TYPE_OR_OTHER_KEYWORD = not_(
    or_(
        ULITSA_PREDICATE,
        SHOSSE_PREDICATE,
        BULVAR_PREDICATE,
        LINIYA_PREDICATE,
        PEREULOK_PREDICATE,
        PROSPECT_PREDICATE,
        PROEZD_PREDICATE,
        PLOSHAD_PREDICATE,
        DOM_WORD,
        KVARTIRA_WORD,
        CITY_WORD,
        CITY_NAME_PREDICATE,
    )
)

# Существительное, не являющееся типом улицы или другим вспомогательным словом (дом, квартира, город)
NOUN_BUT_NO_STREET_TYPE = rule(and_(NOUN, NOT_STREET_TYPE_OR_OTHER_KEYWORD))

# Прилагательное, не являющееся типом улицы или другим вспомогательным словом (д, кв, г, пр, пл и т.д.)
ADJF_BUT_NO_STREET_TYPE = rule(and_(ADJF, NOT_STREET_TYPE_OR_OTHER_KEYWORD))

# Название улицы полностью
STREET_NAME = rule(
    or_(
        rule(ANUM, or_(rule(ADJF_BUT_NO_STREET_TYPE), NOUN_BUT_NO_STREET_TYPE)),
        rule(ADJF_BUT_NO_STREET_TYPE, NOUN_BUT_NO_STREET_TYPE),
        NOUN_BUT_NO_STREET_TYPE,
        rule(IMENI, PERSON),
        rule(PERSON, IMENI),
        rule(NOUN_BUT_NO_STREET_TYPE, NOUN_BUT_NO_STREET_TYPE),
        rule(ADJF_BUT_NO_STREET_TYPE, ADJF_BUT_NO_STREET_TYPE),
        rule(ADJF_BUT_NO_STREET_TYPE),
        ANUM,
        PERSON,
        # Правила для составных названий объектов
        rule(NOUN_BUT_NO_STREET_TYPE, ADJF_BUT_NO_STREET_TYPE),
        rule(ADJF_BUT_NO_STREET_TYPE, NOUN_BUT_NO_STREET_TYPE),
        # Добавляем правила для составных названий с префиксом "ИМЕНИ"
        rule(IMENI, SPECIAL_PREFIX, NOUN_BUT_NO_STREET_TYPE),
        rule(IMENI, SPECIAL_PREFIX, ADJF_BUT_NO_STREET_TYPE),
        rule(IMENI, SPECIAL_PREFIX, NOUN_BUT_NO_STREET_TYPE, NOUN_BUT_NO_STREET_TYPE),
        rule(IMENI, SPECIAL_PREFIX, ADJF_BUT_NO_STREET_TYPE, NOUN_BUT_NO_STREET_TYPE),
        rule(IMENI, SPECIAL_PREFIX, NOUN_BUT_NO_STREET_TYPE, ADJF_BUT_NO_STREET_TYPE),
        rule(IMENI, SPECIAL_PREFIX, ADJF_BUT_NO_STREET_TYPE, ADJF_BUT_NO_STREET_TYPE),
    )
).interpretation(StreetFact.Name)

# Улица с типом
STREET = (
    rule(
        or_(
            rule(TYPE.interpretation(StreetFact.Type), STREET_NAME),
            rule(STREET_NAME),
            rule(STREET_NAME, TYPE.interpretation(StreetFact.Type)),
            # Добавляем правило для территорий
            rule(TERRITORY_PREFIX, STREET_NAME),
        )
    )
    .interpretation(StreetFact)
    .interpretation(AddressFact.Street)
)

# Высокоуровневые административные элементы (индекс, страна, регион, город)
HIGHLEVEL_PARTS = rule(
    or_(
        rule(INDEX, COUNTRY, REGION, CITY),
        rule(COUNTRY, INDEX, REGION, CITY),
        rule(INDEX, COUNTRY, CITY),
        rule(INDEX, REGION, CITY),
        rule(INDEX, COUNTRY),
        rule(COUNTRY, REGION, CITY),
        rule(INDEX, CITY),
        rule(REGION, CITY),
        INDEX,
        COUNTRY,
        CITY,
        REGION,
    )
)

# Главное правило адреса
ADDRESS = rule(
    HIGHLEVEL_PARTS.optional(),
    STREET,
    BUILDING,
).interpretation(AddressFact)
