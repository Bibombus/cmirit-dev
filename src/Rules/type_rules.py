__all__ = [
        'ULITSA_PREDICATE',
        'SHOSSE_PREDICATE',
        'BULVAR_PREDICATE',
        'LINIYA_PREDICATE',
        'PEREULOK_PREDICATE',
        'PROSPECT_PREDICATE',
        'PROEZD_PREDICATE',
        'PLOSHAD_PREDICATE',
        'TYPE'
]

# Пришлось слить это сюда из файла address_rules, из-за циклических импортов
# Циклические импорты вызваны тем, что в модуле AddressInfo в самих классах сразу же объявлены методы парсинга из строк, 
# которые задействуют парсер на данных правилах.
from yargy import rule, or_
from yargy.interpretation import fact
from yargy.predicates import normalized, caseless, in_caseless


__Type = fact("Type", ["value"])

ULITSA_PREDICATE = or_(normalized("улица"), in_caseless({"ул", "у"}))
SHOSSE_PREDICATE = or_(normalized("шоссе"), caseless("ш"))
BULVAR_PREDICATE = or_(normalized("бульвар"), in_caseless({"б-р", "бр", "б"}))
LINIYA_PREDICATE = or_(normalized("линия"), in_caseless({"л-н", "лн", "л"}))
PEREULOK_PREDICATE = or_(normalized("переулок"), in_caseless({"пер", "пр"}))
PROSPECT_PREDICATE = or_(normalized("проспект"), in_caseless({"пр-кт", "пр", "пркт"}))
PROEZD_PREDICATE = or_(normalized("проезд"), in_caseless({"пр-д", "прд", "пр"}))
PLOSHAD_PREDICATE = or_(normalized("площадь"), in_caseless({"плщ", "пл"}))
TERRITORIA_PREDICATE = or_(normalized("территория"), caseless("тер"))

ULITSA = ULITSA_PREDICATE.interpretation(__Type.value.const("УЛ."))
SHOSSE = SHOSSE_PREDICATE.interpretation(__Type.value.const("Ш."))
BULVAR = BULVAR_PREDICATE.interpretation(__Type.value.const("Б-Р"))
LINIYA = LINIYA_PREDICATE.interpretation(__Type.value.const("ЛН."))
PEREULOK = PEREULOK_PREDICATE.interpretation(__Type.value.const("ПЕР."))
PROSPECT = PROSPECT_PREDICATE.interpretation(__Type.value.const("ПР-КТ"))
PROEZD = PROEZD_PREDICATE.interpretation(__Type.value.const("ПР-Д"))
PLOSHAD = PLOSHAD_PREDICATE.interpretation(__Type.value.const("ПЛ."))
TERRITORIA = TERRITORIA_PREDICATE.interpretation(__Type.value.const("ТЕР."))

# Тип улицы полностью
TYPE = or_(ULITSA, SHOSSE, BULVAR, LINIYA, PROSPECT, PEREULOK, PROEZD, PLOSHAD, TERRITORIA)

TYPE_RULE = rule(
    or_(
        ULITSA,
        SHOSSE,
        BULVAR,
        LINIYA,
        PEREULOK,
        PROSPECT,
        PROEZD,
        PLOSHAD,
        TERRITORIA
    )
).interpretation(__Type)