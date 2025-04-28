'''
    Модуль содержащий правила для парсера, 
    а также функции, которые конструируют токенайзер и парсер по умолчанию.

    Файл 'tokenizer_rules.py' содержит правила для токенизатора.
    Файл 'tokenizer_rules.py' содердит правила для парсера.
    Файл 'type_rules.py' содердит правила для парсера.

'''

__all__ = ['Tokenizer', 'Parser']

import yargy
import yargy.tokenizer
from .tokenizer_rules import TOKENIZER_RULES, MorphTokenizer
from .address_rules import ADDRESS


def Tokenizer(rules: list[yargy.tokenizer.TokenRule]=TOKENIZER_RULES) -> MorphTokenizer:
    """Создает экземпляр токенайзера под нашу грамматику

    Args:
        rules (list[yargy.tokenizer.TokenRule], optional): Правила для токенизатора. По умолчанию = TOKENIZER_RULES.

    Returns:
        MorphTokenizer: Готовый к использованию токенизатор.
    """

    return MorphTokenizer(rules)


def Parser(address_rules: yargy.api.Rule = None, tokenizer: MorphTokenizer = None) -> yargy.Parser:
    """Создает экземпляр парсера для грамматики.

    Args:
        address_rules (yargy.api.Rule, optional): Правила для парсера. По умолчанию = .
        tokenizer (MorphTokenizer, optional): _description_. По умолчанию создастся токенизатор с помощью функциии Tokenize.

    Returns:
        yargy.Parser: Готовый к использованию парсер.
    """
    
    if address_rules is None:
        address_rules = ADDRESS
    if tokenizer is None:
        tokenizer = Tokenizer()
    return yargy.Parser(address_rules, tokenizer)