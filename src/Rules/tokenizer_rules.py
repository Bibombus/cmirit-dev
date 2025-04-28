from yargy.tokenizer import MorphTokenizer, TokenRule


TOKENIZER_RULES = [
    # 1-ая 50-летия
    TokenRule("AlphaNumericWord", r'(\d{1,3}-([а-яё]|[А-ЯЁ])+)'),

    # пр-кт л-н б-р
    TokenRule("DashedWord", r'([а-яё]|[А-ЯЁ])+-([а-яё]|[А-ЯЁ])+'),

    # Обычые слова
    TokenRule("RU", r'([а-яё]|[А-ЯЁ])+'),

    # Числа
    TokenRule("INT", r'\d+'),

    # Значимые знаки пунктуации (остальные отбрасываются на этапе токенизации)
    TokenRule("PUNCT", r'[/-]'),
]

def Tokenizer(rules=TOKENIZER_RULES) -> MorphTokenizer:
    return MorphTokenizer(rules)