# -*- coding: utf-8 -*-

import nltk
import re
import torch

from gensim import utils
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from pymorphy2 import MorphAnalyzer
from string import punctuation


RE_LATIN = re.compile(r'[A-Za-z]')
RE_CIRILL = re.compile(r'[А-Яа-я]')


def contains_latin(s: str) -> bool:
    if re.search(RE_LATIN, s):
        return True
    else:
        return False


def get_lang(s: str) -> str:
    if re.search(RE_LATIN, s):
        return 'en'
    elif re.search(RE_CIRILL, s):
        return 'ru'
    else:
        return 'none'


RE_TAGS = re.compile(r'<([^>]+)>', re.UNICODE)
RE_SPEC = re.compile(r'&[a-z]+?;', re.UNICODE)


def strip_tags(s: str) -> str:
    s = RE_TAGS.sub('', s)
    return RE_SPEC.sub('', s)


RE_PUNCT = re.compile('[%s]' % re.escape(punctuation), re.UNICODE)


def strip_punctuation(s):
    return RE_PUNCT.sub(' ', s)


RE_WHITESPACE = re.compile(r'(\s)+', re.UNICODE)


def strip_multiple_whitespaces(s: str) -> str:
    return RE_WHITESPACE.sub(' ', s)


RUS_STOPWORDS = stopwords.words('russian')
ENG_STOPWORDS = stopwords.words('english')


def is_stopword(token: str, lang: str = 'ru') -> list:
    if lang == 'ru' and token in RUS_STOPWORDS:
        return True
    elif lang == 'en' and token in ENG_STOPWORDS:
        return True
    else:
        return False


lemmatizer = WordNetLemmatizer()
morph = MorphAnalyzer(lang='ru')


def lemm_text(tokens: list) -> list:
    result = []
    for token in tokens:
        lang = get_lang(token)

        if lang == 'ru' and not is_stopword(token, lang):
            result.append(morph.parse(token)[0].normal_form)
        elif lang == 'en' and not is_stopword(token, lang):
            result.append(lemmatizer.lemmatize(token))

    return result


# функции для получения "чистой" строки
STR_FILTERS = [strip_tags, strip_punctuation, strip_multiple_whitespaces]


def preprocess_string(s: str) -> list:
    s = utils.to_unicode(s)
    s = s.lower()
    s = s.replace('ё', 'е')

    for f in STR_FILTERS:
        s = f(s)

    tokens = s.split()
    tokens = lemm_text(tokens)

    return ' '.join(tokens)


LEN_SENT = 50
CIRILL_TO_LATIN = {'й': 'q', 'ц': 'w', 'у': 'e', 'к': 'r', 'е': 't', 'н': 'y', 'г': 'u', 'ш': 'i', 'щ': 'o', 'з': 'p', 'х': '[', 'ъ': ']', 'ф': 'a', 'ы': 's', 'в': 'd', 'а': 'f', 'п': 'g', 'р': 'h', 'о': 'j', 'л': 'k', 'д': 'l', 'ж': ';', 'э': '\'', 'я': 'z', 'ч': 'x', 'с': 'c', 'м': 'v', 'и': 'b', 'т': 'n', 'ь': 'm', 'б': ',', 'ю': '.', 'ё': '`'}
LATIN_TO_CIRILL= {value: key for key, value in CIRILL_TO_LATIN.items()}
ALPHABET = {}
symbols = list(LATIN_TO_CIRILL.keys()) + list(CIRILL_TO_LATIN.keys())
for i, s in enumerate(symbols):
    ALPHABET[s] = i


def str_to_vector(s: str):
    vector = []
    other_idx = len(ALPHABET)
    for sym in s:
        if alphabet.get(sym):
            vector.append(ALPHABET[sym])
        else:
            vector.append(other_idx)

    return vector


def correct_keyboard_layout(self, s: str):
    """
    текст на русском - 0
    текст на английском - 1
    текст на англ. в кирилл. раскладке - 2
    текст на рус. в латинской раскладке - 3
    :param self:
    :param s:
    :return:
    """
    s = s.lower()
    vector = sent_to_vector(s)

    if len(vector) < LEN_SENT:
        vector += [66] * (LEN_SENT - len(x))
    else:
        vector = vector[:LEN_SENT]

    y = model(torch.tensor(vector))
    out = y[0].argmax()

    result = ''
    if out == 2:
        for sym in s:
            if CIRILL_TO_LATIN.get(sym):
                result += CIRILL_TO_LATIN[sym]
    elif out == 3:
        for sym in s:
            if LATIN_TO_CIRILL.get(sym):
                result += LATIN_TO_CIRILL[sym]
    else:
        result = s

    return result


