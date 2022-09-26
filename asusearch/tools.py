# -*- coding: utf-8 -*-
"""
Based by gensim.parsing.preprocessing
"""

import re
import nltk
from nltk.corpus import stopwords
from string import punctuation
from pymorphy2 import MorphAnalyzer
from nltk.stem import WordNetLemmatizer
from gensim import utils

RE_LATIN = re.compile(r'[A-Za-z]')


def contains_latin(s: str) -> bool:
    if re.search(RE_LATIN, s):
        return True
    else:
        return False


RE_TAGS = re.compile(r'<([^>]+)>', re.UNICODE)
RE_SPEC = re.compile(r'&[a-z]+?;', re.UNICODE)


def strip_tags(s: str) -> str:
    s = RE_TAGS.sub('', s)
    return RE_SPEC.sub('', s)


RE_PUNCT = re.compile('(\s+[%s]+)|([%s]+\s+)' % (re.escape(punctuation), re.escape(punctuation)), re.UNICODE)


def strip_punctuation(s):
    return RE_PUNCT.sub(' ', s)


RE_WHITESPACE = re.compile(r'(\s)+', re.UNICODE)


def strip_multiple_whitespaces(s: str) -> str:
    return RE_WHITESPACE.sub(' ', s)


RUS_STOPWORDS = stopwords.words('russian')
ENG_STOPWORDS = stopwords.words('english')


def remove_stopwords(tokens: list, lang: str = 'ru') -> list:
    if lang == 'ru':
        return [w for w in tokens if w not in RUS_STOPWORDS]
    elif lang == 'en':
        return [w for w in tokens if w not in ENG_STOPWORDS]
    else:
        return tokens


def strip_short(tokens: list, lang: str = 'ru') -> list:
    if lang == 'ru':
        minsize = 3
    else:
        minsize = 2
    return [t for t in tokens if len(t) >= minsize]


lemmatizer = WordNetLemmatizer()
morph = MorphAnalyzer(lang='ru')


def lemm_text(tokens: list, lang: str = 'ru') -> list:
    if lang == 'ru':
        return [morph.parse(token)[0].normal_form for token in tokens]
    elif lang == 'en':
        return [lemmatizer.lemmatize(token) for token in tokens]
    else:
        return tokens


# функции для получения "чистых" токенов
STR_FILTERS = [lambda x: x.lower(), strip_tags, strip_punctuation, strip_multiple_whitespaces]
# функции языковой обработки токенов
TOKENS_FILTERS = [strip_short, remove_stopwords, lemm_text]


def preprocess_string(s: str, lang: str = 'ru') -> list:
    s = utils.to_unicode(s)
    if lang == 'ru':
        s = s.replace('Ё', 'Е')
        s = s.replace('ё', 'е')

    for f in STR_FILTERS:
        s = f(s)

    tokens = s.split()
    for f in TOKENS_FILTERS:
        tokens = f(tokens, lang)

    return tokens
