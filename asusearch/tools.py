# -*- coding: utf-8 -*-

import nltk
import re

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


def lemm_text(tokens: list, lang) -> list:
    result = []
    for token in tokens:
        if lang == '':
            lang = get_lang(token)
        if lang == 'ru' and not is_stopword(token, lang):
            result.append(morph.parse(token)[0].normal_form)
        elif lang == 'en' and not is_stopword(token, lang):
            result.append(lemmatizer.lemmatize(token))

    return result


# функции для получения "чистой" строки
STR_FILTERS = [strip_tags, strip_punctuation, strip_multiple_whitespaces]


def preprocess_string(s: str, lang: str = ''):
    s = utils.to_unicode(s)
    s = s.lower()
    s = s.replace('ё', 'е')

    for f in STR_FILTERS:
        s = f(s)

    tokens = s.split()
    tokens = lemm_text(tokens, lang)

    return tokens
