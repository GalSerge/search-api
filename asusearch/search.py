# -*- coding: utf-8 -*-

import json
import numpy as np
import os
from gensim.corpora import Dictionary
from gensim.models import TfidfModel
from gensim.similarities import SparseMatrixSimilarity
from asusearch.tools import preprocess_string
from asusearch.tools import contains_latin

import sqlalchemy as sql
from sqlalchemy import create_engine


class Seacher():
    def __init__(self, path_prefix='index'):
        self.index_loaded = False

        # для преобразования запроса в вектор
        self.dictionary = None
        self.tf_idf_model_texts = None
        self.tf_idf_model_titles = None
        # матрица поиска
        self.index_texts = None
        self.index_titles = None

        # подключение к БД с информацией о документах
        metadata_obj = sql.MetaData()
        self.engine = create_engine('sqlite:///' + path_prefix + '/docs.db', echo=True)
        self.connection = self.engine.connect()

        self.docs_table = sql.Table('docs', metadata_obj,
                                    sql.Column('id', sql.Integer, primary_key=True),
                                    sql.Column('type', sql.Integer),
                                    sql.Column('doc_id', sql.Integer),
                                    sql.Column('lang_id', sql.Integer),
                                    sql.Column('coef', sql.Float, default=1.0),
                                    sql.Column('optional', sql.Text),
                                    )
        # если таблица docs отсутствует
        if not self.engine.has_table('docs'):
            metadata_obj.create_all(self.engine)

    async def load(self, path_prefix='index') -> bool:
        if os.path.exists(path_prefix) and os.path.isdir(path_prefix):
            if not os.listdir(path_prefix + '/texts'):
                raise FileNotFoundError(f'Index directory \'{path_prefix}\' is empty')
        else:
            raise NotADirectoryError(f'Doesn\'t exist a directory \'{path_prefix}\'')

        self.dictionary = Dictionary.load(path_prefix + '/dictionary.dict')

        self.index_texts = SparseMatrixSimilarity.load(path_prefix + '/texts/index.index')
        self.tf_idf_model_texts = TfidfModel.load(path_prefix + '/texts/tfidf_model.tfidf')

        self.index_titles = SparseMatrixSimilarity.load(path_prefix + '/titles/index.index')
        self.tf_idf_model_titles = TfidfModel.load(path_prefix + '/titles/tfidf_model.tfidf')

        self.index_loaded = True

        return True

    async def answer(self, query: str, batch_size: int, batch_i: int, type: int = 0) -> tuple:
        if not self.index_loaded:
            raise Exception('Search unavailable. Index not loaded: сall load_index()')

        # начало поисковой выдачи
        begin = batch_i * batch_size

        lemm_query = preprocess_string(query)
        hmean_distances = self.get_distances(lemm_query)

        # если нет результатов, проверяется раскладка
        if np.count_nonzero(hmean_distances, axis=1)[0] == 0:
            query = correct_keyboard_layout(query)
            # если раскладка исправлена, снова вычисляется расстояние
            if query != lemm_query:
                right_query = query
                lemm_query = preprocess_string(query)
                hmean_distances = self.get_distances(lemm_query)

        coefs = self.get_coefs()
        hmean_distances = hmean_distances * coefs

        sorted_distances_id = hmean_distances.argsort()[::-1]
        nonzero_distances = np.nonzero(hmean_distances)[0]
        # id ненулевых расстояний в порядке убывания их величины
        relevant_id = sorted_distances_id[np.in1d(sorted_distances_id, nonzero_distances)]

        query_params = {
            'relevant_id': ', '.join(map(str, relevant_id + 1)),
            'batch_size': batch_size,
            'begin': begin
        }
        if type != 0:
            query_params['type_cond'] = 'AND `type` = ' + type
        else:
            query_params['type_cond'] = ''

        # запрос на получение сведений о релевантных документах в порядке их релевантности
        query = '''
                SELECT `type`, `doc_id`, `lang_id`, `optional`, `coef`
                FROM (SELECT docs.*, ROW_NUMBER() OVER (ORDER BY docs.id) AS `order`
                FROM docs)
                WHERE `order` IN ({relevant_id})
                {type_cond}
                ORDER BY INSTR('{relevant_id}', `order`)
                LIMIT {batch_size} OFFSET {begin}}'''.format(**query_params)

        results = self.connection.execute(query)
        results = results.fetchall()

        return hmean_distances[relevant_id], results, len(nonzero_distances), query

    @staticmethod
    def beta_hmean(a, b):
        """
        Вычисляет среднее гармоническое, отдавая приоритет значениям из массива a
        :param a: numpy.ndarray
        :param b: numpy.ndarray
        :return: numpy.ndarray
        """
        beta = 0.1

        # нормализация, чтобы не обнулялись расстояния
        # if np.sum(a) != 0 or np.sum(b) != 0:
        #     a += 1
        #     b += 1

        non_zero = np.logical_or(a != 0, b != 0)
        a[non_zero] += 1
        b[non_zero] += 1

        return np.divide((1 + beta ** 2) * a * b, (a * beta + b), out=np.zeros_like(a), where=b != 0)

    async def stop(self):
        self.connection.close()
        self.engine.dispose()

    async def get_coefs(self):
        query = 'SELECT `coef` FROM docs'
        results = self.connection.execute(query)
        results = results.fetchall()

        return np.array(results)

    async def get_distances(self, lemm_query):
        query_bow = self.dictionary.doc2bow(lemm_query)
        # вычисление расстояний между запросом и текстами и их заголовками
        texts_distances = self.index_texts[self.tf_idf_model_texts[query_bow]]
        titles_distances = self.index_titles[self.tf_idf_model_titles[query_bow]]

        hmean_distances = self.beta_hmean(titles_distances, texts_distances)

        return hmean_distances
