# -*- coding: utf-8 -*-

import json
import numpy as np
import os
from gensim.corpora import Dictionary
from gensim.models import TfidfModel
from gensim.similarities import SparseMatrixSimilarity
from asusearch.tools import preprocess_string
from asusearch.models import Corrector

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

        self.corrector = Corrector()

        return True

    async def answer(self, query: str, batch_size: int, batch_i: int, type: int = -1) -> tuple:
        if not self.index_loaded:
            raise Exception('Search unavailable. Index not loaded: сall load()')

        lemm_cor_query = None

        # начало поисковой выдачи
        begin = batch_i * batch_size

        clean_query, lemm_query = preprocess_string(query)
        hmean_distances = await self.get_distances(lemm_query)

        # если нет результатов, проверяется раскладка
        if np.count_nonzero(hmean_distances) == 0:
            corrected_query = self.corrector.correct_keyboard_layout(query)
            # если раскладка исправлена, снова вычисляется расстояние
            if corrected_query != query:
                clean_cor_query, lemm_cor_query = preprocess_string(corrected_query)
                hmean_distances = await self.get_distances(lemm_cor_query)

        if np.count_nonzero(hmean_distances) == 0:
            return [], 0, query
        elif lemm_cor_query is not None:
            clean_query = clean_cor_query

        sorted_distances_id = hmean_distances.argsort()[::-1]
        nonzero_distances = np.nonzero(hmean_distances)[0]
        # id ненулевых расстояний в порядке убывания расстояний
        relevant_id = sorted_distances_id[np.in1d(sorted_distances_id, nonzero_distances)][:batch_size * 30]
        relevant_id = ', '.join(map(str, relevant_id + 1))

        if type != -1:
            type_cond = f' AND `type` = {type} '
        else:
            type_cond = ''

        # запрос на получение сведений о релевантных документах в порядке их релевантности
        query = f'''
                    SELECT `type`, `doc_id`, `lang_id`, `optional`, 1, COUNT() OVER () AS `count`
                    FROM (SELECT docs.*, ROW_NUMBER() OVER (ORDER BY docs.id) AS `order`
                    FROM docs)
                    WHERE `order` IN ({relevant_id}) {type_cond}
                    ORDER BY INSTR('{relevant_id}', `order`) LIMIT {batch_size} OFFSET {begin}'''

        results = self.connection.execute(query)
        results = results.fetchall()

        if len(results) > 0:
            size = results[0][5]
        else:
            size = 0

        docs = self.parse_results(results)

        return docs, size, clean_query

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

    @staticmethod
    def parse_results(results):
        docs = []
        for result in results:
            docs.append({
                'type': result[0],
                'doc_id': result[1],
                'lang_id': result[2],
                'score': result[4],
                'optional': json.loads(result[3])
            })

        return docs

    @staticmethod
    def form_subquery_scores(distances):

        values = []
        for i, d in enumerate(distances):
            if d > 0:
                values.append(f'({i + 1}, {d})')

        return f'WITH `scores` (`pos`, `score`) AS (VALUES {", ".join(values)}) SELECT * FROM `scores`'

    async def get_distances(self, lemm_query):
        query_bow = self.dictionary.doc2bow(lemm_query)
        # вычисление расстояний между запросом и текстами и их заголовками
        texts_distances = self.index_texts[self.tf_idf_model_texts[query_bow]]
        titles_distances = self.index_titles[self.tf_idf_model_titles[query_bow]]

        hmean_distances = self.beta_hmean(titles_distances, texts_distances)

        return hmean_distances

    async def complete(self, prefix, count):
        query = f'''SELECT DISTINCT `text` FROM (
                    SELECT `text`, `coef` * 10 AS `coef` FROM `autocomplete` WHERE `text` LIKE "{prefix.lower()}%"
                    UNION ALL
                    SELECT `text`, `coef` * 5 AS `coef` FROM `autocomplete` WHERE `text` LIKE "%{prefix.lower()}%")
                    ORDER BY `coef` DESC LIMIT {count}'''

        results = self.connection.execute(query).all()
        return [result[0] for result in results]
