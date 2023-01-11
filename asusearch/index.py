# -*- coding: utf-8 -*-

import json
import os
from gensim.corpora import Dictionary
from gensim.corpora.mmcorpus import MmCorpus
from gensim.models import TfidfModel
from gensim.similarities import SparseMatrixSimilarity
from asusearch.tools import preprocess_string

import numpy as np
import sqlalchemy as sql
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session


class IndexBuilder:
    def __init__(self, path_prefix='index'):
        self.index_loaded = False

        # для преобразования текстов в векторы
        self.dictionary = Dictionary()
        self.corpus_texts = []
        self.corpus_titles = []

        if not os.path.exists(path_prefix):
            os.mkdir(path_prefix)

        if not os.path.exists(path_prefix + '/texts'):
            os.mkdir(path_prefix + '/texts')

        if not os.path.exists(path_prefix + '/titles'):
            os.mkdir(path_prefix + '/titles')

        # подключение к БД с информацией о документах
        self.engine = create_engine('sqlite:///' + path_prefix + '/docs.db', echo=True)
        self.session = Session(bind=self.engine)

        metadata_obj = sql.MetaData()
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

    async def load(self, path_prefix='index'):
        if os.path.exists(path_prefix) and os.path.isdir(path_prefix):
            if not os.listdir(path_prefix):
                raise FileNotFoundError(f'Index directory \'{path_prefix}\' is empty')

        self.dictionary = Dictionary.load(path_prefix + '/dictionary.dict')
        self.corpus_texts = list(MmCorpus(path_prefix + '/texts/corpus.mm'))
        self.corpus_titles = list(MmCorpus(path_prefix + '/titles/corpus.mm'))

        self.index_loaded = True

    async def save(self, path_prefix='index'):
        # отдельно сохраняется индекс текстов
        tf_idf_model_texts = TfidfModel(self.corpus_texts)
        SparseMatrixSimilarity(tf_idf_model_texts[self.corpus_texts], num_features=len(self.dictionary)).save(
            path_prefix + '/texts/index.index')

        tf_idf_model_texts.save(path_prefix + '/texts/tfidf_model.tfidf')
        MmCorpus.serialize(path_prefix + '/texts/corpus.mm', self.corpus_texts)

        # отдельно сохраняется индекс заголовков
        tf_idf_model_titles = TfidfModel(self.corpus_titles)
        SparseMatrixSimilarity(tf_idf_model_titles[self.corpus_titles], num_features=len(self.dictionary)).save(
            path_prefix + '/titles/index.index')

        tf_idf_model_titles.save(path_prefix + '/titles/tfidf_model.tfidf')
        MmCorpus.serialize(path_prefix + '/titles/corpus.mm', self.corpus_titles)

        self.dictionary.save(path_prefix + '/dictionary.dict')

        self.session.commit()

    # async def add(self, docs: list, optional_fields: list, lang: dict):
    #     lemmatize_texts = []
    #     lemmatize_titles = []
    #     for doc in docs:
    #         lemmas = preprocess_string(s=doc[4], lang=lang[str(doc[2])])
    #         lemmatize_texts.append(lemmas)
    #         lemmas = preprocess_string(s=doc[3], lang=lang[str(doc[2])])
    #         lemmatize_titles.append(lemmas)
    #
    #         _doc = {
    #             'type': doc[0],
    #             'doc_id': doc[1],
    #             'lang_id': doc[2],
    #         }
    #
    #         # дополнительные данные о документах в корпусе
    #         optional = dict()
    #         for field_i, field in enumerate(optional_fields):
    #             optional[field] = doc[field_i + 5]
    #
    #         if optional:
    #             _doc['optional'] = json.dumps(optional)
    #
    #         query_insert = sql.insert(self.docs_table, values=_doc)
    #         self.session.execute(query_insert)
    #
    #     self.dictionary.add_documents(lemmatize_texts)
    #     self.dictionary.add_documents(lemmatize_titles)
    #
    #     self.corpus_texts += [self.dictionary.doc2bow(text) for text in lemmatize_texts]
    #     self.corpus_titles += [self.dictionary.doc2bow(title) for title in lemmatize_titles]

        # return True

    # async def update(self, docs: list, optional_fields: list, lang: dict):
    #     if not self.index_loaded:
    #         raise Exception('Nothing update. Index not loaded: call load()')
    #
    #     for input_doc in docs:
    #         # получение порядкового номера документа в корпусе
    #         doc_order = self.get_doc_order(input_doc) - 1
    #
    #         if doc_order >= 0:
    #             lemmas_title = preprocess_string(s=input_doc[3], lang=lang[str(input_doc[2])])
    #             lemmas_text = preprocess_string(s=input_doc[4], lang=lang[str(input_doc[2])])
    #
    #             self.dictionary.add_documents([lemmas_title])
    #             self.dictionary.add_documents([lemmas_text])
    #
    #             self.corpus_titles[doc_order] = self.dictionary.doc2bow(lemmas_title)
    #             self.corpus_texts[doc_order] = self.dictionary.doc2bow(lemmas_text)
    #
    #             # обновление дополнительных данных о документах в корпусе
    #             optional = dict()
    #             for field_i, field in enumerate(optional_fields):
    #                 optional[field] = input_doc[field_i + 5]
    #
    #             if optional:
    #                 query_update = sql.update(self.docs_table, values={'optional': json.dumps(optional)}). \
    #                     where(sql.and_(self.docs_table.c.type == input_doc[0],
    #                                    self.docs_table.c.doc_id == input_doc[1],
    #                                    self.docs_table.c.lang_id == input_doc[2]))
    #                 self.session.execute(query_update)
    #
    #     return True

    async def update(self, docs: list, optional_fields: list, lang: dict):
        """
        [type, doc_id, lang_id, act, coef, title, text, optional]
        :param docs: список списков или кортежей вида [type, doc_id, lang_id, act, coef, title, text, optional]
        :param optional_fields: список названий дополнительных полей
        :param lang:
        :return:
        """
        for input_doc in docs:
            # получение порядкового номера документа в корпусе
            doc_id = self.get_doc_order(input_doc) - 1

            if input_doc[3] == 'del':
                if doc_id >= 0:
                    self.delete(input_doc, doc_id)
                continue

            lemmas_title = preprocess_string(s=input_doc[5], lang=lang[str(input_doc[2])])
            lemmas_text = preprocess_string(s=input_doc[6], lang=lang[str(input_doc[2])])

            self.dictionary.add_documents([lemmas_title])
            self.dictionary.add_documents([lemmas_text])

            # дополнительные данные о документах в корпусе
            optional = dict()
            # если доп. поля получены, но список полей пуст, он формируется далее
            if len(optional_fields) == 0:
                optional_fields = ['field_' + str(i) for i in range(len(doc) - 7)]

            for field_i, field in enumerate(optional_fields):
                optional[field] = doc[field_i + 7]

            # если doc_id = -1, значит, получен новый документ, и он просто добавляется
            if doc_id >= 0:
                self.corpus_titles[doc_id] = self.dictionary.doc2bow(lemmas_title)
                self.corpus_texts[doc_id] = self.dictionary.doc2bow(lemmas_text)

                query = sql.update(self.docs_table,
                                   values={self.docs_table.c.optional: json.dumps(optional),
                                           self.docs_table.c.coef: self.docs_table.c.coef * 1.05}). \
                    where(sql.and_(self.docs_table.c.type == input_doc[0],
                                   self.docs_table.c.doc_id == input_doc[1],
                                   self.docs_table.c.lang_id == input_doc[2]))

            else:
                self.corpus_texts.append(self.dictionary.doc2bow(lemmas_text))
                self.corpus_titles.append(self.dictionary.doc2bow(lemmas_title))

                _doc = {
                    'type': input_doc[0],
                    'doc_id': input_doc[1],
                    'lang_id': input_doc[2],
                    'coef': self.sigmoid(input_doc[4]),
                    'optional': json.dumps(optional)
                }

                query = sql.insert(self.docs_table, values=_doc)

            self.session.execute(query)
        self.session.commit()

        return True

    async def delete(self, doc: list, doc_id: int):
        query_delete = sql.delete(self.docs_table). \
            where(sql.and_(self.docs_table.c.type == doc[0],
                           self.docs_table.c.doc_id == doc[1],
                           self.docs_table.c.lang_id == doc[2]))

        self.session.execute(query_delete)

        self.corpus_texts.pop(doc_id)
        self.corpus_titles.pop(doc_id)

        return True

    # async def delete(self, docs: list):
    #     if not self.index_loaded:
    #         raise Exception('Nothing delete. Index not loaded: call load()')
    #
    #     for input_doc in docs:
    #         # получение порядкового номера документа в корпусе
    #         doc_order = self.get_doc_order(input_doc) - 1
    #
    #         if doc_order >= 0:
    #             query_delete = sql.delete(self.docs_table). \
    #                 where(sql.and_(self.docs_table.c.type == input_doc[0],
    #                                self.docs_table.c.doc_id == input_doc[1],
    #                                self.docs_table.c.lang_id == input_doc[2]))
    #
    #             self.session.execute(query_delete)
    #
    #             self.corpus_texts.pop(doc_order)
    #             self.corpus_titles.pop(doc_order)
    #
    #     return True

    async def delete_by_type(self, type_: int):
        if not self.index_loaded:
            raise Exception('Nothing delete. Index not loaded: call load_index()')

        query = '''
                SELECT `order`
                FROM (SELECT docs.*, ROW_NUMBER() OVER (ORDER BY docs.id) AS `order`
                FROM docs)
                WHERE `type` = {type}'''.format(type=type_)

        results = self.connection.execute(query)
        results = results.fetchall()

        for doc_order in results:
            self.corpus_texts.pop(doc_order[0])
            self.corpus_titles.pop(doc_order[0])

        query_delete = sql.delete(self.docs_table). \
            where(self.docs_table.c.type == type_)
        self.session.execute(query_delete)

    def get_doc_order(self, doc: tuple) -> int:
        """
        Возвращает doc_order - порядковый номер документа в корпусе,
        doc_order-1 является id строки в таблице индекса

        :param doc: tuple с параметрами
        :return: int doc_order или -1, если такой документ отсутствует
        """
        subquery = sql.select([func.row_number().over(order_by='id').label('order'), self.docs_table]).subquery()
        query_get_order = sql.select(subquery.c.order).where(sql.and_(subquery.c.type == doc[0],
                                                                      subquery.c.doc_id == doc[1],
                                                                      subquery.c.lang_id == doc[2]))
        result = self.session.execute(query_get_order).all()

        if result:
            return result[0][0]
        else:
            return -1

    async def clean_docs(self):
        """
        Очищает таблицу с информацией о документах.
        Требуется вызвать перед добавлением нового индекса.
        :return:
        """
        query = sql.delete(self.docs_table)
        self.session.execute(query)
        self.session.commit()

    async def stop(self):
        self.session.close()
        self.engine.dispose()

    @staticmethod
    def sigmoid(x):
        return 1/(1 + np.exp(-x))
