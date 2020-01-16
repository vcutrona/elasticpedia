import json
import re
from urllib.parse import unquote_plus

from elasticpedia.config.elastic_conf import ElasticConfig
from elasticpedia.spark.session import *


class TurtleReader:

    def __init__(self):
        self._predicate2field = {
            "http://lexvo.org/ontology#label": ElasticConfig.Fields.SURFACE_FORM_KEYWORD.value,
            "http://www.w3.org/2000/01/rdf-schema#label": ElasticConfig.Fields.SURFACE_FORM_KEYWORD.value,
            "http://dbpedia.org/property/refCount": ElasticConfig.Fields.REFCOUNT.value,
            "http://dbpedia.org/ontology/abstract": ElasticConfig.Fields.DESCRIPTION.value,
            "http://www.w3.org/2000/01/rdf-schema#comment": ElasticConfig.Fields.DESCRIPTION.value,
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type": ElasticConfig.Fields.TYPE.value,
            "http://purl.org/dc/terms/subject": ElasticConfig.Fields.CATEGORY.value,
            "http://dbpedia.org/property/wikiPageUsesTemplate": ElasticConfig.Fields.TEMPLATE.value,
            "http://dbpedia.org/ontology/wikiPageRedirects": ElasticConfig.Fields.REDIRECT.value
        }

        self._regex = re.compile('^<(\S+)> <(\S+)> ["<]([\s\S]+)[">]')

    def _parse_ttl_line(self, line):
        """
        Convert a line of a .ttl into a tuple (subj, pred, obj).
        :param line: a line of a .ttl file.
        :return:
        """
        s = self._regex.match(line.value)
        return s.group(1), s.group(2), s.group(3)

    def _ttl_as_rdd(self, files_path):
        """
        Create an RDD from .ttl files contained in the given directory.
        Comments are filtered out and the RDD is already sorted.
        :param files_path: path to a file, or to a directory with many files in (e.g., /dbpedia/all_data/*)
        :return:
        """
        df = get_spark() \
            .read \
            .text(files_path)

        return df \
            .filter(~df.value.startswith('#')) \
            .drop_duplicates() \
            .orderBy(df.value) \
            .rdd \
            .map(self._parse_ttl_line)

    def _line_to_pair(self, rdf_tup, redirects):
        """
        Create a pair (subj, {pred: [obj]}), given a tuple (subj, pred, obj) (subj and obj
        are swapped when pred = redirect).
        Throw an Exception in the pred is not known.
        :param rdf_tup: a triple (subj, pred, obj)
        :param redirects: a list of redirected entities
        :return:
        """
        uri = rdf_tup[0]
        pred = rdf_tup[1]
        obj = rdf_tup[2]

        if pred not in self._predicate2field:
            raise Exception(f'No existing mapping for predicate: {pred}')

        field = self._predicate2field[pred]

        if len(redirects) == 0 or uri not in redirects:
            if field == ElasticConfig.Fields.REDIRECT.value:
                return obj, {field: [uri]}
            else:
                return uri, {field: [obj]}

    @staticmethod
    def _reduce_pairs_to_doc(p1, p2):
        """
        Merge two dicts that contain only lists as values.
        :param p1:
        :param p2:
        :return:
        """
        return {k: p1.get(k, []) + p2.get(k, []) for k in {*p1, *p2}}

    @staticmethod
    def _doc_to_json(row):
        """
        Convert documents to JSONs. Documents are also extended with
        new surface forms.
        :param row:
        :return:
        """
        uri, doc = row
        doc[ElasticConfig.Fields.URI.value] = uri
        if ElasticConfig.Fields.SURFACE_FORM_KEYWORD.value not in doc:
            doc[ElasticConfig.Fields.SURFACE_FORM_KEYWORD.value] = []
        doc[ElasticConfig.Fields.SURFACE_FORM_KEYWORD.value].append(
            unquote_plus(uri.replace("http://dbpedia.org/resource/", "")))
        doc[ElasticConfig.Fields.SURFACE_FORM_KEYWORD.value] = list(
            set(doc[ElasticConfig.Fields.SURFACE_FORM_KEYWORD.value]))
        return uri, json.dumps(doc)

    def _get_redirects(self, redirects_files_path):
        """
        Return the list of all the entities that are redirected to a main entity.
        All the files contained in the target directory must contain only triples
        with <http://dbpedia.org/ontology/wikiPageRedirects> as pred.
        :param redirects_files_path: path a one file, or to a directory with many files in (e.g., /dbpedia/redirects/*)
        :return:
        """
        rdd = self._ttl_as_rdd(redirects_files_path).cache()
        errors = rdd.filter(lambda x: x[1] != 'http://dbpedia.org/ontology/wikiPageRedirects').collect()

        if len(errors) > 0:
            raise Exception(f'predicate must be http://dbpedia.org/ontology/wikiPageRedirects')

        return rdd.map(lambda x: x[0]).distinct().collect()

    def get_documents_rdd(self, data_files_path, redirects_files_path):
        """
        Return the RDD containing the documents to be indexed.
        :param data_files_path: path to a file, or to a directory with many files in (e.g., /dbpedia/redirects/*)
        :param redirects_files_path: path to a file, or to a directory with many files in (e.g., /dbpedia/redirects/*).
        Redirected entities are discarded.
        :return:
        """
        redirects = []
        if redirects_files_path:
            redirects = self._get_redirects(redirects_files_path)
        return self._ttl_as_rdd(data_files_path) \
            .map(lambda rdf_tup: self._line_to_pair(rdf_tup, redirects)) \
            .filter(bool) \
            .reduceByKey(self._reduce_pairs_to_doc) \
            .map(self._doc_to_json)
