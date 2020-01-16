import json
import re

from elasticpedia.config.elastic_conf import ElasticConfig
from elasticpedia.spark.session import *


class TurtleReader:

    def __init__(self):
        self._predicate2field = {
            "http://lexvo.org/ontology#label": ElasticConfig.Fields.SURFACE_FORM_KEYWORD.value,
            "http://dbpedia.org/property/refCount": ElasticConfig.Fields.REFCOUNT.value,
            "http://dbpedia.org/ontology/abstract": ElasticConfig.Fields.DESCRIPTION.value,
            "http://www.w3.org/2000/01/rdf-schema#comment": ElasticConfig.Fields.DESCRIPTION.value,
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type": ElasticConfig.Fields.CLASS.value,
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

    def _line_to_doc(self, rdf_tup, redirects):
        """
        Create a document, given a tuple (subj, pred, obj),
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
            .map(lambda rdf_tup: self._line_to_doc(rdf_tup, redirects)) \
            .filter(bool) \
            .reduceByKey(lambda d1, d2: {k: d1.get(k, []) + d2.get(k, []) for k in {*d1, *d2}}) \
            .map(lambda x: (x[0], json.dumps({**{ElasticConfig.Fields.URI.value: x[0]}, **x[1]})))
