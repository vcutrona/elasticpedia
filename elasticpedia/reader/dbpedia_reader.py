from itertools import chain
from urllib.parse import unquote_plus

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, MapType, ArrayType

from elasticpedia.config.elastic_conf import ElasticConfig
from elasticpedia.spark.session import *
from elasticpedia.utils.string_utils import clean_space, split_camelcase


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

    @staticmethod
    def _ttl_as_df(files_path):
        """
        Create a DataFrame from .ttl files contained in the given directory.
        Comments are filtered out and the RDD is already sorted.
        The DataFrame containes 3 cols: subj, pred, obj
        :param files_path: path to .ttl file(s) (e.g., /dbpedia/all_data/*.ttl)
        :return:
        """
        ttl_regex = r'^<(\S+)> <(\S+)> ["<]([^">]+)[">].* '

        df = get_spark() \
            .read \
            .text(files_path)

        return df \
            .filter(~df.value.startswith('#')) \
            .dropDuplicates() \
            .select(F.regexp_extract(df.value, ttl_regex, 1).alias('subj'),
                    F.regexp_extract(df.value, ttl_regex, 2).alias('pred'),
                    F.regexp_extract(df.value, ttl_regex, 3).alias('obj'))

    @staticmethod
    def _extend_surface_forms(uri, doc):
        """
        Extend a doc by adding: (i) the uri of the entity, and (ii) new surface forms
        :param uri:
        :param doc:
        :return:
        """
        label = unquote_plus(uri.replace("http://dbpedia.org/resource/", ""))
        clean = clean_space(label)
        no_camel_case = split_camelcase(clean)
        labels = list({label,
                       clean,
                       no_camel_case,
                       label.lower(),
                       clean.lower(),
                       no_camel_case.lower()
                       })

        if ElasticConfig.Fields.SURFACE_FORM_KEYWORD.value not in doc:
            doc[ElasticConfig.Fields.SURFACE_FORM_KEYWORD.value] = []
        doc[ElasticConfig.Fields.SURFACE_FORM_KEYWORD.value] = \
            list({*doc[ElasticConfig.Fields.SURFACE_FORM_KEYWORD.value] + labels})

        return doc

    def _get_redirects(self, redirects_files_path):
        """
        Return the list of all the entities that are redirected to a main entity.
        All the files contained in the target directory must contain only triples
        with <http://dbpedia.org/ontology/wikiPageRedirects> as pred.
        :param redirects_files_path: path to .ttl file(s) (e.g., /dbpedia/redirects/*.ttl)
        :return:
        """
        df = self._ttl_as_df(redirects_files_path).cache()
        errors = df.select('pred').distinct().collect()

        if not (len(errors) == 1 and errors[0].pred == 'http://dbpedia.org/ontology/wikiPageRedirects'):
            raise Exception(f'Predicate must be http://dbpedia.org/ontology/wikiPageRedirects ({errors[0]} given).')

        return df.select('subj').distinct().collect()

    def get_documents_df(self, data_files_path, redirects_files_path):
        """
        Return the RDD containing the documents to be indexed. If redirects are provided, they
        will be filtered out.
        :param data_files_path: path to .ttl file(s) (e.g., /dbpedia/all_data/*.ttl)
        :param redirects_files_path:  path to .ttl file(s) (e.g., /dbpedia/redirects/*.ttl).
        Redirected entities are discarded.
        :return:
        """
        # Read data
        df = self._ttl_as_df(data_files_path)

        # Filter redirected entities, if any
        redirects = []
        if redirects_files_path:
            redirects = [row.subj for row in self._get_redirects(redirects_files_path)]
        df = df .filter(~df.subj.isin(redirects))

        # Replace RDF properties with index fields names
        mapping = F.create_map([F.lit(x) for x in chain(*self._predicate2field.items())])
        df = df \
            .withColumn('pred', mapping[df.pred]) \
            .dropna()  # remove unknown properties

        # Swap subj and obj when pred = redirect (store the relation as subj hasRedirect obj)
        df = df \
            .withColumn('subj_new', F.when(df.pred != ElasticConfig.Fields.REDIRECT.value, df.subj).otherwise(df.obj)) \
            .withColumn('obj_new', F.when(df.pred != ElasticConfig.Fields.REDIRECT.value, df.obj).otherwise(df.subj)) \
            .drop('subj', 'obj') \
            .select(F.col('subj_new').alias('subj'), F.col('pred'), F.col('obj_new').alias('obj'))

        # Group by subj,pred (multi-value predicates)
        df = df \
            .groupBy('subj', 'pred') \
            .agg(F.collect_list('obj').alias("obj"))

        # Create a document (dict) for each entity (subj)
        df = df \
            .groupBy('subj') \
            .agg(F.map_from_entries(F.collect_list(F.struct("pred", "obj"))).alias("doc"))

        # Extend the doc with the URI, new surface forms and convert it to a JSON.
        extend_surface_forms = F.udf(self._extend_surface_forms, MapType(StringType(), ArrayType(StringType())))
        df = df.withColumn('doc', extend_surface_forms('subj', 'doc'))

        return df
