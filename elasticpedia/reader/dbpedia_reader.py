from itertools import chain
from urllib.parse import unquote_plus

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType, StructField, StructType

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
    def _surface_forms_from_uri(uri):
        """
        Return surfaces forms extracted from the URI
        :param uri:
        :param doc:
        :return:
        """
        label = unquote_plus(uri.replace("http://dbpedia.org/resource/", ""))
        clean = clean_space(label)
        no_camel_case = split_camelcase(clean)
        return list({label,
                     clean,
                     no_camel_case,
                     label.lower(),
                     clean.lower(),
                     no_camel_case.lower()
                     })

    def _get_redirects(self, redirects_files_path):
        """
        Return a DataFrame containing the entities that are redirected to a main entity.
        All the files contained in the target directory must contain only triples
        with <http://dbpedia.org/ontology/wikiPageRedirects> as predicate.
        :param redirects_files_path: path to .ttl file(s) (e.g., /dbpedia/redirects/*.ttl)
        :return:
        """
        if not redirects_files_path:
            schema = StructType([StructField("subj", StringType())])
            return get_spark().createDataFrame([], schema)

        df = self._ttl_as_df(redirects_files_path).cache()
        errors = df.select('pred').distinct().cache()

        if not (errors.count() == 1 and errors.take(1)[0].pred == 'http://dbpedia.org/ontology/wikiPageRedirects'):
            raise Exception(f'Predicate must be http://dbpedia.org/ontology/wikiPageRedirects ({errors[0]} given).')

        return df.select('subj').distinct()

    def get_documents_df(self, data_files_path, redirects_files_path):
        """
        Return a DataFrame containing the entities to be indexed.
        Redirects are filtered out, if given.
        :param data_files_path: path to .ttl file(s) (e.g., /dbpedia/all_data/*.ttl)
        :param redirects_files_path:  path to .ttl file(s) (e.g., /dbpedia/redirects/*.ttl).
        :return:
        """
        # DF schema: subj, pred, obj
        df = self._ttl_as_df(data_files_path)

        # Filter redirected entities, if any
        redirects = self._get_redirects(redirects_files_path)
        df = df.join(redirects, df.subj == redirects.subj, 'left_anti')

        # Replace RDF properties with index fields names
        mapping = F.create_map([F.lit(x) for x in chain(*self._predicate2field.items())])
        df = df \
            .withColumn('pred', mapping[df.pred]) \
            .dropna()  # remove unknown properties

        # Swap subj and obj when pred = redirect (store the relation as subj hasRedirect obj)
        # Make subj the uri col
        uri_col = ElasticConfig.Fields.URI.value
        df = df \
            .withColumn(uri_col, F.when(df.pred != ElasticConfig.Fields.REDIRECT.value, df.subj).otherwise(df.obj)) \
            .withColumn('obj_new', F.when(df.pred != ElasticConfig.Fields.REDIRECT.value, df.obj).otherwise(df.subj)) \
            .drop('subj', 'obj') \
            .select(F.col(uri_col), F.col('pred'), F.col('obj_new').alias('obj'))

        # Pivot table grouping by uri; collect objects into lists
        df = df.groupBy(uri_col).pivot("pred").agg(F.collect_list('obj'))

        # Add a column with extra surface forms
        extra_surface_forms = F.udf(self._surface_forms_from_uri, ArrayType(StringType()))
        df = df.withColumn("extra_surface_forms", extra_surface_forms(uri_col))

        # If the surface forms column already exists, merge it with the new one
        if ElasticConfig.Fields.SURFACE_FORM_KEYWORD.value in df.columns:
            merge_surface_forms = F.udf(lambda sf1, sf2: list({*sf1 + sf2}), ArrayType(StringType()))
            df = df \
                .withColumn(ElasticConfig.Fields.SURFACE_FORM_KEYWORD.value,
                            merge_surface_forms(ElasticConfig.Fields.SURFACE_FORM_KEYWORD.value,
                                                'extra_surface_forms')) \
                .drop('extra_surface_forms')
        else:  # else just rename the new one
            df = df.withColumnRenamed('extra_surface_forms', ElasticConfig.Fields.SURFACE_FORM_KEYWORD.value)

        return df
