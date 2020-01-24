"""Main module."""

from elasticpedia.config.elastic_conf import ElasticConfig
from elasticpedia.reader.dbpedia_reader import TurtleReader


class DBpediaIndexer:

    def __init__(self, es_config: ElasticConfig):
        self._config = es_config
        self._reader = TurtleReader()

    def index(self, data_files_path, redirects_files_path=None):
        """
        Index entities listed in the .ttl files to ElasticSearch,
        filtering out entities contained in the redirect files (if given).
        :param data_files_path: path to .ttl file(s) (e.g., /dbpedia/all_data/*.ttl)
        :param redirects_files_path: path to .ttl file(s) (e.g., /dbpedia/redirects/*.ttl)
        :return:
        """
        self._reader.get_documents_df(data_files_path, redirects_files_path) \
            .write \
            .format("org.elasticsearch.spark.sql") \
            .options(**self._config.options) \
            .mode("overwrite") \
            .save(self._config.index_name)
