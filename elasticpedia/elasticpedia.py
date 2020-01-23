"""Main module."""
import json

from elasticpedia.config.elastic_conf import ElasticConfig
from elasticpedia.reader.dbpedia_reader import TurtleReader


class DBpediaIndexer:

    def __init__(self, es_config: ElasticConfig):
        self._config = es_config.get_config()
        self._reader = TurtleReader()

    def index(self, data_files_path, redirects_files_path=None):
        """
        Index entities listed in the data dump to ElasticSearch,
        filtering out entities contained in the redirect files (if given).
        :param data_files_path: path to .ttl file(s) (e.g., /dbpedia/all_data/*.ttl)
        :param redirects_files_path: path to .ttl file(s) (e.g., /dbpedia/redirects/*.ttl)
        :return:
        """
        # Create the tuple (uri, doc) for each row
        # In addition, add the uri to the doc
        self._reader.get_documents_df(data_files_path, redirects_files_path) \
            .rdd \
            .map(lambda x: (x.subj, json.dumps({**x.doc, **{ElasticConfig.Fields.URI.value: x.subj}}))) \
            .saveAsNewAPIHadoopFile(path='-',
                                    outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                                    keyClass="org.apache.hadoop.io.NullWritable",
                                    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                                    conf=self._config
                                    )
