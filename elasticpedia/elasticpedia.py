"""Main module."""
from elasticpedia.config.elastic_conf import ElasticConfig
from elasticpedia.reader.dbpedia_reader import TurtleReader


class DBpediaIndexer:

    def __init__(self, es_config: ElasticConfig):
        self._config = es_config.get_config()
        self._reader = TurtleReader()

    def index(self, data_files_path, redirects_files_path=None):
        self._reader.get_documents_rdd(data_files_path, redirects_files_path).saveAsNewAPIHadoopFile(
            path='-',
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=self._config
        )
