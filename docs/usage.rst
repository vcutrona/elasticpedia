=====
Usage
=====

Get the DBpedia datasets
------------------------
Download the DBpedia files containing documents to index (e.g., `DBpedia 2016 Core`_).
You can check which files are indexed by DBpedia Lookup here: https://github.com/dbpedia/lookup#get-the-following-dbpedia-datasets.

.. _DBpedia 2016 Core: http://downloads.dbpedia.org/2016-10/core/


Setup the environment
---------------------
Create the .env file with the following variables::

    SPARK_APPLICATION_NAME=<name of the app>  # default: elasticpedia
    SPARK_MASTER_URL=<local|spark-master-host|yarn>  # default: local
    ELASTIC_NODES=<comma-separated-list-of-elastic-nodes>  # default: localhost
    ELASTIC_INDEX_NAME=<index_name>
    ELASTIC_WAN_ONLY=<True|False>  # Default: False
    ALL_DATA_DIR=/path/to/all_data/*.ttl
    REDIRECTS_DIR=/path/to/redirect/links/*.ttl

Also, the elasticsearch-spark library must be available in Spark’s classpath.
If it is not the case, download the `ES-Hadoop`_ library (choose the version compatible with your Elasticsearch installation) and extract the elasticsearch-spark connector from the dist directory.

.. _ES-Hadoop: https://www.elastic.co/downloads/hadoop


Run your app
------------
Use Elasticpedia in your project (e.g., index.py)::

    import os

    from dotenv import load_dotenv

    from elasticpedia import elasticpedia
    from elasticpedia.config.elastic_conf import ElasticConfig

    load_dotenv()
    es_config = ElasticConfig(os.getenv("ELASTIC_INDEX_NAME"), mapping_id=ElasticConfig.Fields.URI.value)
    elasticpedia.DBpediaIndexer(es_config).index(os.getenv("ALL_DATA_DIR"), os.getenv("REDIRECTS_DIR"))

Run your project:

.. code-block:: console
    $ spark-submit --jars ./elasticsearch-spark-20_2.11-7.5.1.jar index.py

Usage with Docker
-----------------
We built a Docker image for running Elasticpedia in a dockerized environment. The images is based on the `BDE Docker Spark image`_.
Here is an example of docker-compose file with ElasticSearch 7.5.1 and Spark 2.4.4::

    version: '2.2'
    services:
      es01:
        image: docker.elastic.co/elasticsearch/elasticsearch:7.5.1
        container_name: es01
        environment:
          - node.name=es01
          - cluster.name=es-docker-cluster
          - cluster.initial_master_nodes=es01
          - bootstrap.memory_lock=true
          - ES_JAVA_OPTS=-Xms512m -Xmx512m
        ulimits:
          memlock:
            soft: -1
            hard: -1
        volumes:
          - es01data:/usr/share/elasticsearch/data
        ports:
          - 9200:9200
      dbpedia-indexer:
        image: vcutrona/dbpedia-indexer:1.0.0
        container_name: dbpedia-indexer
        environment:
          - ENABLE_INIT_DAEMON=false
          - ELASTIC_NODES=es01
          - ELASTIC_INDEX_NAME=dbpedia-df
          - ELASTIC_WAN_ONLY=true
        volumes:
          - /path/to/redirect/links/:/redirects
          - /path/to/all_data/:/all_data
      spark-master:
        image: bde2020/spark-master:2.4.4-hadoop2.7
        container_name: spark-master
        ports:
          - "8080:8080"
          - "7077:7077"
        environment:
          - INIT_DAEMON_STEP=setup_spark
        volumes:
          - /path/to/redirect/links/:/redirects
          - /path/to/all_data/:/all_data
      spark-worker:
        image: bde2020/spark-worker:2.4.4-hadoop2.7
        depends_on:
          - spark-master
        environment:
          - "SPARK_MASTER=spark://spark-master:7077"
        volumes:
          - /path/to/redirect/links/:/redirects
          - /path/to/all_data/:/all_data
    volumes:
      es01data:
        driver: local
        name: es01data

Run the following command to run the application with 1 master and 3 workers:

.. code-block:: console
    $ docker-compose up -d --scale spark-worker=3

.. _BDE Docker Spark image: https://github.com/big-data-europe/docker-spark
