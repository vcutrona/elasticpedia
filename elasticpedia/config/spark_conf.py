import os


class SparkConfig:

    def __init__(self, master: str = 'local', app_name: str = 'elasticpedia'):
        self._master = os.getenv("SPARK_MASTER_URL") if os.getenv("SPARK_MASTER_URL") else master
        self._app_name = os.getenv("SPARK_APPLICATION_NAME") if os.getenv("SPARK_APPLICATION_NAME") else app_name

    @property
    def master(self):
        return self._master

    @property
    def app_name(self):
        return self._app_name
