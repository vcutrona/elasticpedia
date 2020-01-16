from enum import Enum


class ElasticConfig:

    def __init__(self,
                 resource: str,
                 nodes: str = "localhost",
                 port: int = 9200,
                 input_json: str = "yes",
                 mapping_id: str = None):
        self._nodes = nodes
        self._port = port
        self._resource = resource
        self._input_json = input_json
        self._mapping_id = mapping_id

    class Fields(Enum):
        URI = "uri"
        SURFACE_FORM_KEYWORD = "surface_form_keyword"
        SURFACE_FORM_PREFIX = "surface_form_prefix"
        REFCOUNT = "ref_count"
        DESCRIPTION = "description"
        CLASS = "class"
        CATEGORY = "category"
        TEMPLATE = "template"
        REDIRECT = "redirect"

    def get_config(self):
        cfg = {
            "es.nodes": self._nodes,
            "es.port": f'{self._port}',
            "es.resource": self._resource,
            "es.input.json": self._input_json,
        }

        if self._mapping_id:
            cfg["es.mapping.id"] = self._mapping_id

        return cfg
