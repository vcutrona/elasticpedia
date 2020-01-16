from enum import Enum


class ElasticConfig:

    def __init__(self, nodes: str, port: int, resource: str, input_json: str, mapping_id: str):
        self._nodes = nodes
        self._port = port
        self._resource = resource
        self._input_json = input_json
        self._mapping_id = mapping_id

    class Fields(Enum):
        URI = "URI"
        SURFACE_FORM_KEYWORD = "SURFACE_FORM_KEYWORD"
        SURFACE_FORM_PREFIX = "SURFACE_FORM_PREFIX"
        REFCOUNT = "REFCOUNT"
        DESCRIPTION = "DESCRIPTION"
        CLASS = "CLASS"
        CATEGORY = "CATEGORY"
        TEMPLATE = "TEMPLATE"
        REDIRECT = "REDIRECT"

    def get_config(self):
        return {
            "es.nodes": self._nodes,
            "es.port": f'{self._port}',
            "es.resource": self._resource,
            "es.input.json": self._input_json,
            "es.mapping.id": self._mapping_id
        }
