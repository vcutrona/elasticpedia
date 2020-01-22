import os
from enum import Enum


class ElasticConfig:

    def __init__(self,
                 resource: str,
                 nodes: str = "localhost",
                 port: int = 9200,
                 input_json: str = "yes",
                 wan_only: str = "false",
                 mapping_id: str = None):

        self._nodes = os.getenv("ELASTIC_NODES") if os.getenv("ELASTIC_NODES") else nodes
        self._port = os.getenv("ELASTIC_PORT") if os.getenv("ELASTIC_PORT") else port
        self._wan_only = os.getenv("ELASTIC_WAN_ONLY") if os.getenv("ELASTIC_WAN_ONLY") else wan_only
        self._resource = resource
        self._input_json = input_json
        self._mapping_id = mapping_id

    class Fields(Enum):
        URI = "uri"
        SURFACE_FORM_KEYWORD = "surface_form_keyword"
        SURFACE_FORM_PREFIX = "surface_form_prefix"
        REFCOUNT = "ref_count"
        DESCRIPTION = "description"
        TYPE = "type"
        CATEGORY = "category"
        TEMPLATE = "template"
        REDIRECT = "redirect"

    def get_config(self):
        cfg = {
            "es.nodes": self._nodes,
            "es.port": f'{self._port}',
            "es.resource": self._resource,
            "es.input.json": self._input_json,
            "es.nodes.wan.only": self._wan_only
        }

        if self._mapping_id:
            cfg["es.mapping.id"] = self._mapping_id

        return cfg
