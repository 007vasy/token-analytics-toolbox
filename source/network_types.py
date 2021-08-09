from typing import List, Dict, Union
from dataclasses import dataclass, field

from constants import HASH_NODE_PROPERTY,SOURCE, TARGET

Properties = Dict[str, Union[str, int, float]]

@dataclass
class NetworkObj:
    properties: Properties = field(default_factory=dict)

    @staticmethod
    def property_to_neo4j_ready_str(properties: Properties = None) -> str:
        def property_type_checker(property_value):
            if isinstance(property_value, int) or isinstance(property_value, float):
                property_value
            elif isinstance(property_value, str):
                property_value = '''"''' + property_value.replace('"', r"\"") + '''"'''
            elif not property_value:
                property_value = "''"
            return property_value

        resp: str = ""
        if properties:
            resp = "{"
            for key in properties.keys():
                resp += """{key}:{value},""".format(
                    key=key, value=property_type_checker(properties[key])
                )
            resp = resp[:-1] + "}"
        return resp

    def get_neo4j_ready_properties(self):
        return self.property_to_neo4j_ready_str(self.properties)

    def get_neo4j_ready_labels(self):
        return ":".join(self.labels)

    def get_neo4j_batch_ready_properties(self):
        return "{" + ",".join([f'{p}:param.{p}' for p in self.properties.keys()]) +"}" if self.properties.keys() else ""

@dataclass
class Node(NetworkObj):
    labels: List[str] = field(default_factory=list)
    properties: Properties = field(default_factory=dict)

@dataclass
class Edge(NetworkObj):
    source: Node = field(default_factory=Node)
    target: Node = field(default_factory=Node)
    label: str = ""
    properties: Properties = field(default_factory=dict)

    def get_neo4j_ready_labels(self):
        return ":" + self.label

    def get_source_code_id(self):
        return f'"{self.source.properties[HASH_NODE_PROPERTY]}"'

    def get_target_code_id(self):
        return f'"{self.target.properties[HASH_NODE_PROPERTY]}"'


@dataclass
class Network:
    name: str
    nodes: List[Node]
    edges: List[Edge]

@dataclass
class EdgeInfo:
    source_label: str
    target_label: str

Network_Entities = List[Node or Edge]
Nodes = List[Node]
Edges = List[Edge]