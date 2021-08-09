from neo4j import GraphDatabase
from typing import List, Dict

from constants import HASH_NODE_PROPERTY,NEO4J_URL_KEYWORD,NEO4J_USER_KEYWORD,NEO4J_SECRET_KEYWORD, NAME_NODE_PROPERTY
from network_types import Network, Node, Edge, Network_Entities
import os


class Neo4jHandler:
    def __init__(self):
        uri = os.getenv(NEO4J_URL_KEYWORD,"bolt://localhost:7687")
        user = os.getenv(NEO4J_USER_KEYWORD,"neo4j")
        password = os.getenv(NEO4J_SECRET_KEYWORD,"letmein")
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.write_transaction(
                self._create_and_return_greeting, message
            )
            print(greeting)

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run(
            "CREATE (a:Greeting) "
            "SET a.message = $message "
            "RETURN a.message + ', from node ' + id(a)",
            message=message,
        )
        return result.single()[0]

    @staticmethod
    def _generate_node_query(node:Node):
        return f"""MERGE (a:{node.get_neo4j_ready_labels()} {node.get_neo4j_ready_properties()}) RETURN a;"""

    @staticmethod
    def _generate_edge_query(edge:Edge):
        return f"""
                MATCH (s)
                WHERE s.{HASH_NODE_PROPERTY} = {edge.get_source_code_id()}
                MATCH (t)
                WHERE t.{HASH_NODE_PROPERTY} = {edge.get_target_code_id()}
                MERGE (s)-[:{edge.label} {edge.get_neo4j_ready_properties()}]->(t)
                RETURN s,t;"""

    def _insert_node(self, tx, node: Node):
        result = tx.run(
            self._generate_node_query(node)
        )
        return result.single()[0]

    def _insert_edge(self, tx, edge: Edge):
        result = tx.run(
            self._generate_edge_query(edge)
        )
        return result.single()[0]

    def insert_nodes(self, nodes: List[Node]):
        with self.driver.session() as session:
            for node in nodes:
                session.write_transaction(self._insert_node, node)

    def insert_edges(self, edges: List[Edge]):
        with self.driver.session() as session:
            for edge in edges:
                session.write_transaction(self._insert_edge, edge)

    @staticmethod
    def generate_batch_node_query(nodes: List[Node]):
        return f"UNWIND $params as param MERGE (n:{nodes[0].get_neo4j_ready_labels()} {nodes[0].get_neo4j_batch_ready_properties()});"

    @staticmethod
    def generate_batch_edge_query(edges: List[Edge]):
       
        return f"""
                UNWIND $params as param  
                MATCH (s)
                WHERE s.{HASH_NODE_PROPERTY} = param.source_hash
                MATCH (t)
                WHERE t.{HASH_NODE_PROPERTY} = param.target_hash
                MERGE (s)-[{edges[0].get_neo4j_ready_labels()} {edges[0].get_neo4j_batch_ready_properties()}]->(t);"""
    
    def generate_batch_query(self, entities:Network_Entities):
        result = ""

        if isinstance(entities[0],Node):
            result = self.generate_batch_node_query(entities)

        if isinstance(entities[0],Edge):
            result = self.generate_batch_edge_query(entities)
        
        return result

    def insert_batch_query(self, batch_query:str, _params:Network_Entities):
        params = [
            {   **item.properties,
                "source_hash":item.source.properties[HASH_NODE_PROPERTY],
                "target_hash":item.target.properties[HASH_NODE_PROPERTY]
                } if isinstance(item, Edge) else item.properties for item in _params ]
        with self.driver.session() as session:
            session.run(batch_query, params=params) 

    @staticmethod
    def get_label_types_from_entities(entities:Network_Entities)->List[str]:
        return list(set([e.get_neo4j_ready_labels() for e in entities]))

    def do_batch(self,entities:Network_Entities, batch_size, entity_creation_config: Dict[str,str]):
        entity_count = len(entities)
        print(f"Updating/Inserting {entity_count} entities")
        batch_size = min(entity_count,batch_size)
        if batch_size > 0:
            for index in range(0, entity_count, batch_size):
                sub_entities: List[Edge] = entities[index:index+batch_size]
                for label_cat in self.get_label_types_from_entities(entities):
                    entities_to_insert = [entity for entity in sub_entities if label_cat == entity.get_neo4j_ready_labels()]
                    if entities_to_insert:
                        batch_query = self.generate_batch_query(entities_to_insert)
                        batch_query.replace("MERGE",entity_creation_config.get(label_cat,"MERGE"))
                        self.insert_batch_query(batch_query, entities_to_insert)

    def insert(self, network: Network, batch_size = 1, entity_creation_config: Dict[str,str]={}):
        if batch_size == 1:
            self.insert_nodes(network.nodes)
            self.insert_edges(network.edges)
        else:
            for network_entity in [network.nodes, network.edges]:
                self.do_batch(network_entity, batch_size, entity_creation_config)
    
    @staticmethod
    def _check_blocks(tx, blocks):
        result = tx.run(
            """ 
            UNWIND $blocks AS block_number
            MATCH (b:BLOCK {block_number:block_number})
            RETURN b.block_number as existing_block_numbers
            """,
            blocks=blocks
        )
        return result.value()
    
    @staticmethod
    def _check_last_n_blocks(tx, n_blocks_to_check):
        result = tx.run(
            """ 
            MATCH (b:BLOCK)
            WHERE NOT (b)<-[:PREVIOUS]-(:BLOCK)
            WITH [x IN range(MAX(b.block_number),MAX(b.block_number) - $n_blocks_to_check,-1)] AS block_range
            UNWIND block_range AS block_number
            MATCH (b:BLOCK {block_number:block_number})
            WITH Collect(b.block_number) as existing_block_numbers, block_range
            RETURN REVERSE([block_number IN block_range WHERE NOT block_number IN existing_block_numbers]) AS missing_blocks
            """,
            n_blocks_to_check=n_blocks_to_check
        )
        return result.single()[0]
    
    def check_last_n_blocks(self,n_blocks_to_check):
        with self.driver.session() as session:
            return session.write_transaction(
                self._check_last_n_blocks, n_blocks_to_check
            )
    
    def correct_missing_block_to_block_connections(self):
        def _correct_missing_b_to_b_conn(tx):
            result = tx.run(
                """ 
                MATCH (a:BLOCK),(b:BLOCK)
                WHERE a.block_number = b.block_number+1 AND NOT (a)-[:PREVIOUS]->(b)
                CREATE (a)-[:PREVIOUS]->(b)
                """,
            )
            return result.value()

        with self.driver.session() as session:
            return session.write_transaction(
                _correct_missing_b_to_b_conn
            )
    
    @staticmethod
    def _addresses_interacted_with(tx, address_to_check):
        result = tx.run(
            """ 
            MATCH (a1:ADDRESS {hash:$address_to_check})--(t:TRANSACTION)--(a2:ADDRESS)
            RETURN collect(a2.hash) as addresses_interacted
            """,
            address_to_check=address_to_check
        )
        return result.single()[0]
    
    def addresses_interacted_with(self,address_to_check):
        with self.driver.session() as session:
            return session.write_transaction(
                self._addresses_interacted_with, address_to_check
            )
