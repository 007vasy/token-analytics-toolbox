from web3 import Web3
import json
from typing import List
import os
from constants import *
from network_types import Network, Node, Edge
from neo4j_handler import Neo4jHandler
import requests
import subprocess
import urllib
import uuid
import pandas as pd
from pathlib import Path

def w3_client():
    return Web3(Web3.HTTPProvider(os.getenv(INFURA_HTTP_KEYWORD)))

def infura_2_neo4j(request):
    w3 = w3_client()
    block = w3.eth.getBlock(request[BLOCK_ID], full_transactions=True)
    nodes:List[Node] = []
    edges:List[Edge] = []
    block_node = Node(labels=[BLOCK_NODE_LABEL],
            properties={
                HASH_NODE_PROPERTY:block.hash.hex(),
                BLOCKNUMBER_NODE_PROPERTY: block.number
            }
        )
    parent_block_node = Node(
        labels=[BLOCK_NODE_LABEL],
        properties={
            HASH_NODE_PROPERTY:block.parentHash.hex(),
            BLOCKNUMBER_NODE_PROPERTY: block.number -1
        }
    )

    nodes.extend([block_node,parent_block_node])
    edges.append(Edge(label=PREVIOUS_EDGE_LABEL,source=block_node,target=parent_block_node))

    for transaction in block.get(TRANSACTIONS,[]):
        transaction_node = Node(labels=[TRANSACTION_NODE_LABEL],properties={
            HASH_NODE_PROPERTY:transaction.hash.hex(),
        })
        if transaction["from"]:
            from_node = Node(labels=[ADDRESS_NODE_LABEL],properties={
                HASH_NODE_PROPERTY:transaction["from"]
            })
            edges.append(Edge(label=FROM_EDGE_LABEL,source=from_node, target=transaction_node))
        if transaction["to"]:
            to_node = Node(labels=[ADDRESS_NODE_LABEL],properties={
                HASH_NODE_PROPERTY:transaction["to"]
            })
            edges.append(Edge(label=TO_EDGE_LABEL,source=transaction_node, target=to_node))
        nodes.extend([transaction_node, from_node, to_node])
        edges.append(Edge(label=HOLDS_EDGE_LABEL,source=block_node, target=transaction_node))
        

    extracted_network = Network(name='Eth-Analytics',nodes=nodes,edges=edges)
    neo4j_handler = Neo4jHandler()
    entity_creation_config = {
        BLOCK_NODE_LABEL: MERGE_KEYWORD,
        TRANSACTION_NODE_LABEL: CREATE_KEYWORD,
        ADDRESS_NODE_LABEL: MERGE_KEYWORD,
        PREVIOUS_EDGE_LABEL: CREATE_KEYWORD,
        FROM_EDGE_LABEL: MERGE_KEYWORD,
        TO_EDGE_LABEL: MERGE_KEYWORD,
        HOLDS_EDGE_LABEL: MERGE_KEYWORD,
    }
    neo4j_handler.insert(extracted_network,100,entity_creation_config)
    neo4j_handler.close()
    return "Ok"

def look_for_missing_blocks(event, context):
    infura_2_neo4j = os.getenv(INFURA_2_NEO4J_URL_KEYWORD,"https://127.0.0.1:5000")
    neo4j_handler = Neo4jHandler()
    blocks_to_insert = neo4j_handler.check_last_n_blocks(100)
    print(blocks_to_insert)
    auth_token=get_access_token(infura_2_neo4j)
    hed = {'Authorization': 'Bearer ' + auth_token}
    verify = not ("localhost" in infura_2_neo4j or "127.0.0.1" in infura_2_neo4j)
    for block in blocks_to_insert:
        data = {BLOCK_ID:block}
        r = requests.post(infura_2_neo4j, json = data, headers=hed, verify=verify)
    neo4j_handler.correct_missing_block_to_block_connections()
    return "Ok"

def update_token(event, context):
    neo4j_handler = Neo4jHandler()
    w3 = w3_client()
    block_id = event.get(BLOCK_ID,"latest")
    contract_address = event.get(CONTRACT_ADDRESS_KEY)
    contract_abi = event.get(CONTRACT_ABI_KEY)
    block = w3.eth.get_block(block_id, full_transactions=True)
    w3.eth.default_block = block.number
    contract = w3.eth.contract(address=contract_address, abi=contract_abi)
    
    if contract_address == USDC_CONTRACT_ADDRESS:
        symbol = "USDC"
    else:
        try:
            symbol = contract.functions.symbol().call()
        except Exception as e:
            print(e)
    
    

    interacted_with_contract = neo4j_handler.addresses_interacted_with(contract_address)
    curr_token_balances = {address_hash:contract.functions.balanceOf(address_hash).call() for address_hash in interacted_with_contract}
    nodes:List[Node] = []
    edges:List[Edge] = []

    contract_node = Node(labels=[ADDRESS_NODE_LABEL],
            properties={
                HASH_NODE_PROPERTY:contract.address,
            }
        )
    block_node = Node(labels=[BLOCK_NODE_LABEL],
            properties={
                HASH_NODE_PROPERTY:block.hash.hex(),
                BLOCKNUMBER_NODE_PROPERTY: block.number
            }
        )
    nodes.extend([contract_node,block_node])
    
    token_labels = [TOKEN_NODE_LABEL]

    if symbol != "":
        token_labels.append(symbol)

    for address,balance in curr_token_balances.items():
        address_node = Node(
            labels=[ADDRESS_NODE_LABEL],
            properties={
                HASH_NODE_PROPERTY:address
            })
        token_node = Node(
            labels=token_labels,
            properties={
                BALANCE_NODE_PROPERTY:str(balance), # TODO report neo4j the 64bit issue 
                HASH_NODE_PROPERTY:address + contract.address + str(block.number) 
            })
        nodes.extend([address_node, token_node])
        edges.append(Edge(label=BELONGS_TO_EDGE_LABEL, source=contract_node, target=token_node))
        edges.append(Edge(label=OWNS_EDGE_LABEL, source=address_node, target=token_node))
        edges.append(Edge(label=STATE_AT_EDGE_LABEL, source=token_node, target=block_node))

    entity_creation_config = {
        TOKEN_NODE_LABEL: MERGE_KEYWORD,
        ADDRESS_NODE_LABEL: MERGE_KEYWORD,
        CONTRACT_NODE_LABEL: MERGE_KEYWORD,
    }

    contract_network = Network("ContractUpdate", nodes=nodes,edges=edges)

    neo4j_handler.insert(contract_network,1000,entity_creation_config)
    neo4j_handler.close()
    return "Ok"

if __name__ == "__main__":
    data_folder_path = Path('../data')
    input_data_filename = Path('export-0x5922b0bbae5182f2b70609f5dfd08f7da561f5a4.csv')
    print(f"Processing schema file: {data_folder_path}/{input_data_filename} ...")
    block_data = pd.read_csv(data_folder_path/input_data_filename, index_col=False)

    for blockno in list(block_data["Blockno"]):
        print(blockno)
        payload = {BLOCK_ID:blockno}
        infura_2_neo4j(payload)

        payload.update({CONTRACT_ADDRESS_KEY:MM_CONTRACT_ADDRESS,CONTRACT_ABI_KEY:MM_CONTRACT_ABI})
        # print(payload)
        update_token(payload,{})
        payload.update({CONTRACT_ADDRESS_KEY:USDC_CONTRACT_ADDRESS,CONTRACT_ABI_KEY:USDC_CONTRACT_ABI})
        update_token(payload,{})