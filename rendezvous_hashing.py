import hashlib
from pickle_hash import serialize_PUT

from server_config import NODES

class NodeRing():

    def __init__(self, nodes):
        assert len(nodes) > 0
        self.nodes = nodes
    
    def get_node(self, key_hex):
        node_index = self.rendezvous_hashing(key_hex)
        return self.nodes[node_index]

    def rendezvous_hashing(self, key):
        max_weight = 0
        node_index = 0
        for i in NODES:
            data_bytes, node_key = serialize_PUT(i)
            weight = (int(key,16)+int(node_key,16))%len(NODES) #compute weight for each node
            if weight > max_weight:
                max_weight = weight
                node_index = NODES.index(i)
        print("Selected node is ", node_index)
        return node_index


def test():
    ring = NodeRing(nodes=NODES)
    node = ring.get_node('9ad5794ec94345c4873c4e591788743a')
    print(node)
    print(ring.get_node('ed9440c442632621b608521b3f2650b8'))


# Uncomment to run the above local test via: python3 node_ring.py
#test()
