#import hashlib
#from pickle_hash import serialize_PUT

from server_config import NODES

class NodeRing():

    def __init__(self, nodes):
        assert len(nodes) > 0
        self.nodes = nodes
        self.vnodepernode = 4 # define number of virtual nodes to be configured per physical node
        self.vnodes = [i for i in range(len(nodes)*self.vnodepernode)] # create a list of physical nodes
        self.rf = 3 # define the desired replication factor
    
    def get_node(self, key_hex):
        key = int(key_hex, 16)
        node_index = self.consistent_hashing(key_hex)
        retval = []
        for i in range(self.rf):
            retval.append(self.nodes[(node_index+i)%len(self.nodes)])
        return retval #return a list of nodes   #self.nodes[node_index]
    
    def consistent_hashing(self, key):
        dest_vnode = int(key,16)%len(self.vnodes) # map user key to a vnode
        result = {"dest_node": {"physical": dest_vnode%len(self.nodes), "virtual":dest_vnode}, "replica_nodes": [{"physical": (dest_vnode+i)%len(self.nodes), "virtual":dest_vnode+i} for i in range(1,self.rf)]}
        print(result) # print vnode, physical node and replication details
        return int(dest_vnode%len(self.nodes))

def test():
    ring = NodeRing(nodes=NODES)
    node = ring.get_node('9ad5794ec94345c4873c4e591788743a')
    print(node)
    print(ring.get_node('ed9440c442632621b608521b3f2650b8'))


# Uncomment to run the above local test via: python3 node_ring.py
#test()
