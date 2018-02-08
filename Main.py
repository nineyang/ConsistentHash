class ConsistentHash:
    """
    :nodes 节点
    :virtual 虚拟节点，为了保持整个环的平衡
    """

    def __init__(self, nodes, virtual=3):
        self.actual_nodes = self.base_nodes = nodes[:]
        self.virtual = virtual
        for node in nodes:
            self.actual_nodes += self.addNode(node)

        print(self.actual_nodes)

    def addNode(self, node):
        tmp = []
        for item in range(self.virtual):
            tmp.append(node + '#' + str(item))

        return tmp

    def hash(self):
        pass


if __name__ == "__main__":
    servers = [
        'node1',
        'node2',
        'node3'
    ]
    consistentHash = ConsistentHash(servers)
