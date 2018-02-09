from hashlib import md5
from struct import unpack_from
from collections import OrderedDict


class ConsistentHash:
    """
    :nodes 节点
    :virtual 虚拟节点，为了保持整个环的平衡
    """

    def __init__(self, nodes, virtual=3):
        self.base_nodes = nodes[:]
        self.actual_nodes = {}
        self.virtual = virtual
        # 这里的rings相当于整个环，几个节点所有数据的集合，这里只是为了方便演示
        self.rings = {}
        for node in nodes:
            self.addNode(node)
        # 这里按照key做一次排序，方便比较其大小
        self.actual_nodes = OrderedDict(sorted(self.actual_nodes.items(), key=lambda t: t[0]))

    def addNode(self, node):
        # todo 这里还需要迁移数据
        self.actual_nodes[self.hash(node)] = node
        for item in range(self.virtual):
            tmp = node + '#' + str(item)
            self.actual_nodes[self.hash(tmp)] = tmp

    def removeNode(self, node):
        # todo 迁移数据
        pass

    def hash(self, value):
        return unpack_from(">I", md5(str(value).encode('utf-8')).digest())[0]

    '''
    删除key
    '''

    def addKey(self, key, value):
        node = self.__findKeyWhereIn(key)
        # 需要处理的添加业务逻辑(为方便处理和演示，我这里先用一个全局的ring代替，线上环境应该真实的存放于对应的节点中)
        self.rings[key] = value
        return node

    '''
    添加key
    '''

    def removeKey(self, key):
        node = self.__findKeyWhereIn(key)
        # 需要处理的删除业务逻辑(为方便处理和演示，我这里先用一个全局的ring代替，线上环境应该删除真实节点存放的数据)
        self.rings.pop(key)
        pass

    '''
    查找key应该存放在哪个节点
    '''

    def __findKeyWhereIn(self, key):
        hash = self.hash(key)
        for item in self.actual_nodes.keys():
            if hash < item:
                return self.actual_nodes[item]


if __name__ == "__main__":
    servers = [
        '192.168.1.1',
        '192.168.1.2',
        '192.168.1.3'
    ]
    consistentHash = ConsistentHash(servers)
    print(consistentHash.rings)
    consistentHash.addKey('name', 'nine')
    print(consistentHash.rings)
    consistentHash.removeKey('name')
    print(consistentHash.rings)
