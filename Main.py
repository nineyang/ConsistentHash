from hashlib import md5
from struct import unpack_from
from collections import OrderedDict
import itertools


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
        print(self.__getNextNode(3272578760))

    '''
    添加节点
    '''

    def addNode(self, node, use_virtual=True):
        # 找到后面的Node
        right_node = self.__findKeyWhereIn(node)
        hash = self.hash(node)
        # 这里需要删除right_node以hash左边的节点
        for item in self.rings:
            if item < hash:
                # 实际业务中需要把这个值从旧的节点(right_node)转移到新的节点(node)中
                pass
        self.actual_nodes[hash] = node
        if use_virtual:
            for item in range(self.virtual):
                tmp = node + '#' + str(item)
                self.actual_nodes[self.hash(tmp)] = tmp

    '''
    删除节点
    '''

    def removeNode(self, node):
        # 找到当前节点的下一个节点
        right_node = self.__getNextNode(node)
        hash = self.hash(node)
        # 这里需要把node节点的全部转移到right_node上面去
        for item in self.rings:
            if item < hash:
                # 实际业务中需要把这个值从旧的节点(node)转移到新的节点(right_node)中
                pass
        self.actual_nodes.pop(node)

    '''
    计算哈希
    '''

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

    '''
    获取数据
    '''

    def getKey(self, key):
        node = self.__findKeyWhereIn(key)
        return self.rings[key]

    '''
    查找key应该存放在哪个节点
    '''

    def __findKeyWhereIn(self, key):
        hash = self.hash(key)
        for item in self.actual_nodes.keys():
            if hash < item:
                return self.actual_nodes[item]

    '''
    获取当前节点的下一个节点
    '''

    def __getNextNode(self, node):
        for index, value in enumerate(self.actual_nodes):
            if node == value:
                actual_nodes = list(self.actual_nodes)
                length = len(actual_nodes)
                # 因为这里是一个环，所以需要取模算
                return list(actual_nodes)[(index + 1) % length]


if __name__ == "__main__":
    servers = [
        '192.168.1.1',
        '192.168.1.2',
        '192.168.1.3'
    ]
    consistentHash = ConsistentHash(servers)
    print(consistentHash.rings)
    consistentHash.addKey('name', 'nine')
    print(consistentHash.getKey('name'))
    print(consistentHash.rings)
    consistentHash.removeKey('name')
    print(consistentHash.rings)
