from overpy import Node


# future: embed this into overpy.Node
class HashableNodeSnapshot(Node):
    def __init__(self, node: Node):
        super().__init__(
            node_id=node.id,
            lat=node.lat,
            lon=node.lon,
            attributes=node.attributes,
            result=node._result,
            tags=node.tags,
        )

    def __hash__(self) -> int:
        return hash((self.id, self.lat, self.lon, frozenset(self.tags.items())))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, HashableNodeSnapshot) or isinstance(other, Node):
            return (self.id, self.lat, self.lon, self.tags.items()) == (
                other.id,
                other.lat,
                other.lon,
                other.tags.items(),
            )
        
        return NotImplemented
