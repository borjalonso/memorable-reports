class Tree:
    def __init__(self, node):
        self.children = []
        self.node = node


class Node:
    def __init__(self, name: str,
                 columns: list,
                 master_table: int,
                 inner_joins: list):
        self.name = name
        self.columns = columns
        self.master_table = master_table
        self.inner_joins = inner_joins


def add_node(tree: Tree, node: Node, parent_node: Node):

    subtree = search_subtree(tree, parent_node)
    subtree.children.append(Tree(node=node))


def get_node(node_list: list, node_name: str):

    for node in node_list:
        if node.name == node_name:
            return node

    return None


def search_subtree(tree: Tree, node: Node):

    if tree.node == node:
        return tree
    for subtree in tree.children:
        searched_tree = search_subtree(subtree, node)
        if searched_tree is not None:
            return searched_tree
    return None


def depth(tree: Tree):
    """
    Tree depth is calculated 1 + the depth of the deepest child

    :param tree: (Tree) object which depth is calculated
    :return: (int) depth of the Tree
    """
    if len(tree.children) == 0:
        return 1
    return 1 + max(map(depth, tree.children))


def depth_first_search(tree: Tree, function):

    function(tree.node)
    for child in tree.children:
        depth_first_search(child, function)


def create_tree():

    node = Node(name='users',
                columns=["id", "created_at"],
                master_table=1,
                inner_joins=[
                  {
                    "on" : "id",
                    "join_with" : "profile",
                    "join_with_on" : "user_id"
                  },
                  {
                    "on" : "id",
                    "join_with" : "user_given_to",
                    "join_with_on" : "user_id"
                  },
                  {
                    "on" : "id",
                    "join_with" : "facebookuser",
                    "join_with_on" : "user_id"
                  },
                  {
                    "on" : "id",
                    "join_with" : "twitteruser",
                    "join_with_on" : "user_id"
                  }
                ])

    tree = Tree(node=node)

    node = Node(name='profile',
                columns=["user_id",
                  "age",
                  "country_id",
                  "emailConfirmed",
                  "telephoneConfirmed",
                  "emailSubscribed",
                  "telephoneSubscribed",
                  "shareMyData",
                  "robinson"],
                master_table=0,
                inner_joins=[
                  {
                    "on" : "country_id",
                    "join_with" : "country",
                    "join_with_on" : "id"
                  }
                ])

    add_node(tree=tree, node=node, parent_node=tree.node)

    node = Node(name="user_given_to",
                columns=["user_id", "given_to", "given_at"],
                master_table=0,
                inner_joins=[])

    add_node(tree=tree, node=node, parent_node=tree.node)

    node = Node(name="facebookuser",
                columns=["user_id", "facebook_id"],
                master_table=0,
                inner_joins=[])

    add_node(tree=tree, node=node, parent_node=tree.node)

    node = Node(name="twitteruser",
                columns=["user_id", "twitter_id"],
                master_table=0,
                inner_joins=[])

    add_node(tree=tree, node=node, parent_node=tree.node)

    node = Node(name='country',
                columns=["id", "name"],
                master_table=0,
                inner_joins=[])

    add_node(tree=tree, node=node, parent_node=tree.children[0].node)

    return tree
