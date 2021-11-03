import pandas as pd
import os
from sqlalchemy import create_engine
from reports.data_extraction.database.utils.Tree import *


class DatabaseReport:

    config_path = os.path.join(os.path.dirname(__file__), "../config")
    db_config_file = "report_config.json"

    # default constructor
    def __init__(self, report_type):
        self.report_type = report_type

        self.mariadb_engine = None
        self.table_indexes = list()
        self.list_of_tables = None
        self.master_table = list()
        self.merged_table = list()
        self.trees = list()

    def set_db_connexion(self, db_name='local_backup'):
        """
        Establish an active and stable connexion session with the database.

        :param db_name: (str) set of credentials stored in 'db_access_credentials.json' file used to connect to the database.
        :return: void
        """

        dbapi = pd.read_json(os.path.join(self.config_path, "db_access_credentials.json"), orient='index')

        # Let's create a connexion from the DBAPI variables
        connexion = 'mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}?auth_plugin={5}'
        connexion = connexion.format(dbapi['user'][db_name],
                                     dbapi['password'][db_name],
                                     dbapi['server_url'][db_name],
                                     dbapi['port'][db_name],
                                     dbapi['schema'][db_name],
                                     dbapi['authentication_plugin'][db_name])

        self.mariadb_engine = create_engine(connexion)

    def load_db_tables(self):
        """
        Loads all the tables and columns indicated in the 'table_properties.json' file.

        :return: void
        """

        self.list_of_tables = pd.read_json(os.path.join(self.config_path, self.db_config_file),
                                           orient='records')[self.report_type]['table']

        # Loading tables from database
        for aux_index, table in enumerate(self.list_of_tables):
            """
            table_indexes: position in which each table is located in the list variable 'list_of_tables'.
            list_of_tables: list data structure used to allocate the database tables in DataFrame format.
            """
            self.table_indexes.append(table['name'])
            self.list_of_tables[aux_index] = pd.read_sql_table(table_name=table['name'],
                                                               con=self.mariadb_engine,
                                                               columns=table['columns'],
                                                               parse_dates=table['parse_dates'])

            if table['master_table']:
                self.master_table.append(table['name'])

    def merge_tables(self):
        """
        Takes the instructions contained in the 'report_config.json' configuration file and translates it into one
        single DataFrame taking into account the merge strategy specified in the previous configuration file.

        :return: void
        """

        table_params = pd.read_json(os.path.join(self.config_path, self.db_config_file),
                                    orient='records')[self.report_type]['table']

        self.trees = create_tree(table_params)

        for tree in self.trees:
            self._recursive_merge(tree=tree)
            self.merged_table.append(self._get_table(self.master_table[0]))

            # Drop table from list_of_tables, table_indexes and master_table
            del self.list_of_tables[self.table_indexes.index(self.master_table[0])]
            self.table_indexes.remove(self.master_table[0])
            del self.master_table[0]

    def _recursive_merge(self, tree: Tree):
        """
        Replicates the tree's hierarchical merge structure using pandas DataFrames.
            - Tree nodes represent database tables.
            - Tree branches represent database joints.

        Merges are executed bottom-up from the tree
        Example:
            master_table
            |__ table_1
            |       |__ table_2
            |               |__ table_3
            |__ table_4

        > table_2 = merge(table_2 & table_3)
        > table_1 = merge(table_1 & table_2)
        > master_table = merge(master_table & table_1)
        > master_table = merge(master_table & table_4)
        > end

        :param tree: (Tree) object that contains the hierarchical structure to be replicated.
        :return: void
        """

        # Keep record of the parent subtree
        parent = tree

        # Visit all child nodes:
        #   1) If child node is a leaf node, then merge parent node with child node
        #   2) If child node is a root node, then:
        #       2.1) Merge recursively all its children.
        #       2.2) Merge parent node with child node
        for child in tree.children:
            if len(child.children) == 0:
                self._merge_dataframes(left_node=parent.node, right_node=child.node)
                parent.children = parent.children[1:]
            else:
                self._recursive_merge(child)
                self._merge_dataframes(left_node=parent.node, right_node=child.node)

    def _merge_dataframes(self, left_node: Node, right_node: Node):
        """
        Merges the tables received as parameters and updates 'list_of_tables' with the resultant DataFrame.

        :param left_node: (Node) that represents the left side of the merge.
        :param right_node: (Node) that represents the object to merge with.
        :return: void
        """

        left_dataframe = self._get_table(left_node.name)
        right_dataframe = self._get_table(right_node.name)

        join_id = 0
        while left_node.inner_joins[join_id]['join_with'] != right_node.name:
            join_id += 1

        merged_df = left_dataframe.merge(right_dataframe,
                                         how='left',
                                         left_on=left_node.inner_joins[join_id]['on'],
                                         right_on=left_node.inner_joins[join_id]['join_with_on'],
                                         suffixes=(None, "_" + right_node.name))
        self._set_table(left_node.name, merged_df)

    def _get_table(self, table_name: str):
        return self.list_of_tables[self.table_indexes.index(table_name)]

    def _set_table(self, table_name: str, table: pd.DataFrame):
        self.list_of_tables[self.table_indexes.index(table_name)] = table


def create_tree(table_params):
    """


    :param table_params:
    :return:
    """

    # Skeleton definition
    #   1) Root nodes creation
    #   2) Branch and leaf nodes creation
    nodes = list()
    tree_nodes = list()
    trees = list()

    for table in table_params:
        node = Node(name=table['name'],
                    columns=table['columns'],
                    master_table=table['master_table'],
                    inner_joins=table['inner_joins'])

        if table['master_table']:
            tree = Tree(node=node)
            tree_nodes.append(tree)

        else:
            nodes.append(node)

    # Trees composition
    #   1) For each Tree in trees, recursively add all its child nodes
    for tree in tree_nodes:
        trees.append(_recursive_node_addition(tree=tree, joins=tree.node.inner_joins, parent_node=tree.node, node_list=nodes))

    return trees


def _recursive_node_addition(tree: Tree, joins: list, parent_node: Node, node_list: list):
    for join in joins:
        node = get_node(node_list=node_list, node_name=join['join_with'])

        if len(node.inner_joins) == 0:
            add_node(tree=tree, node=node, parent_node=parent_node)

        else:
            add_node(tree=tree, node=node, parent_node=parent_node)
            _recursive_node_addition(tree=tree, joins=node.inner_joins, parent_node=node, node_list=node_list)

    return tree
