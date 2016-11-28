"""
Description: Module used to handle tinydb basic apis for tinydb basic operation
"""
__author__ = "pratik khandge"
__copyright__ = ""
__credits__ = ["pratik khandge"]
__license__ = ""
__version__ = "0.1"
__maintainer__ = "pratik"
__email__ = "pratik.khandge@gmail.com"
__status__ = "Developement"

# Python imports
from tinydb import TinyDB, Query


class TinyConn(object):
    """
    Description: Class for tiny database basic database operation
    """

    def __init__(self, db_path):
        """
        Description: Connection parameters for the tiny database
        Parameters: db_path (str) - database path
        """
        try:
            self.db_path = db_path
            self.db = TinyDB(self.db_path)
        except Exception as e:
            raise Exception("Could not connection to {} Exception {}:".format(self.db_path, e))

    def close(self):
        """
        Description: Close tiny database connection.
        """
        if hasattr(self, 'db') and self.db._opened:
            self.db.close()

    def insert_document(self, data, table_name=TinyDB.DEFAULT_TABLE):
        """
        Description: Insert document in  tiny database.
        Parameter: data: dict
                   table_name: string
        Returns: eid
        """
        try:
            table = self.db.table(table_name)
            return table.insert(data)
        except Exception as e:
            raise Exception("Could not insert document in table {} Exception: {}".format(table_name, e))

    def insert_multiple_documents(self, data, table_name=TinyDB.DEFAULT_TABLE):
        """
        Description: Insert multiple doc in  tiny database.
        Parameter: data: list
                   table_name: string
        Returns: list of eids
        """
        try:
            table = self.db.table(table_name)
            return table.insert_multiple(data)
        except Exception as e:
            raise Exception("Couid not insert documents in table {} Exception: {}".format(table_name, e))

    def list_all_documents(self, table_name=TinyDB.DEFAULT_TABLE):
        """
        Description: Fetch all doc from tiny database table.
        Parameter: table_name: string
        Returns: list
        """
        try:
            table = self.db.table(table_name)
            return table.all()
        except Exception as e:
            raise Exception("Could not fetch documents from table {} Exception: {}".format(table_name, e))

    def list_documents(self, query=None, table_name=TinyDB.DEFAULT_TABLE):
        """
        Description: Search doc in  tiny database.
        Parameter: query: tinydb query
                   table_name: string
        Returns: list
        """
        try:
            if not query:
                return self.list_all_documents(table_name=table_name)
            table = self.db.table(table_name)
            records = table.search(query)
            return records
        except Exception as e:
            raise Exception("Could not fetch documents from table {} Exception: {}".format(table_name, e))

    def update_document(self, query, eids=None, update_dict=None, table_name=TinyDB.DEFAULT_TABLE, **kwargs):
        """
        Description: Update doc in tiny database.
        Parameter: query: tinydb query
                   eids: doc ids
                   update_dict: dict
                   table_name: string
        Returns: eid
        """
        update_dict = update_dict or kwargs
        table = self.db.table(table_name)
        if query:
            return table.update(update_dict, query)
        return table.update(update_dict, eids=eids)

    def update_all_document(self, update_dict=None, table_name=TinyDB.DEFAULT_TABLE, **kwargs):
        """
        Description: Update all document in tiny database.
        Parameters: update_dict: dict
                    table_name: string
        Returns: list of eids
        """
        table = self.db.table(table_name)
        update_dict = update_dict or kwargs
        eids = (i.eid for i in table.all())
        return table.update(update_dict, eids=eids)

    def remove_document(self, query=None, eids=None, table_name=TinyDB.DEFAULT_TABLE):
        """
        Description: Remove doc from  tiny database.
        Parameter: query: tinydb query
                   eids: doc ids
                   table_name: string
        Returns: list of eids
        """
        table = self.db.table(table_name)
        return table.remove(query, eids=eids)

    def get_document_by_eid(self, eid, table_name=TinyDB.DEFAULT_TABLE):
        """
        Description: Get by 'eid' attr.
        Parameters: eid: str
                    table_name: string
        Returns: dict
        """
        query = Query()
        table = self.db.table(table_name)
        return table.get(query.eid == eid)

    def delete_document_by_eid(self, eid, table_name=TinyDB.DEFAULT_TABLE):
        """
        Description: Delete by 'eid' attr.
        Parameters: eid: str
                    table_name: string
        Returns: eid
        """
        query = Query()
        table = self.db.table(table_name)
        return table.remove(query.eid == eid)

    def __del__(self):
        """
        Description: Descructor to close db connection
        """
        self.close()


