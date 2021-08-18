from typing import Dict
from pymongo import MongoClient, collection as mongoCollection


class Migrator:
    def handle(self, type: str, schema: str, collection: str, row: Dict):
        connection = MongoClient("mongodb://localhost:27017")
        database = connection[schema]
        table = database[collection]
        if type == 'UPDATE':
            return self.__update(row, table)
        if type == 'DELETE':
            return self.__delete(row, table)
        if type == "INSERT":
            return self.__insert(row, table)

    def __insert(self, row: Dict, collection: mongoCollection.Collection):
        exists = collection.find({"id": row['id']}).count() > 0
        if exists == False:
            collection.insert(row)
            return True
        return True

    def __update(self, row: Dict, collection: mongoCollection.Collection):
        exists = collection.find({"id": row['id']}).count() > 0
        if exists == False:
            collection.insert(row)
            return True
        collection.find_one_and_update({"id": row['id']}, {'$set': row})
        return True

    def __delete(self, row: Dict, collection: mongoCollection.Collection):
        collection.delete_one({'id': row['id']})
        return True