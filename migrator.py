from typing import Dict, List
from pymongo import MongoClient, collection as mongoCollection
from datetime import datetime

class Migrator:
    def handle(self, connection: MongoClient, action: str, schema: str, collection: str, rows: List[Dict]):
        schema = connection[schema]
        collection = schema[collection]
        if action in ('UPDATE', 'INSERT'):
            return self.__upsert(rows, collection)
        if action == 'DELETE':
            return self.__delete(rows, collection)

    def __upsert(self, rows: List[Dict], collection: mongoCollection.Collection):
        for row in rows:
            search = {
                "$and": [
                    {"id": row['id']},
                    {
                        "updated_at": {
                            "$lte": datetime.strptime(row['updated_at'], "%Y-%m-%dT%H:%M:%S.%f%z")
                        },
                    }
                ]
            }
            collection.replace_one(search, row, upsert=True)

    def __delete(self, rows: List, collection: mongoCollection.Collection):
        search = [row['id'] for row in rows]
        collection.delete_many({"id": {"$in": search}})
        return True
