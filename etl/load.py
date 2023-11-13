from pymongo import UpdateOne
from utils.mongo import Mongo


class PostModel(Mongo):
    _connection_name = 'mongo_connection'
    _collection_name = 'post'
    _db_name = 'data_pipline'


def run(data):
    post_model = PostModel()
    operations = []
    for d in data:
        operations.append(UpdateOne({'_id': d['_id']}, {'$set': d}, upsert=True))
    result = post_model.collection.bulk_write(operations)
