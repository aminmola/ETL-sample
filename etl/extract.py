from utils.mongo import Mongo


class PostModel(Mongo):
    _connection_name = 'mongo_connection1'
    _collection_name = 'post'
    _db_name = 'data_pipline'


def run():
    out = []
    post_model = PostModel()
    query = {"$and": [{"title": {"$nin": [None, '', -1]}}, {"Tags" : None}]}
    posts = post_model.collection.find(query, {"_id": "$_id"}).limit(1000)
    for doc in posts:
        out.append(doc)
    return out
