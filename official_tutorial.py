import motor.motor_asyncio
import pymongo
import asyncio
import time

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "test_database"
CL_NAME = "test_collection"

async_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
async_db = async_client[DB_NAME]
async_collection = async_db[CL_NAME]


client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[CL_NAME]


"""
INSERT ONE DOCUMENT
"""


async def do_insert():
    document = {"key": "value"}
    result = await async_collection.insert_one(document)
    print("result %s" % repr(result.inserted_id))


loop = asyncio.get_event_loop()


"""
INSERT MANY DOCUMENT
"""
N = 2000


def generate_doc_list(N):
    return [{"idx": i} for i in range(N)]


async def do_insert_many():
    then = time.time()
    result = await async_collection.insert_many(generate_doc_list(N))
    now = time.time()
    print(f"Async run {now-then}")


def sync_do_insert_many():
    then = time.time()
    collection.insert_many(generate_doc_list(N))
    now = time.time()
    print(f"normal run {now-then}")


"""
QUERY
"""

import pprint


async def do_find_list():
    cursor = async_collection.find({"idx": {"$lt": 2}}).sort("idx")
    for document in await cursor.to_list(length=10):
        pprint.pprint(document)


async def do_find_for():
    async for document in async_collection.find({"idx": {"$gt": 1998}}):
        pprint.pprint(document)

"""
UPDATE
"""

async def do_replace():
    old_document = await async_collection.find_one({'idx': 50})
    print('found document: %s' % pprint.pformat(old_document))

    _id = old_document['_id']
    result = await async_collection.replace_one({'_id': _id}, {'key': "value"})
    print('replaced %s document' % result.modified_count)

    new_document = await async_collection.find_one({'_id': _id})
    print('replaced %s document' % pprint.pformat(new_document))


if __name__ == "__main__":
    collection.remove()
    loop.run_until_complete(do_insert_many())
    # loop.run_until_complete(do_find_list())
    # loop.run_until_complete(do_find_for())
    loop.run_until_complete(do_replace())
