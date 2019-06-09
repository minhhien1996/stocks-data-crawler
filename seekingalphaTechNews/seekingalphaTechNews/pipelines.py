# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pydash
import pymongo


class InsertToMongoPipeline(object):
    def __init__(self, mongo_uri, mongo_db, mongo_collection):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGODB_URI'),
            mongo_db=crawler.settings.get('MONGODB_DATABASE'),
            mongo_collection=crawler.settings.get('MONGODB_COLLECTION')
        )
    
    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.db[self.mongo_collection].create_index(
            [("title", pymongo.DESCENDING)], unique=True)
        self.db[self.mongo_collection].create_index(
            [("related_indexes", pymongo.ASCENDING)], unique=False)

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        if item.get('related_indexes'):
            keys = [
                'category',
                'title',
                'ref_url',
                'crawled_at',
                'date_string_at_crawled',
                'timestamp',
                'contents',
                'related_indexes',
            ];
            mongodb_item = pydash.pick_by(item, lambda v, k: k in keys)
            try:
                self.db[self.mongo_collection].insert_one(mongodb_item)
            except:
                print('Item exists')
        return item
