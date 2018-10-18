from search_base import SearchEngine
from models import ProductsIndex

from google.appengine.api import memcache
from google.appengine.ext import ndb

class DatastoreSearchAPI(SearchEngine):
    def insert_bulk(self, items):
        to_save = []
        for item in items:
            product = ProductsIndex.create(item)
            to_save.append(product)
        ndb.put_multi(to_save)

    def insert(self, item):
        product = ProductsIndex.create(item)
        product.put()

    def search(self, query):
        print "Inside DatastoreSearchAPI"
        results = memcache.get(query)
        if results is None:
           results = ProductsIndex.search(query)
           memcache.add(query, results, 86400)
        output = []
        print "Results from query - ", results
        for item in results:
            print "Name - ", item.name
            out = {
                'value': item.name,
                'label': item.name,
            }
            output.append(out)
        return output

    def delete_all(self):
        run = True
        while run:
            res = ProductsIndex.query().fetch(limit=100, keys_only=True)
            if res:
                ndb.delete_multi(res)
            else:
                run = False