import re

from google.appengine.ext import ndb

regex_replace = re.compile('[\W_]+')


class ProductsIndex(ndb.Model):
    """Datastore model representing product"""
    partial_strings = ndb.StringProperty(repeated=True)
    name = ndb.StringProperty()
    idx  = ndb.StringProperty()


    @classmethod
    def search(cls, text_query):
        """Execute search query"""
        words = text_query.lower().split(' ')
        words = [w for w in words if w]
        query = cls.query()
        for word in words:
            query = query.filter(cls.idx == word)
        return query.fetch(20)

    @classmethod
    def create(cls, item):
        """Create object (doesn't save)"""
        key = ndb.Key(cls, int(item['sku']))
        obj = cls(key=key, name=item['name'])
        return obj
