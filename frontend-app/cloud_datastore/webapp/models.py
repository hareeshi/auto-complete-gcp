import re

from google.appengine.ext import ndb

regex_replace = re.compile('[\W_]+')


class ProductsIndex(ndb.Model):
    """Datastore model representing product"""
    partial_strings = ndb.StringProperty(repeated=True)
    name = ndb.StringProperty()
    idx  = ndb.StringProperty()
#    price = ndb.FloatProperty()
#    url = ndb.StringProperty()
#    type = ndb.StringProperty()


    @classmethod
    def search(cls, text_query):
        """Execute search query"""
#       print "Text_Query", text_query
        words = text_query.lower().split(' ')
        words = [w for w in words if w]
        query = cls.query()
        for word in words:
            query = query.filter(cls.idx == word)
#        print query
        return query.fetch(20)

    @classmethod
    def create(cls, item):
        """Create object (doesn't save)"""
        key = ndb.Key(cls, int(item['sku']))
        obj = cls(key=key, name=item['name'])
        return obj
