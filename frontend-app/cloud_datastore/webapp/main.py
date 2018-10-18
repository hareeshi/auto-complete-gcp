import logging
import base64

from flask import Flask, render_template, request
from flask.json import jsonify

from google.appengine.ext import deferred

from search_datastore import DatastoreSearchAPI
from models import ProductsIndex

app = Flask(__name__)
app.debug = True
search_client = DatastoreSearchAPI()
project_id = 'extended-signal-219515'
topic_name = 'search-weights'

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/search')
def search():
    """based on user query it executes search and returns list of item in json"""
    query = request.args.get('query', '')
    results = search_client.search(query)
    return jsonify(results)

@app.route('/submitEvent')
def submitEvent():
    """gets data for one product and saves into search index"""
    # Publisher.
    from googleapiclient.discovery import build
    service = build('pubsub', 'v1')
    mytopic = 'projects/extended-signal-219515/topics/search-weights'
    data = request.args.get('search', '')
    service.projects().topics().publish(topic=mytopic, body={
      "messages":
        [{
         "data": base64.b64encode(data.encode('utf-8'))
        }]
    }).execute()
    return 'ok'


@app.route('/upload', methods=['POST'])
def upload():
    """gets data for one product and saves into search index"""
    json_data = request.get_json()
    search_client.insert(json_data)
    return 'ok'


@app.route('/upload_bulk', methods=['POST'])
def upload_bulk():
    """gets list of products and saves into search index"""
    json_data = request.get_json()
    logging.info("received {} items".format(len(json_data)))
    search_client.insert_bulk(json_data)
    return 'ok'


@app.route('/delete')
def delete():
    """deletes all items in search"""
    deferred.defer(search_client.delete_all)
    return 'ok'


@app.route('/test')
def test():
    res = Products.query(Products.partial_strings == 'blue', Products.partial_strings == 'lines').fetch()
    for line in res:
        print line
    return 'ok'