
from flaskapp import app
from flask import render_template
from flask import request
from flask import jsonify

from cassandra.cluster import Cluster
import ConfigParser


config = ConfigParser.ConfigParser()
config.read("/home/ubuntu/flaskapp/flask.conf")

cluster = Cluster([config.get('FlaskConfig', 'cluster')])
session = cluster.connect(config.get('FlaskConfig', 'session'))

@app.route('/')
@app.route('/index')
def index():
  return "Hello, World!"


@app.route('/api/<userpropensity>/<userid>')
def get_propensity(userpropensity, userid):
       stmt = "SELECT * FROM userpropesnity WHERE id=%s"
       response = session.execute(stmt, parameters=[userpropensity, userid])
       response_list = []
       for val in response:
            response_list.append(val)
       jsonresponse = [{"User id": x.userid, "Type": x.logtype, "Propensity": x.propensity, "time": x.timestamp} for x in response_list]
       return jsonify(propensity=jsonresponse)

if __name__ == '__main__':
  app.run()
