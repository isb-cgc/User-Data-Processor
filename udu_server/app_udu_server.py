#!/usr/bin/env python

import ssl
import os
from flask import Flask, request, jsonify, abort, make_response
from flask_basicauth import BasicAuth
from werkzeug.utils import secure_filename
from google.cloud import datastore, pubsub
import datetime
import tasks_for_psq
import psq

#
# Make sure we come up with a unique name, though clearly if this was handling
# multiple requests at once (which we are not doing, since we are just using
# the straight app.run) it would still have a race condition:
#

def time_stamped_unique(fname, fmt='%Y-%m-%d-%H-%M-%S-{num}_{fname}'):
    num = 0
    while True:
        test_name = datetime.datetime.now().strftime(fmt).format(num=num, fname=fname)
        test_name = os.path.join(app.config['UPLOAD_FOLDER'], test_name)
        if not os.path.isfile(test_name):
            return test_name
        num += 1

#
# Do not use environment variables. We use a secrets file and load our own dictionary:
#

def read_dict(my_file_name):
    retval = {}
    with open(my_file_name, 'r') as f:
        for line in f:
            split_line = line.split('=')
            retval[split_line[0].strip()] = split_line[1].strip()
    return retval


#
# Here we read the secrets file, build the Flask server, install config
# settings, and build the psq queues. Since we are using SSL, we get away
# with using Basic Authentication
#

my_secrets = read_dict('udu_secrets.txt')
my_config = read_dict('udu_config.txt')

PROJECT_ID = my_config['UDU_PSQ_PROJECT_ID']
UPLOAD_FOLDER = my_config['UDU_UPLOAD_FOLDER']
RESPONSE_LOCATION_PREFIX = my_config['UDU_RESPONSE_LOCATION']
PING_COUNT = int(my_config['UDU_PING_COUNT'])

# FLASK
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['BASIC_AUTH_USERNAME'] = my_secrets['UDU_PSQ_USERNAME']
app.config['BASIC_AUTH_PASSWORD'] = my_secrets['UDU_PSQ_PASSWORD']
basic_auth = BasicAuth(app)

# PUB/SUB QUEUE
pubsub_client = pubsub.Client(project=PROJECT_ID)
#datastore_client = datastore.Client(project=PROJECT_ID)
q = psq.Queue(pubsub_client) #, storage = psq.DatastoreStorage(datastore_client))

#
# This is the guts of the server. Takes UDU job requests and queues them up
# for execution using psq:
#

@app.route('/jenkins/job/user-data-proc/buildWithParameters', methods=['POST'])
@basic_auth.required
def run_udu_job():
    if request.method == 'POST':

        #
        # Extract the needed URLs and do sanity checking:
        #

        success_url = request.args.get('SUCCESS_POST_URL')
        failure_url = request.args.get('FAILURE_POST_URL')
        if (not (success_url and success_url.strip()) or
            not (failure_url and failure_url.strip())):
            print 'missing URLs'
            return abort(400)
        if 'config.json' not in request.files:
            print 'missing config.json'
            return abort(400)
        my_file = request.files['config.json']
        if not (my_file.filename and my_file.filename.strip()):
            print 'empty filename'
            return abort(400)

        if my_file.filename == 'config.json':
            my_file_name = time_stamped_unique(my_file.filename)
            my_file.save(my_file_name)



        #
        # WJRL 3/19/17: Google Pub/Sub behaves terribly if there is only one message
        # published to a topic. It sits for ~10 minutes, or even more. We can deal with
        # this by creating a pile of no-op calls before and after to flush the message
        # queue:
        #
            for _ in xrange(10):
                q.enqueue(tasks_for_psq.ping_the_pipe)

            q.enqueue(tasks_for_psq.processUserData, my_file_name, success_url, failure_url)

            for _ in xrange(10):
                q.enqueue(tasks_for_psq.ping_the_pipe)

            resp = make_response(jsonify("processing"))
            resp.headers['Location'] = RESPONSE_LOCATION_PREFIX + my_file_name
            return resp
        else:
            print 'unexpected filename'
            return abort(400)
    else:
        print 'unexpected transport'
        return abort(400)

#
# We advertise a function that allows us to unclog the task queue with pings:
#

@app.route('/pipePing', methods=['GET'])
def pinger():
    q.enqueue(tasks_for_psq.ping_the_pipe)
    return jsonify("hello")

#
# We run the Flask server using https. Note that we are depending on Django to call us using https.
# We are not doing redirects to https:
#

if __name__ == '__main__':
    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    context.load_cert_chain('flask-server.crt', 'flask-server.key')
    app.run(host='0.0.0.0', debug=True, ssl_context=context)
