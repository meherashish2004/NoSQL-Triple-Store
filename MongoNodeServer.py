import json
import sys
from flask import Flask, abort, request, jsonify, render_template_string
from flask_cors import CORS
import requests
from MongoDBConnector import MongoDBQueryConnector  

# Instantiate MongoDBServer
mongo_server = MongoDBQueryConnector()
nodeHealth = True

# AUTH KEY
AUTH_KEY = '10e61c6b073b7b051225fbd21f0f0d879fc59afd07119cba10ff2ae3da5575dd'

# NODE ID
NODE_ID = 1

# MASTER URL
MASTER_URL = 'http://127.0.0.1:5000'

# SHARDING 
SHARDING = {
    1: [("<!!!>","<created>"),("<Jaroslav_Volak>", "<isCitizenOf>")],
    2: [("<Jaroslav_Volek>", "<hasFamilyName>"), ("<Steve_Pickell>", "<hasGender>")],
    3: [("<Steve_Pickell>", "<hasGivenName>"), ("<₩uNo>", "<wasBornIn>")]
}

NODE_SHARDS = []

# Initialize Flask app
app = Flask(__name__)
CORS(app)

def get_sharding_data():
    global SHARDING, NODE_SHARDS

    payload = {"server_id": NODE_ID, "auth_key": AUTH_KEY}
    response = requests.post(MASTER_URL + '/master/get_sharding_data/', data=payload)
    if response.status_code == 200:
        result = (response.json())
        SHARDING = result['sharding']
        NODE_SHARDS = result['node_shards']
        print(SHARDING)
        print(NODE_SHARDS)
    
@app.route('/node/get_shardings_data_new', methods=['GET'])
def updateShardingsData():
    try:
        get_sharding_data()
        return jsonify({"status": True}), 200
    except Exception as e:
        return jsonify({"status": False}), 503

# Define routes for different queries
# 1)
@app.route('/node/search_by_subject', methods=['POST'])
def search():
    try:
        global nodeHealth
        if nodeHealth:
            # Get parameters from the request
            auth_key = request.form['auth_key']

            if not auth_key.__eq__(AUTH_KEY):
                return abort(400) # not authorized
            
            # Get parameters from the request
            subject = request.form['subject']
            print('subject:', subject)
            
            # Call the search method from MongoDBServer class
            result, length, status = mongo_server.fetch_rows_related_to_subject(subject)

            # Return response as JSON
            return jsonify({"result": result, "length": length, "status":status})
        else:
            print('node is OFF')
            abort(503)
    except Exception as e:
        print(e)
        abort(503)

# 2)
@app.route('/node/update_by_subject_predicate_key', methods=['POST'])
def update():
    try:
        global nodeHealth
        if nodeHealth:
            # Get parameters from the request
            auth_key = request.form['auth_key']

            if not auth_key.__eq__(AUTH_KEY):
                return abort(400) # not authorized
            
            # Get parameters from the request
            subject = request.form['subject']
            predicate = request.form['predicate']
            new_object = request.form['object']

            print('here1')
            if not mongo_server.withinValidShard(subject, predicate, SHARDING, NODE_SHARDS):
                print('does not fall within a shard\n')
                abort(503)

            print('here2')
            # Call the update method from MongoDBServer class
            result = mongo_server.update_or_add_subject_predicate(subject, predicate, new_object)
            print('here3')

            # response = requests.post(MASTER_URL + '/update_info', data={'source_id': NODE_ID, "result": result['new_row']})

            return jsonify({'status': result['status'], 'new_row': result['new_row'], 'old_row': result['old_row']})
            # Return response as JSON
            # if response.status_code==200:
            #     return jsonify(result)
            # else:
            #     print('Something went wrong. line 74. Master didnt recieve update')
            #     return jsonify(result)
        else:
            print('Node health is False.')
            abort(503)
    except Exception as e:
        print('Exception Occured:',e)
        abort(503)

# 3)
@app.route('/node/merge_changes', methods=['POST'])
def merge():
    try:
        global nodeHealth
        if nodeHealth:
            # Get parameters from the request
            auth_key = request.form['auth_key']
            print("auth_key: ", auth_key)
            if not auth_key.__eq__(AUTH_KEY):
                print('user_id not found')
                abort(400) # not authorized
            
            # Get parameters from the request
            print(request.form['modifications'])
            modifications = eval(request.form['modifications'])
            print(modifications)
            print(type(modifications))

            # Call the merge method from MongoDBServer class
            result = mongo_server.mergeSelf(modifications, SHARDING, NODE_SHARDS)

            # Return response as JSON
            return jsonify({"status": result})
        else:
            return jsonify({"status": False}), 200
    except Exception as e:
        print(e)
        return jsonify({"status": False}), 200

# 4)
@app.route('/node/echo_changes', methods=['POST'])
def broadcastLocalUpdates():
    try:
        global nodeHealth
        if nodeHealth:
            # Get parameters from the request
            auth_key = request.form['auth_key']

            if not auth_key.__eq__(AUTH_KEY):
                return abort(400) # not authorized
            
            # Call the merge method from MongoDBServer class
            print('here10')
            result = mongo_server.remoteMergeLocalUpdates()
            print('here13')
            # Return response as JSON
            return jsonify({'result': result})
        else:
            return jsonify({"status": False}), 200
    except Exception as e:
        print(e)
        return jsonify({"status": False}), 200


# 5)
@app.route('/node/health', methods=['GET'])
def node_health():
    global nodeHealth
    if nodeHealth:
        return jsonify({"nodeStatus": True})
    else:
        abort(503)


# 6)
@app.route('/node/turnoff', methods=['GET'])
def turnoff():
    global nodeHealth
    nodeHealth = False
    return jsonify({"status":True, "message": "node is turned off and will not respond to any request with status 200."})


# 7)
@app.route('/node/turnon', methods=['GET'])
def turnon():
    global nodeHealth
    nodeHealth = True
    return jsonify({"status":True, "message": "node is turned on and is ready to take requests"})

@app.route('/node/test_server', methods=['GET'])
def test():
    if nodeHealth:
        html_string = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Server 1 (Mongo)</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    background-image: linear-gradient(to bottom, #50C9B3, #28A745);
                    margin: 0;
                    padding: 0;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    flex-direction: column;
                }

                h1 {
                    color: #333;
                    text-align: center;
                    margin-bottom: 20px;
                }

                p {
                    color: #666;
                    text-align: center;
                    line-height: 1.6;
                    max-width: 80%;
                }
            </style>
        </head>
        <body>
            <h1>Server is up and healthy!!!</h1>
            <p>You are seeing this message because the server is running and is healthy. Please make a GET request to <code>'/node/turnoff'</code> to see a different message.</p>
        </body>
        </html>
        """
        return render_template_string(html_string)
    else:
        html_string = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Server 1 (Mongo)</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    background-image: linear-gradient(to right, #ffc107, #ff9800);
                    margin: 0;
                    padding: 0;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    flex-direction: column;
                }

                h1 {
                    color: #333;
                    text-align: center;
                    margin-bottom: 20px;
                }

                p {
                    color: #666;
                    text-align: center;
                    line-height: 1.6;
                    max-width: 80%;
                }
            </style>
        </head>
        <body>
            <h1>Server is down and cannot process any queries!!!</h1>
            <p> you are seeing this message because, server is down & is not healthy. Please make a get request to '/node/turnon' to see a different message </p>
        </body>
        </html>
        """
        return render_template_string(html_string)


if __name__ == '__main__':
    get_sharding_data()
    # Run the Flask app on the specified port
    app.run(port=5091, debug=True)



# @app.route('/node/merge_with_target/',methods=['POST'])
# def mergeWithTarget():
#     global nodeHealth
#     if nodeHealth:
#         target_id = request.form['target_server_id']   
#         source_id = NODE_ID
#         merge_payload = mongo_server.remoteMergeLocalUpdates() 

#         response = requests.post(MASTER_URL + '/merge_node_request', data={"source_id": source_id, "target_id":target_id, "merge_payload": merge_payload })

#         if response.status_code == 200:
#             return {"status": True}, 200
#         else:
#             return {"status": False}, 200        
#     else:
#         abort(503)
