from datetime import datetime
from flask import Flask, abort, request, jsonify
from flask_cors import CORS
import requests
import threading
import json
import secrets
import random
import time

app = Flask(__name__)
CORS(app)

# List of active nodes (constant)
NODE_URLS =  {1: 'http://127.0.0.1:5091', 2: 'http://127.0.0.1:6091', 3: 'http://127.0.0.1:7091'} 

# List of active nodes (changeable at runtime)
active_nodes = ['http://127.0.0.1:5091', 'http://127.0.0.1:6091', 'http://127.0.0.1:7091']
node_health = {1:True, 2:True, 3:True}

# AUTH_KEY
AUTH_KEY = '10e61c6b073b7b051225fbd21f0f0d879fc59afd07119cba10ff2ae3da5575dd'
ADMIN_KEY = '10e61c6b073a7b081225fbd21f0f0d879fc59afd07119cba10fa2ae3da6675dd'

# Node Ids
NODE_IDS = {
    'http://127.0.0.1:5091': 1,
    'http://127.0.0.1:6091': 2,
    'http://127.0.0.1:7091': 3,
}

# dict: assignment of shards to nodes (constant)
NODE_TO_SHARDS = {
    1: [1, 3],  
    2: [2, 3],  
    3: [1, 2]
}

SHARDS_TO_NODES = {
    1: [1, 3],
    2: [2, 3],
    3: [1, 2]
}

# dict: assignment of shards to nodes (can be modified at runtime)
node_shards = {
    1: [1, 3],  
    2: [2, 3],  
    3: [1, 2]
}

shard_nodes = {
    1: [1, 3],
    2: [2, 3],
    3: [1, 2]
}

# dict: shard_id to subject_min, subject_max
SHARDING = {
    1: [("<!!!>","<created>"),("<Jaroslav_Volak>", "<isCitizenOf>")],
    2: [("<Jaroslav_Volek>", "<hasFamilyName>"), ("<Steve_Pickell>", "<hasGender>")],
    3: [("<Steve_Pickell>", "<hasGivenName>"), ("<₩uNo>", "<wasBornIn>")]
}

# user_id, node-to-shard responsibilities
USERS = {

}


node_merge_status_list = {1: [(2,datetime.min.strftime("%Y-%m-%d %H:%M:%S.%f")), (3,datetime.min.strftime("%Y-%m-%d %H:%M:%S.%f"))], 2: [(3,datetime.min.strftime("%Y-%m-%d %H:%M:%S.%f")), (1,datetime.min.strftime("%Y-%m-%d %H:%M:%S.%f"))], 3: [(1,datetime.min.strftime("%Y-%m-%d %H:%M:%S.%f")),(2,datetime.min.strftime("%Y-%m-%d %H:%M:%S.%f"))]}

node_update_operations_list = {1:{}, 2:{}, 3:{}}

user_session_metadata = {}

def getShardID(subject , predicate=None):
    if predicate==None:
        for shard in list(SHARDING.keys()):
            if ((SHARDING[shard][0][0] <= subject)) and ((SHARDING[shard][1][0] >= subject)):
                return shard
        return 0
    else:
        for shard in list(SHARDING.keys()):
            if ((SHARDING[shard][0][0] <= subject) and (SHARDING[shard][0][1] <= predicate)) and ((SHARDING[shard][1][0] >= subject) and (SHARDING[shard][1][1] >= predicate)):
                return shard
        return 0

def existElement(node_shards, shard, i):
    try:
        node_shards[i].index(shard)
        return True
    except Exception as e:
        return False


def filter_updates(shard_id, updates):
    filtered_updates = {}

    for key in list(dict(updates).keys()):
        if SHARDING[shard_id][0] <= key and key <= SHARDING[shard_id][1]:
            filtered_updates[key] = updates[key]

    return filter_updates

def merge_updates(updates1, updates2):
    merged_updates = {}

    for key in list(dict(updates1).keys()):
        merged_updates[key] = updates1[key]

    for key in list(dict(updates2).keys()):
        merged_updates[key] = updates2[key]

    return merged_updates



def send_merge(target_node_id, updates):
    payload = {"modifications": updates, 'auth_key': AUTH_KEY}
    response = requests.post(active_nodes[target_node_id] + "/node/merge_changes", data=payload)
    response = json.loads(response.json())
    
    if response.status_code == 200:
        return True
    else:
        return False
    
def findA(List, v):
    for value in List:
        if value != v:
            return v

def manage_shards_nodes(node_url, flag=0):
    '''
        flag=0 ==> node_url has gone down
        flag=1 ==> node_url is back online
    '''
    if flag==0:
        global USERS
        node_id = NODE_IDS[node_url]

        shards = node_shards[node_id]
        status = True
        USERS_COPY = USERS.copy()
        for k, v in USERS_COPY.items():
            for shard in shards:
                if v[shard] == node_id:
                    v[shard] = findA(SHARDS_TO_NODES[shard], node_id)
                    USERS[k] = v
        return True
    elif flag==1:
        node_id = NODE_IDS[node_url]

        shards = node_shards[node_id]
        status = True
        USERS_COPY = USERS.copy()
        for k, v in USERS_COPY.items():
            for shard in shards:
                if v[shard] != node_id:
                    v[shard] = SHARDS_TO_NODES[shard][random.randint(0, len(SHARDS_TO_NODES[shard]) - 1)]
                    USERS[k] = v
        return True

def manage_nodes(node_url, flag=0):
    '''
        flag=0 ==> node_url has gone down
        flag=1 ==> node_url is back online
    '''
    if flag==0:
        node_id = NODE_IDS[node_url]

        shards = node_shards[node_id]

        status = True
        for shard in shards:
            for nodeID in list(node_shards.keys()):
                if node_health[nodeID] and existElement(node_shards, shard, nodeID):
                    filterupdates = filter_updates(shard, node_update_operations_list[node_id])
                    result = send_merge(nodeID, filterupdates)  
                    status = status and result 
                    print("status: ", status)
                    
        print("Final status:", status)
        return status
    elif flag==1:
        node_id = NODE_IDS[node_url]

        shards = NODE_TO_SHARDS[node_id]

        filtered_updates = []
        for shard in shards:
            for node_id in list(node_shards.keys()):
                if existElement(node_shards, shard, node_id):
                    filtered_updates.append(filter_updates(shard, node_update_operations_list[node_id]))

        merged_updates = {}
        for i in range(0, len(filtered_updates)-1):
            if i==0:
                merged_updates = merge_updates(filtered_updates[i], filtered_updates[i+1])
            else:
                merged_updates = merge_updates(merged_updates, filtered_updates[i+1])
        
        status = send_merge(node_id, merged_updates)
        return status   


def check_node_health(node_url):
    try:
        response = requests.get(node_url + '/node/health', timeout=2)
        print(response.status_code)
        if response.ok:
            if node_health[NODE_IDS[node_url]] == False:
                manage_nodes(node_url, flag=1)
            node_health[NODE_IDS[node_url]] = True
        else:
            if node_health[NODE_IDS[node_url]] == True:
                manage_nodes(node_url, flag=0)
            node_health[NODE_IDS[node_url]] = False

            print(f"Node {node_url} is down.")
    except Exception as e:
        active_nodes.remove(node_url)
        print(f"Node {node_url} is down.")

def health_check():
    count = 0
    while True:
        print("Active_nodes", active_nodes)
        for node_url in active_nodes:
            threading.Thread(target=check_node_health, args=(node_url,)).start()
        # Adjust the check interval based on your requirement
        time.sleep(5)
        if count> 5000:
            break

def NTS_to_STN(node_to_shards):
    stn = {1: [], 2:[], 3:[]}
    for i in list(node_to_shards.keys()):
        for shard in node_to_shards[i]:
            stn[shard].append(i) 
    return stn         
@app.route('/get-user-settings/', methods=['GET'])
def usersettings():
    try:
        global USERS
        hash_function = secrets.token_bytes
        random_bytes = hash_function(32)
        hex_string = random_bytes.hex() 
        user_id = hex_string

        meta = {}
        for shard in list(SHARDING.keys()):
            random_index = random.randint(0, len(SHARDS_TO_NODES[shard]) - 1)
            random_node = None
            if node_health[SHARDS_TO_NODES[shard][random_index]] == False:
                random_node = findA(SHARDS_TO_NODES[shard], SHARDS_TO_NODES[shard][random_index])
            else:
                random_node = SHARDS_TO_NODES[shard][random_index]
            
            meta[shard] = random_node
        
        USERS[hex_string] = meta
        print("USERS:", USERS)
        return jsonify({'user_id': hex_string, 'status': True}), 200
    except Exception as e:
        print(e)
        return jsonify({'user_id': '', 'status': False}), 200
    
@app.route('/set-shard-settings/', methods=['POST'])
def clustersettings():
    try:
        global NODE_TO_SHARDS
        global SHARDS_TO_NODES
        admin_key = request.form['admin_key']
        if not admin_key.__eq__(ADMIN_KEY):
            return jsonify({'settings': '', 'status': False}), 200
        
        settings = request.form['new_settings']
        NODE_TO_SHARDS = settings
        SHARDS_TO_NODES = NTS_to_STN(NODE_TO_SHARDS)
        for active_node in active_nodes:
            response = requests.get(active_node + '/node/get_shardings_data_new')
            if not response.ok:
                print('get to update_settings unsuccessfull.')
        return jsonify({'settings': [NODE_TO_SHARDS, SHARDS_TO_NODES], 'status': True}), 200 
    except Exception as e:
        print(e)
        return jsonify({'settings': '', 'status': False}), 200


@app.route('/search', methods=['POST'])
def search_request():
    try:
        start_time = time.time()
        global USERS
        # Get the subject parameter from the request
        print(request.form)

        user_id = request.form['user_id']
        subject = request.form['subject']

        if user_id not in list(USERS.keys()):
            return jsonify({'status': False}), 200
                
        print(user_id, subject)

        shard = getShardID(subject)
        if shard == 0:
            return jsonify({"status": False}), 200
         
        node_id = USERS[user_id][shard]
        print("node_id: ",node_id)
        response = requests.post(NODE_URLS[int(node_id)] + '/node/search_by_subject', data={'auth_key':AUTH_KEY, 'subject': subject})

        if response.status_code!=200:
            return jsonify({"status": False}), 200
        
        dic = response.json()

        if not dic['status']:
            return jsonify({"status": False}), 200

        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time:.4f} seconds")
        return jsonify({"status": True, 'result': dic['result'], 'length': dic['length']}), 200
    except Exception as e:
        print(e)
        return jsonify({"status": False}), 200

@app.route('/update', methods=['POST'])
def update_request():
    try:
        start_time = time.time()
        global USERS
        # Get the subject parameter from the request
        print(request.form)

        user_id = request.form['user_id']
        subject = request.form['subject']
        predicate = request.form['predicate']
        new_object = request.form['object']

        if user_id not in list(USERS.keys()):
            return jsonify({'status': False}), 200

        print(user_id, subject, predicate, new_object)

        shard = getShardID(subject, predicate)
        if shard == 0:
            return jsonify({"status": False}), 200
         
        node_id = USERS[user_id][shard]
        print("node_id: ",node_id)
        
        print('Here-1')
        response = requests.post(NODE_URLS[int(node_id)] + '/node/update_by_subject_predicate_key', data={'auth_key': AUTH_KEY, 'subject': subject, 'predicate': predicate, 'object': new_object})

        print('Here0')
        if response.status_code!=200:
            return jsonify({"status": False}), 200
        
        print('Here1')
        dic = response.json()
        print('Here2')

        if not dic['status']:
            return jsonify({"status": False}), 200
        
        print('Here3')
        node_update_operations_list[int(node_id)][(subject, predicate)] = {'new_object': dic['new_row']['object'], 'timestamp': dic['new_row']['timestamp']}
        
        print('Here4')

        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time:.4f} seconds")
        return jsonify({"status": True, 'new_row': dic['new_row']}), 200
    except Exception as e:
        print(e)
        return jsonify({"status": False}), 200

@app.route('/merge_2_servers/', methods=['POST'])
def merge_servers():
    try:
        start_time = time.time()
        
        global USERS
        user_id = request.form['user_id']

        if user_id not in list(USERS.keys()):
            return jsonify({'status': False}), 200

        source_id = request.form['source_id']
        target_id = request.form['target_id']

        response1 = requests.post(NODE_URLS[int(source_id)] + '/node/echo_changes', data={'auth_key': AUTH_KEY})
        response2 = requests.post(NODE_URLS[int(target_id)] + '/node/echo_changes', data={'auth_key': AUTH_KEY})

        result1 = response1.json()
        result2 = response2.json()

        print('echo_changes: ', result2['result'])
        print('echo_changes: ', result1['result'])

        response3 = requests.post(NODE_URLS[int(source_id)] + '/node/merge_changes', data={'auth_key': AUTH_KEY, 'modifications': str(result2['result'])})
        response4 = requests.post(NODE_URLS[int(target_id)] + '/node/merge_changes', data={'auth_key': AUTH_KEY, 'modifications': str(result1['result'])})

        print('here4')
        print('_________________________')
        print(response3.text)
        print(response4.text)
        print('_________________________')
        
        print('merge_changes status: ', json.loads(response3.text)['status'])
        print('merge_changes status: ', json.loads(response4.text)['status'])

        status =  json.loads(response3.text)['status'] and  json.loads(response4.text)['status']
        message = None 
        if status: 
            message = 'success' 
        else:
            message = 'failed'
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time:.4f} seconds")
        return jsonify({'status': status , 'message': message}), 200
        
    except Exception as e:
        print(e)
        return jsonify({'status': False}), 200

    
@app.route('/master/get_sharding_data/', methods=['POST'])
def get_sharding_data():
    auth_key = request.form['auth_key']
    server_id = request.form['server_id']

    if not auth_key.__eq__(AUTH_KEY):
        return jsonify({'status': False}), 400
    
    payload = {
        "status": True,
        "sharding": SHARDING,
        "node_shards": node_shards[int(server_id)]
    }
    return jsonify(payload), 200

@app.route('/master/get_sharding_data/client', methods=['GET'])
def get_sharding_data_client():
    global USERS
    user_id = request.form['user_id']
    
    if user_id not in list(USERS.keys()):
        return jsonify({'status':False}), 200
    
    payload = {
        "status": True,
        "user_id": user_id,
        "meta-data": USERS[user_id],   
    }
    return jsonify(payload), 200
    

if __name__ == '__main__':
    # Start the health checking thread
    threading.Thread(target=health_check, daemon=True).start()
    # Run the Flask app
    app.run(port=5000, debug=True)





# @app.route('/merge_node_request', methods=['POST'])
# def merge_request():
#     try:
#         source_id = request.form['source_id']
#         target_id = request.form['target_id']
#         merge_payload = request.form['merge_payload']

#         print(merge_payload)
#         response = requests.post(NODE_URLS[int(target_id)] + '/node/echo_changes', data={"target_server_id": source_id})

#         merge_payload2 = json.loads(response.json())
#         print(merge_payload2)

#         response = requests.post(NODE_URLS[int(target_id)] + '/node/merge_changes', data={"source_server_id":source_id, "modifications": merge_payload})

#         if response.status_code != 200:
#             print('something went wrong here.: line 184')
#             return {"message": "Error during merge on target node", "status": False}, 500  # Return error response

        
#         payload = {"modifications": merge_payload2, "source_server_id": target_id}

#         response = requests.post(NODE_URLS[int(source_id)] + "/node/merge_changes", data=payload)

#         if response.status_code == 200:
#             return {"status": True}, 200
#         else: 
#             return abort(500)
#     except Exception as e:
#         print(e) 

# @app.route('/get_node_info', methods=['GET'])
# def nodeInfo():
#     master_json = {
#         "type": "Master",
#         "server_id": 0,
#         "health_status": True,
#         "shards": [],
#         "syncStatus": []
#     }

#     node_jsons = []
#     for node_url in NODE_URLS:
#         node_id = NODE_IDS[node_url]
#         health_status = node_health[node_id]
#         shards = node_shards[node_id]
#         syncStatus = node_merge_status_list[node_id]

#         node_json = {
#             "type": "Node",
#             "server_id": node_id,
#             "health_status": health_status,
#             "shards": shards,
#             "syncStatus": syncStatus
#         }

#         node_jsons.append(node_json)
    
#     node_jsons.append(master_json)
#     print(node_jsons)
#     return jsonify(node_jsons)

# @app.route('/settings/shardings', methods=['POST'])
# def setSharding():
#     SHARDING = request.form['new_sharding']
#     return {"status": True}, 200

# @app.route('settings/shardings/transfer_from_node_to_node', methods=['POST'])
# def transferShardFromNodeToNode():
#     try:
#         shard_id = request.form['shard_id']
#         source_server_id = request.form['source_server_id']
#         target_server_id = request.form['target_server_id']

#         # get shard from source server:
#         response = requests.post(active_nodes[int(source_server_id)] + '/get_shard/', data={"shard_id":shard_id})
        
#         if response.status_code == 200:
#             shard = response.json()
#         else:
#             abort(500)
#         # send shard to target server:
#         response2 = requests.post(active_nodes[int(target_server_id)] + '/send_shard/', data = {"shard_id": shard})
#         del response

#         if response2.status_code == 200:
#             return {"status": True}, 200 
#         else:
#             abort(500)
#     except Exception as e:
#         print(e)
