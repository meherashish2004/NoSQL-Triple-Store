from pymongo import MongoClient
from datetime import datetime
from tqdm import tqdm
from dbQueryConnector import DatabaseQueryConnector
import pandas as pd

class MongoDBQueryConnector(DatabaseQueryConnector):
    def __init__(self):
        # Connect to MongoDB

        # Supply the host and port accordingly
        self.client = MongoClient('localhost', 27017)
        # Supply your database name. Cnnot contain ' ' in the name.  
        self.db = self.client['Yago']  
        # Supply your collection name. Here it is: Yago  (Yago.Yago)
        self.collection = self.db['Yago']  
        # set state to indicate whether the server has un merged changes or not
        self.isModified = False
        # set state to indicate when the server was recently merged
        self.mergedAt = datetime.now()
        # set state to record update modifications made to server along with timestamp. 
        # Using Dictionary for efficient retriveal, updation, and avoiding of duplicates & older modifications.
        self.updateModifications = {}


    def fetch_rows_related_to_subject(self, subject):
        """
        Function to fetch all rows related to a subject from the YAGO dataset.
        
        Args:
        - subject: The subject for which rows are to be fetched.
        
        Returns:
        - A list of rows related to the subject and length of the list and success/failure status (bool).
        """
        try:
            print('subject: ', subject)
            rows = self.collection.find({'subject': subject})
            new_rows = [ {k: v for k, v in row.items() if k != '_id'} for row in rows]
            return (list(new_rows), len(list(rows)), True)
        except Exception as e:
            print(e)
            return ([], 0, False)
    

    def update_or_add_subject_predicate(self, subject, predicate, new_object, timestampArg=None):
        """
        Function to update or add an object-based subject and predicate in the YAGO dataset.
        
        Args:
        - subject: The subject to be updated or added.
        - predicate: The predicate associated with the subject.
        - new_object: The new object to be associated with the subject and predicate.
        - timestampArg: Default value `None`. When given in args, thegiven timestamp is used for the timestamp attribute of the row.
        
        Returns:
        - True, new_row, old_row, otherwise raises Exception & returns False & error obj.
        """
        try:
            # Check if the subject and predicate already exist
            existing_row = self.collection.find_one({'subject': subject, 'predicate': predicate})
            
            if existing_row:
                # Update the existing row with the new object
                old_timestamp = existing_row['timestamp']
                old_object = existing_row['object']

                if timestampArg==None:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                else:
                    timestamp = timestampArg

                self.collection.update_one({'_id': existing_row['_id']}, {'$set': {'object': new_object, 'timestamp':timestamp}})
                
                new_entry = {"timestamp": timestamp, "subject": subject, "predicate": predicate, "object": new_object}
                self.updateModifications[str((subject, predicate))] = {"new_object": new_object, "timestamp": timestamp}
                self.isModified = True
                return ({"new_row": {"subject":subject, "predicate": predicate, "object": new_object, "timestamp":timestamp},
                         "old_row": {"subject":subject, "predicate": predicate, "object": old_object, "timestamp":old_timestamp},
                         "status": True})
            else:
                # Add a new row with the subject, predicate, and object
                if timestampArg==None:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                else:
                    timestamp = timestampArg
                
                new_row = {'subject': subject, 'predicate': predicate, 'object': new_object, 'timestamp': timestamp}
                self.collection.insert_one(new_row)

                new_entry = {"timestamp": timestamp, "subject": subject, "predicate": predicate, "object": new_object}
                self.updateModifications[str(subject, predicate)] = {"new_object": new_object, "timestamp": timestamp}
                self.isModified = True
                return ({"new_row": {"subject":subject, "predicate": predicate, "object": new_object, "timestamp":timestamp},
                         "old_row": {},
                         "status": True})
        except Exception as e:
            print(e)
            return ({"new_row": {},
                     "old_row": {},
                     "status": False,
                     "error": e})
            

    def withinValidShard(self, subject, predicate, shardings, node_shards):
        print(subject, predicate, shardings, node_shards)
        print('Here 10')
        for shard in node_shards:
            print('Here 9')
            print('shard', shard)
            if ((shardings[str(shard)][0][0] <= subject) and (shardings[str(shard)][0][1] <= predicate)) and ((shardings[str(shard)][1][0] >= subject) and (shardings[str(shard)][1][1] >= predicate)):
                print(shard)
                return True
        return False

    def mergeSelf(self, modifications_dic, shardings, node_shards):
        """
        Function to merge modifications from another MongoDB server represented by modifications Dictionary.
        
        Args:
        - modifications_df: A Dictionary containing modifications with timestamps.
        
        Returns:
        - True, otherwise raises exception.
        """

        try:
            # Group modifications by subject and predicate
            keysList = list(dict(modifications_dic).keys())
            
            for spkey in keysList:
                subject, predicate = eval(spkey)
                print('subject:', subject)
                print('predicate:', predicate)
                if not self.withinValidShard(subject, predicate, shardings, node_shards):
                    pass
                
                # Get the most recent modification for each subject-predicate pair
                latest_modification = dict(modifications_dic)[spkey]
                new_object = latest_modification['new_object']
                new_timestamp = latest_modification['timestamp']
                
                # Check if the subject and predicate already exist
                existing_row = self.collection.find_one({'subject': subject, 'predicate': predicate})
                timestamp_new = datetime.strptime(new_timestamp, "%Y-%m-%d %H:%M:%S.%f")
                timestamp_old = datetime.strptime(existing_row['timestamp'], "%Y-%m-%d %H:%M:%S.%f")

                if existing_row:
                    # Update the existing row with the new object
                    if timestamp_new > timestamp_old: # update your row to reflect the most recent state
                        self.collection.update_one({'_id': existing_row['_id']}, {'$set': {'object': new_object, 'timestamp':new_timestamp}})
                    else:
                        pass # do nothing, as you already have the latest changes
                else:
                    # Add a new row with the subject, predicate, and object, along with timestamp
                    new_row = {'subject': subject, 'predicate': predicate, 'object': new_object, 'timestamp': new_timestamp}
                    self.collection.insert_one(new_row)
            
            self.mergedAt = datetime.now()

            # Print a message indicating the merge operation is completed
            print("Merge operation completed.")
            return True
        except Exception as e:
            raise e
    
    
    def remoteMergeLocalUpdates(self):
        """
        Function to return local time-stamped modifications Dictionary data structure to another server, to allow merging.
        
        Args:
        - None

        Returns:
        - modifications_dic: A python Dictionary containing modifications with timestamps.
        """
        # return local copy of modifications list along with timestamps
        # for other servers to process and merge with their changes
        print('here12')
        print(self.updateModifications)
        modifications_dic = self.updateModifications.copy()
        print('here11')
        return modifications_dic

"""
***************************************** TESTING CODE FOR SINGLE CLASS *****************************************************************
*               - test_connection() --- basic testing code, to instantiate object and check connection                                  *
*                                       via search & update queries.                                                                    *
*                                                                                                                                       *
*               - populate_database() --- to add rows to the database using update query method,                                        *
*                                         from a .tsv file                                                                              *
*                                                                                                                                       *
*****************************************************************************************************************************************  
"""

def test_connection():    
    """
    Function to test if the object is instantiating as expected and connecting to the database.

    Args:
    - None

    Returns:
    - None
    """
    mongoServer = MongoDBQueryConnector()
    
    # Update or add an object-based subject and predicate
    subject = 'Albert_Einstein'
    predicate = 'hasWonPrize'
    new_object = 'Nobel_Prize_in_Physics'
    mongoServer.update_or_add_subject_predicate(subject, predicate, new_object)

    # Fetch all rows related to a subject
    subject = 'Albert_Einstein'
    rows, length, status = mongoServer.fetch_rows_related_to_subject(subject)
    print(f"Rows related to '{subject}':")
    for row in rows:
        print(row)

    print(mongoServer.updateModifications)

def populate_database():
    """
    Function to populate the database with data-rows from .tsv file 
    The file path is hardcoded, since this function is not meant for general usage.

    Args:
    - None

    Returns: 
    - None
    """
    mongoServer = MongoDBQueryConnector()
    print("created server instance... ")

    # Count the number of lines in the file (equal to number of rows + 1)
    total_lines = sum(1 for _ in open('yago_uniques.tsv', encoding='utf-8'))

    # Initialize tqdm to show progress
    with tqdm(total=total_lines, desc="\033[1;32m Reading data from .tsv file. In progress ... \033[m", colour='magenta') as pbar:
        df_iterator = pd.read_csv("yago_uniques.tsv", sep='	', iterator=True, chunksize=10000)
        num_rows = 0
        num_chunks = 0
        for chunk in df_iterator:
            num_chunks += 1 
            num_rows += len(chunk)
            for index, row in chunk.iterrows():
                # Extract values from the row
                subject = row[0]  # Assuming first column is subject
                predicate = row[1]  # Assuming second column is predicate
                obj = row[2]  # Assuming third column is object
                timestamp = datetime.min.strftime("%Y-%m-%d %H:%M:%S.%f")  # Adding min-default-starting-timestamp

                # Insert data into MongoDB
                try:
                    mongoServer.update_or_add_subject_predicate(subject, predicate, obj, timestamp)
                except Exception as e:
                    raise e
                finally:
                    pbar.update(1)
            # print("\033[1;32m Number of rows: ", num_rows)
        print("Num rows:", num_rows)

# For testing
if __name__ == "__main__":

    test_connection()
    # populate_database()
