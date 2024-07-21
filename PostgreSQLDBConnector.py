import psycopg2
from datetime import datetime
from tqdm import tqdm
from dbQueryConnector import DatabaseQueryConnector
import pandas as pd

class PostgreSQLQueryConnector(DatabaseQueryConnector):
    def __init__(self):
        # Connect to PostgreSQL
        
        # Supply the database credentials accordingly
        self.conn = psycopg2.connect(
            dbname='Yago',
            user='postgres',
            password='Joshuga@2103',
            host='localhost',
            port='5432'
        )
        self.cur = self.conn.cursor()

        # Set state to indicate whether the server has unmerged changes or not
        self.isModified = False

        # Set state to indicate when the server was recently merged
        self.mergedAt = datetime.now()

        # Set state to record update modifications made to the server along with the timestamp.
        # Using Dictionary for efficient retrieval, updating, and avoiding duplicates & older modifications.
        self.updateModifications = {}

        self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")

        # Fetch all the rows from the result set
        rows = self.cur.fetchall()

        # Display the table names
        for row in rows:
            print(row[0])
    
    def fetch_rows_related_to_subject(self, subject):
        """
        Function to fetch all rows related to a subject from the YAGO dataset.
        
        Args:
        - subject: The subject for which rows are to be fetched.
        
        Returns:
        - A list of rows related to the subject, length of the list, and success/failure status (bool).
        """
        try:
            query = 'SELECT * FROM public."Yago 13" WHERE subject = %s'
            self.cur.execute(query, (subject,))
            rows = self.cur.fetchall()
            return (rows, len(rows), True)
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
        - timestampArg: Default value `None`. When given in args, the given timestamp is used for the timestamp attribute of the row.
        
        Returns:
        - True, new_row, old_row, otherwise raises Exception & returns False & error obj.
        """
        try:
            # Check if the subject and predicate already exist
            query = 'SELECT * FROM public."Yago 13" WHERE subject = %s AND predicate = %s'
            self.cur.execute(query, (subject, predicate))
            existing_row = self.cur.fetchone()

            if existing_row:
                print('existing row', existing_row)
                # Update the existing row with the new object
                old_timestamp = existing_row[3]
                old_object = existing_row[1]

                if timestampArg is None:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                else:
                    timestamp = timestampArg

                update_query = 'UPDATE public."Yago 13" SET object = %s, timestamp = %s WHERE subject = %s AND predicate = %s'
                self.cur.execute(update_query, (new_object, timestamp, subject, predicate))

                print('existing row updated')
                new_entry = {"timestamp": timestamp, "subject": subject, "predicate": predicate, "object": new_object}
                self.updateModifications[str((subject, predicate))] = {"new_object": new_object, "timestamp": timestamp}
                self.isModified = True
                self.conn.commit()
                
                print('commited')
                return ({"new_row": {"subject": subject, "predicate": predicate, "object": new_object, "timestamp": timestamp},
                         "old_row": {"subject": subject, "predicate": predicate, "object": old_object, "timestamp": old_timestamp},
                         "status": True})
            else:
                # Add a new row with the subject, predicate, and object
                print('non-existing row')
                if timestampArg is None:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                else:
                    timestamp = timestampArg

                insert_query = 'INSERT INTO public."Yago 13" (subject, predicate, object, timestamp) VALUES (%s, %s, %s, %s)'
                self.cur.execute(insert_query, (subject, predicate, new_object, timestamp))
                print('non-existing row added')
                new_entry = {"timestamp": timestamp, "subject": subject, "predicate": predicate, "object": new_object}
                self.updateModifications[str((subject, predicate))] = {"new_object": new_object, "timestamp": timestamp}
                self.isModified = True
                self.conn.commit()

                print('committed')
                return ({"new_row": {"subject": subject, "predicate": predicate, "object": new_object, "timestamp": timestamp},
                         "old_row": {},
                         "status": True})
        except Exception as e:
            print(e)
            self.conn.rollback()
            return ({"new_row": {},
                     "old_row": {},
                     "status": False,
                     "error": str(e)})
    
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
        Function to merge modifications from another PostgreSQL server represented by modifications Dictionary.
        
        Args:
        - modifications_dic: A Dictionary containing modifications with timestamps.
        
        Returns:
        - True, otherwise raises exception.
        """
        try:
            for key, modification in modifications_dic.items():
                subject, predicate = eval(key)
                print('subject:', subject)
                print('predicate:', predicate)
                
                if not self.withinValidShard(subject, predicate, shardings, node_shards):
                    pass

                # Get the most recent modification for each subject-predicate pair
                new_object = modification['new_object']
                new_timestamp = modification['timestamp']

                print('object',new_object)
                print('timestamp',new_timestamp)
                # Check if the subject and predicate already exist
                query = 'SELECT * FROM public."Yago 13" WHERE subject = %s AND predicate = %s'
                self.cur.execute(query, (subject, predicate))
                existing_row = self.cur.fetchone()

                if existing_row:
                    # Update the existing row with the new object
                    if datetime.strptime(new_timestamp, "%Y-%m-%d %H:%M:%S.%f") > datetime.strptime(existing_row[3], "%Y-%m-%d %H:%M:%S.%f"):
                        update_query = 'UPDATE public."Yago 13" SET object = %s, timestamp = %s WHERE subject = %s AND predicate = %s'
                        self.cur.execute(update_query, (new_object, new_timestamp, subject, predicate))
                else:
                    # Add a new row with the subject, predicate, and object
                    insert_query = 'INSERT INTO public."Yago 13" (subject, predicate, object, timestamp) VALUES (%s, %s, %s, %s)'
                    self.cur.execute(insert_query, (subject, predicate, new_object, new_timestamp))
            
            self.mergedAt = datetime.now()
            print("Merge operation completed.")
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            raise e

    def remoteMergeLocalUpdates(self):
        """
        Function to return local time-stamped modifications Dictionary data structure to another server, to allow merging.
        
        Args:
        - None

        Returns:
        - modifications_dic: A python Dictionary containing modifications with timestamps.
        """
        # Return local copy of modifications list along with timestamps
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
    postgresServer = PostgreSQLQueryConnector()
    
    # Update or add an object-based subject and predicate
    subject = '<Steve_Pickell>'
    predicate = 'hasWonPrize'
    new_object = 'Nobel_Prize_in_Physics'
    postgresServer.update_or_add_subject_predicate(subject, predicate, new_object)

    # Fetch all rows related to a subject
    subject = '<Steve_Pickell>'
    rows, length, status = postgresServer.fetch_rows_related_to_subject(subject)
    print(f"Rows related to '{subject}':")
    for row in rows:
        print(row)

    print(postgresServer.updateModifications)

# For testing
if __name__ == "__main__":

    test_connection()
