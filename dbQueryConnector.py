from abc import ABC, abstractmethod

# Define an abstract class
class DatabaseQueryConnector(ABC):
    
    @abstractmethod
    def fetch_rows_related_to_subject(self, subject):
        """
        Function to fetch all rows related to a subject from the YAGO dataset.
        
        Args:
        - subject: The subject for which rows are to be fetched.
        
        Returns:
        - A list of rows related to the subject and length of the list.
        """
        
        pass
    
    @abstractmethod
    def update_or_add_subject_predicate(self, subject, predicate, new_object, timestampArg=None):
        """
        Function to update or add an object-based subject and predicate in the YAGO dataset.
        
        Args:
        - subject: The subject to be updated or added.
        - predicate: The predicate associated with the subject.
        - new_object: The new object to be associated with the subject and predicate.
        - timestampArg: Default value `None`. When given in args, thegiven timestamp is used for the timestamp attribute of the row.
        
        Returns:
        - True, otherwise raises Exception.
        """

        pass

    @abstractmethod
    def mergeSelf(self, modifications_dic):
        """
        Function to merge modifications from another MongoDB server represented by modifications Dictionary.
        
        Args:
        - modifications_df: A Dictionary containing modifications with timestamps.
        
        Returns:
        - True, otherwise raises exception.
        """

        pass

    @abstractmethod
    def remoteMergeLocalUpdates(self):
        """
        Function to return local time-stamped modifications Dictionary datastructure to another server, to allow merging.
        
        Args:
        - None

        Returns:
        - modifications_dic: A python Dictionary containing modifications with timestamps.
        """
        
        pass
