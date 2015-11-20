

class Lookup:


    old_mapping = []
    old_raw_data = []

    def __init__(self):
        self.i_var = "bob"
        pass

    @staticmethod
    def loadcsv():
        # Load the cleaned up and preformatted CSV; format in the following form
        print("in Load CSV ..")

    @staticmethod
    def splitname(old_name):
        # Take the old name in the form <PAR>/<DUN>/<DM>
        # Ensure the size is correct: PAR 3 chars, DUN + DM 2 chars
        # Example:
        print("in Split Name ..")


    @staticmethod
    def matchingdm(dm_name):
        # Data structure
        # id: <DM_NAME_NORMALIZED>
        # Look up the passed dm_name (normalized)??
        print("in Matching DM .. ")
        # Take DM and find in list (via UNION)? SET
        # if yes; copy over all the attributes
        #   then break apart data into the new fields

    @staticmethod
    def copy_dm_attributes(old_id):
        print("in Copy DM Attributes .. ")

    @staticmethod
    def add_new_dm_attributes(old_id, new_data_collection):
        print("in Add New DM Attributes ...")
