import csvkit

# source-sink-excel
# Put the ability to get outout to Excel
class Bogr:
    # Write the shapefile from

    def __init__(self):
        pass

    def writeshapefile(self):
        """

        :rtype: object
        """
        print("Die!!!")

    @staticmethod
    def abc():
        print("abc!!!")



class ModifiedShapefile:

    @staticmethod
    def writeexcel():
        print("Write to Excel")

    @staticmethod
    def writeshapefile(new_feature_map):
        print("Write to Shapefile!!")
        # Iterate through each row


class ECRecommendation:

    csv_filename = "./source/Sarawak/Sarawak.csv"

    def __init__(self, filename=None):
        if filename is not None:
            ECRecommendation.csv_filename = filename

    @staticmethod
    def extractdata():
        print("in Extract Data ...")
        # Open up csv; going through each
        with open(ECRecommendation.csv_filename, 'rb') as csv_file:
            csv_source = csvkit.reader(csv_file)
            for row in csv_source:
                #    print row[0]
                print ', '.join(row)
