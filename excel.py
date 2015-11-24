import pprint

import csvkit

import copy

# source-sink-excel
# Put the ability to get outout to Excel
import fiona
from fiona.crs import from_epsg

from matching import Lookup

class CurrentShapefile:
    template_node = None
    shape_filename = "./source/Sarawak/16_Swk_13_Ori.shp"
    shapefile_source = None
    shapefile_driver = None
    shapefile_schema = None
    lookup = None

    def __init__(self, filename=None):
        if filename is not None:
            self.shape_filename = filename
        self.shapefile_source = fiona.open(self.shape_filename)
        # Print out the scehma for analysis
        pprint.pprint(self.shapefile_source.schema)
        # Below does not seem to be needed; maybe just applied to those in with block
        self.shapefile_driver = self.shapefile_source.driver
        self.shapefile_schema = self.shapefile_source.schema
        # Take out the first node as a template
        self.template_node = self.shapefile_source[0]
        # Reset the geometry properties ... possible??
        self.template_node['geometry']['coordinates'] = []
        # DEBUG: TEMPLATE NODE
        # pprint.pprint(self.template_node)

    def extract_feature_map(self):
        print("in Extract Feature Map ...")
        new_map = {}
        new_feature_map = {}
        for f in self.shapefile_source:
            # TODO: Should we check for the pre-reqs of properties "NAME" and "NAMA_DM"
            # print("Source ID is " + source_id)
            # Below for debug purposes:
            #   min will have NAME, NAMA_DM
            #   likely will have PAR_LAMA, DUN_LAMA .. maybe DM_LAMA, PAR_BARU, DUN_BARU, PENGUNDI
            #   Case Sensitivty matters!!!!
            # pprint.pprint(f['properties']['NAME'])
            par, dun, dm  = f['properties']['NAME'].split('/')
            # Update from bursting even if it already exists??  Will it be problematic??
            f['properties'].update(
                NAMA_LAMA=f['properties']['NAME'],
                PAR_LAMA=par,
                DUN_LAMA=dun,
                DM_LAMA=dm
            )
            # Normalize the DM name
            # TODO: Remove any non alphanum chars ..

            nama_dm = filter(unicode.isalnum, f['properties']['NAMA_DM'].upper())
            source_id = f['id']
            new_map[nama_dm] = source_id
            new_feature_map[source_id] = f

        # Map the old mapping for lookup later own; use the static version ...
        self.lookup = Lookup(new_map, new_feature_map)

        return new_feature_map

    def find_dm_match(self, nama_dm):
        # Normalize the nama_dmused for lookup
        lookup_key = filter(unicode.isalnum, nama_dm.upper())
        # If find a match; pop it out
        return self.lookup.get_old_mapping().pop(lookup_key)

    def size_of_new_map(self):
        return len(self.lookup.get_old_mapping())

    def pprint_new_map(self):
        pprint.pprint(self.lookup.get_old_mapping())

    def pprint_new_feature_map(self):
        pprint.pprint((self.lookup.get_old_raw_data()['0']))

    def update_new_feature_map(self, matching_key, row):
        # Probably to be used later .. now just atatch it to a map
        full_code, par_code, par_name, dun_code, dun_name, dm_code, dm_name, population = row
        self.lookup.get_old_raw_data()[matching_key]['properties'].update(
            NAME=full_code,
            PAR_BARU=par_code,
            DUN_BARU=dun_code,
            DM_BARU=dm_code,
            PENGUNDI=population
        )

    def get_new_feature_map(self):
        return self.lookup.get_old_raw_data()

    def add_row_to_new_feature_map(self, row):
        # Add the new row with the information on hand
        new_row = copy.deepcopy(self.template_node)
        # Get out the data again .. abetter way to do it??
        full_code, par_code, par_name, dun_code, dun_name, dm_code, dm_name, population = row
        # Modifiy the node; fill in the unknowns ..
        new_row['properties'].update(
            NAME=full_code,
            NAMA_DM=dm_name.upper(),
            PAR_BARU=par_code,
            DUN_BARU=dun_code,
            DM_BARU=dm_code,
            PENGUNDI=population,
            PAR_LAMA='N/A',
            DUN_LAMA='N/A',
            DM_LAMA='N/A',
            NAMA_LAMA='N/A'
        )
        # Append the row to existing ...
        self.lookup.get_old_raw_data()[dm_name]=new_row
        # DEBUG: Was not updating ['properties'] above originally!!
        # pprint.pprint(self.lookup.get_old_raw_data()[dm_name])
        # pprint.pprint(self.lookup.get_old_raw_data()['0'])

class ModifiedShapefile:

    shape_filename = './results/new-Sarawak.shp'
    sink_driver = None
    sink_schema = None

    def __init__(self, sink_driver, sink_schema, filename=None):
        # Modify the schema to your liking; assuming it is default like
        #   Perak; which not be the case
        # TODO: Put in the checks to make sure it complies if it does not already exist .
        sink_schema['properties']['NAMA_LAMA'] = 'str:80'
        sink_schema['properties'][u'PAR_LAMA'] = 'str:80'
        sink_schema['properties'][u'DUN_LAMA'] = 'str:15'
        sink_schema['properties'][u'DM_LAMA'] = 'str:40'
        # DEBUG: Modified schema
        # pprint.pprint(sink_schema)
        ModifiedShapefile.sink_driver = sink_driver
        ModifiedShapefile.sink_schema = sink_schema
        if filename is not None:
            ModifiedShapefile.shape_filename = filename

    @staticmethod
    def writeexcel():
        print("Write to Excel")

    @staticmethod
    def writeshapefile(new_feature_map):
        print("Write to Shapefile!!")
        # Iterate through each row
        # Somehow the EPSG:3857 is not set; dunno why ..
        with fiona.open(
            ModifiedShapefile.shape_filename, 'w',
            crs=from_epsg(3857),
            driver=ModifiedShapefile.sink_driver,
            schema=ModifiedShapefile.sink_schema,
            ) as sink:
            for norm_dm in new_feature_map:
                # pprint.pprint(new_feature_map[norm_dm])
                sink.write(new_feature_map[norm_dm])


class ECRecommendation:

    ec_feature_map = []
    csv_filename = "./source/Sarawak/Sarawak.csv"

    def __init__(self, filename=None):
        if filename is not None:
            self.csv_filename = filename

    def extractdata(self):
        print("in Extract Data ...")
        # Open up csv; going through each
        csv_filename = self.csv_filename

        with open(csv_filename, 'rb') as csv_file:
            csv_source = csvkit.reader(csv_file)
            for n in csv_source:
                self.ec_feature_map.append(n)

    def find_dm_match(self, current_shape_file):
        print("in Find DM Match of ED Data ...")
        hit_first_line = False
        # Example row (should skip result in first line)
        # FULL_CODE, PAR_CODE, PAR_NAME, DUN_CODE, DUN_NAME, DM_CODE, DM_NAME, POPULATION
        # 192/01/01, 192, MAS GADING, 01, Opar, 01, Sebiris, 348
        print("Size of ECMAP at start is %d " % current_shape_file.size_of_new_map())
        for row in self.ec_feature_map:
            # Init ...
            matching_key = ""
            #    print row[0]
            # print ', '.join(row)
            full_code, par_code, par_name, dun_code, dun_name, dm_code, dm_name, population = row
            if not hit_first_line:
                hit_first_line = True
            else:
                # DEBUG: Skip out after first data component
                print("DM is " + dm_name)

                try:
                    matching_key = current_shape_file.find_dm_match(dm_name)
                    # DEBUG:
                    # pprint.pprint(matching_key)
                    # Lookup and modify .. this first entry
                    current_shape_file.update_new_feature_map(matching_key, row)
                    # break
                except KeyError as e:
                    print("MSG: " + e.message)
                    current_shape_file.add_row_to_new_feature_map(row)

        print("Size of ECMAP at end is %d " % current_shape_file.size_of_new_map())
        current_shape_file.pprint_new_map()

