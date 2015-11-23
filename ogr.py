from excel import *
from matching import Lookup

print("Sinar Project OGR Attribute Tables Processing!!!")

# Sarawak
# s = fiona.open("./source/Sarawak/16_Swk_13_Ori.shp")
# Kedah test ..
# s = fiona.open("./source/Kedah/02_Kdh_13_Ori.shp")

# Simplest .. Perak
# s = fiona.open("./source/Perak/P070_N42_Kampar_ML.shp")

# Structure as per below:
# id:
# type:
# geometry: << The Polygon details are here; and needs to be moved >>
# properties: << Attributes are here >>
# Lookup.old_raw_data = s

# Print out everything ..
# pprint.pprint(list(Lookup.old_raw_data[1:2]))

# Schema
# pprint.pprint(s.schema)

# Load up the look up index
# First item
# one_item = Lookup.old_raw_data[0:1]

# pprint.pprint(one_item[0]['properties'])

# First, build up the mapping
with fiona.open("./source/Perak/P070_N42_Kampar_ML.shp") as source:
    #
    # Iterate through the data and get the needed properties
    # Maps NORMALIZED DM NAME to OLD_ID
    new_map = {}
    # Maps OLD_ID to FEATURE_DATA
    new_feature_map = {}
    # Take out the first node as a template
    template_node = source[0]
    # Reset the geometry properties ... possible??
    template_node['geometry']['coordinates'] = []
    print("TEMPLATE NODE:")
    pprint.pprint(template_node)
    for f in source:
        source_id = f['id']
        print("Source ID is " + source_id)
        pprint.pprint(f['properties']['Name'].split('/'))
        par, dun, dm  = f['properties']['Name'].split('/')
        # print("PAR is " + bob)
        new_map["DM" + dm] = source_id
        # TODO: If has key; should put in a tie breaker or notify!!

        # new_map[source_id].update(
        #    DM_NAME= "DM" + dm
        # )
        # new_map[source_id]['DM_NAME'] = "DM" + dm
        new_row = f.copy()
        new_row['properties'].update(
            NAMA_LAMA=f['properties']['Name'],
            PAR_LAMA=par,
            DUN_LAMA=dun,
            DM_LAMA=dm
        )
        # Put original here after some optional modification
        new_feature_map["DM" + dm] = new_row
        # pprint.pprint(new_row)

    # Will need to determine the last id; so can start using the next ID
    # when adding items not found/matched
    # When lookup found; copy over the details; else add new node?? what happens without geometry?
    new_feature_map["DMBOB"] = template_node.copy()
    new_feature_map["DMBOB"]['properties'].update(
        NAMA_LAMA='a/b/c',
        PAR_LAMA='123',
        DUN_LAMA='10',
        DM_LAMA='02'
    )
    # new_feature_map["DMBOB"] = {
    #    'id': 'BOB',
    #    'type': 'Feature',
    #    'properties': [
    #        ("Name", 'BOB'),
    #        ("PAR_LAMA", ''),
    #        ("DUN_LAMA", ''),
    #        ("DM_LAMA", '')
    #    ]
    # }
    # pprint.pprint(new_map)
    pprint.pprint(new_feature_map['DMBOB'])
    msf = ModifiedShapefile(source.driver, source.schema, './results/new-Kampar.shp')
    msf.writeshapefile(new_feature_map)

    # Attach to top level Lookup for next round processing
    Lookup.old_mapping = new_map
    Lookup.old_raw_data = new_feature_map

    # Loop through looking for people from new_map??
    # ec = ECRecommendation()
    # ec.extractdata()

    # Loop through again the current shapefile that needs to be transformed??

# Normalize for lookup


# Use naive matching

# Leave those no match .. behind at the bottom

# Output

# ModifiedShapefile.writeexcel()

# Output to Excel

# Functions below ...

# Actual item starts below ...

a = CurrentShapefile()
# Need to extract thigns out
a.extract_feature_map()
# To duble check the mappings have been made
a.pprint_new_map()
# We can compare now ..
# print("SIZE of NEWMAP: %d" % a.size_of_new_map())
# a.find_dm_match('astana')
# print("SIZE of NEWMAP: %d" %  a.size_of_new_map())

# after we extract out the csv??
ec = ECRecommendation()
ec.extractdata()
ec.find_dm_match(a)
# a.pprint_new_feature_map()

# print("<<<<<<<<<<    >>>>>>>>>")
# csf = CurrentShapefile("./source/Kedah/02_Kdh_13_Ori.shp")
# fmap = csf.extract_feature_map()