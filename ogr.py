import pprint

import csvkit
import fiona

from excel import *
from matching import Lookup

print("Sinar Project OGR Attribute Tables Processing!!!")

# Sarawak
# s = fiona.open("./source/Sarawak/16_Swk_13_Ori.shp")
# Kedah test ..
# s = fiona.open("./source/Kedah/02_Kdh_13_Ori.shp")

# Simplest .. Perak
s = fiona.open("./source/Perak/P070_N42_Kampar_ML.shp")

# Structure as per below:
# id:
# type:
# geometry: << The Polygon details are here; and needs to be moved >>
# properties: << Attributes are here >>
Lookup.old_raw_data = s

# Print out everything ..
# pprint.pprint(list(Lookup.old_raw_data[1:2]))

# Schema
pprint.pprint(s.schema)

# Load up the look up index
# First item
one_item = Lookup.old_raw_data[0:1]

pprint.pprint(one_item[0]['properties'])

# First, build up the mapping
with fiona.open("./source/Perak/P070_N42_Kampar_ML.shp") as source:
    # Copy over the schema and add own
    sink_schema = source.schema.copy()
    sink_schema['properties'][u'PAR_LAMA'] = 'str:80'
    sink_schema['properties'][u'DUN_LAMA'] = 'str:15'
    sink_schema['properties'][u'DM_LAMA'] = 'str:40'
    pprint.pprint(sink_schema)
    #
    # Iterate through the data and get the needed properties
    new_map = {}
    for f in source:
        source_id =  f['id']
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
        # pprint.pprint(new_row)
    pprint.pprint(new_map)


# Open up csv; going through each
with open("./source/Sarawak/Sarawak.csv", 'rb') as csv_file:
    csv_source = csvkit.reader(csv_file)
    # for row in csv_source:
    #    print row[0]
    #    print ', '.join(row)

# Normalize for lookup


# Use naive matching

# Leave those no match .. behind at the bottom

# Output

# b = Bogr()
# b.writeshapefile()

# Bogr.abc()

ModifiedShapefile.writeexcel()
ModifiedShapefile.writeshapefile()

# Output to Excel

# Functions below ...