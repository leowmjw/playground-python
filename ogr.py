from excel import *

print("Sinar Project OGR Attribute Tables Processing!!!")

# Sarawak
# s = fiona.open("./source/Sarawak/16_Swk_13_Ori.shp")
# Kedah test ..
# s = fiona.open("./source/Kedah/02_Kdh_13_Ori.shp")

# Simplest .. Perak
# s = fiona.open("./source/Perak/P070_N42_Kampar_ML.shp")

# Actual item starts below ...

a = CurrentShapefile()
# Need to extract thigns out
a.extract_feature_map()
# To duble check the mappings have been made
# a.pprint_new_map()

# after we extract out the csv??
ec = ECRecommendation()
ec.extractdata()
ec.find_dm_match(a)
# a.pprint_new_feature_map()

# Write out the new output ..
msf = ModifiedShapefile(a.shapefile_source.driver, a.shapefile_source.schema)
msf.writeshapefile(a.get_new_feature_map())

# print("<<<<<<<<<<    >>>>>>>>>")
# csf = CurrentShapefile("./source/Kedah/02_Kdh_13_Ori.shp")
# fmap = csf.extract_feature_map()