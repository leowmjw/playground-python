from excel import *

print("Sinar Project OGR Attribute Tables Processing!!!")

# Sarawak
# s = fiona.open("./source/Sarawak/16_Swk_13_Ori.shp")
# Kedah test ..
# s = fiona.open("./source/Kedah/02_Kdh_13_Ori.shp")

# Simplest .. Perak
# s = fiona.open("./source/Perak/P070_N42_Kampar_ML.shp")

# Actual item starts below ...

# Analyze the structure of the sample output .. use that??
# wip = fiona.open('/Users/leow/Desktop/TINDAK_MALAYSIA/temp/srw-shape-wip.dbf')
# pprint.pprint(wip.schema)
# wip2 = fiona.open('/Users/leow/Desktop/TINDAK_MALAYSIA/temp/srw-shape.dbf')
# pprint.pprint(wip2.schema)
# exit()

# a = CurrentShapefile()
# Need to extract thigns out
# a.extract_feature_map()
# To duble check the mappings have been made
# a.pprint_new_map()

# after we extract out the csv??
# ec = ECRecommendation()
# ec.extractdata()
# ec.find_dm_match(a)
# a.pprint_new_feature_map()

# Write out the new output ..
# msf = ModifiedShapefile(a.shapefile_source.driver, a.shapefile_schema)
# Uncomment below to write to actual shapefile ..
# msf.writeshapefile(a.get_new_feature_map())
# For review case of Sarawak to check specific case
# my_new_feature_map = a.get_new_feature_map()
# for k in my_new_feature_map:
#    if my_new_feature_map[k]['properties']['NAMA_DM'] == "BANGKIT":
#        print(">>>>>>>>>>>>>>>>>>>>>>>>>")
#        pprint.pprint(my_new_feature_map[k]['properties'])

# print("<<<<<<<<<<    >>>>>>>>>")
# csf = CurrentShapefile("./source/Kedah/02_Kdh_13_Ori.shp")
# fmap = csf.extract_feature_map()


# State: Perak
# Important PATHS
# CurrentShapefile: ./source/Perak/06_Prk_13_Ori.shp
# ECRecommendation: ./source/Perak/Perak.csv
# ModifiedShapefile: ./results/Perak/06-Perak-New-DM


a = CurrentShapefile('./source/N9/11_Nsn_13_Ori.shp')
# Need to extract thigns out
a.extract_feature_map()
# To double check the mappings have been made
# a.pprint_new_map()

# after we extract out the csv??
ec = ECRecommendation('./source/N9/N9.csv')
ec.extractdata()
ec.find_dm_match(a)
# a.pprint_new_feature_map()

# Write out the new output ..
msf = ModifiedShapefile(a.shapefile_source.driver, a.shapefile_schema, './results/N9/11-N9-New-DM')
# Uncomment below to write to actual shapefile ..
msf.writeshapefile(a.get_new_feature_map())

