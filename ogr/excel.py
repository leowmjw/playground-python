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
    # wip_template_node = None
    shape_filename = "./source/Sarawak/16_Swk_13_Ori.shp"
    base_filename = "./template/srw-shape.shp"
    # wip_filename = "./template/srw-shape-wip.dbf"
    shapefile_source = None
    shapefile_driver = None
    shapefile_schema = None
    # wip_schema = None
    lookup = None

    def __init__(self, filename=None):
        if filename is not None:
            self.shape_filename = filename
        self.shapefile_source = fiona.open(self.shape_filename)
        base = fiona.open(self.base_filename)
        # wip = fiona.open(self.wip_filename)
        # Print out the scehma for analysis
        pprint.pprint(base.schema)
        # Below does not seem to be needed; maybe just applied to those in with block
        self.shapefile_driver = self.shapefile_source.driver
        self.shapefile_schema = base.schema
        # self.wip_schema = wip.schema
        # Take out the first node as a template
        CurrentShapefile.template_node = base[0]
        # Reset the geometry properties ... possible??
        CurrentShapefile.template_node['geometry']['coordinates'] = []
        # DEBUG: TEMPLATE NODE
        # pprint.pprint(self.template_node)
        # Close unneeded
        base.close()
        # wip.close()

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
            par, dun, dm = f['properties']['NAME'].split('/')
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
            if not new_map.has_key(nama_dm):
                # New key; or maybe the only one; create a new list
                new_map[nama_dm] = []
            # Append to new or existing list
            new_map[nama_dm].append(source_id)
            new_feature_map[source_id] = f

        # Map the old mapping for lookup later own; use the static version ...
        self.lookup = Lookup(new_map, new_feature_map)

        return new_feature_map

    def find_dm_match(self, nama_dm):
        # Normalize the nama_dmused for lookup
        lookup_key = filter(unicode.isalnum, nama_dm.upper())
        # If find a match; pop it out
        # DEBUG: Can have Empty Index ..
        # pprint.pprint(self.lookup.get_old_mapping()[lookup_key])
        # Pop out the oldest item; so that FIFO takes place
        return self.lookup.get_old_mapping()[lookup_key].pop(0)

    def size_of_new_map(self):
        return len(self.lookup.get_old_mapping())

    def pprint_new_map(self):
        pprint.pprint(self.lookup.get_old_mapping())

    def prune_new_map(self):
        # Prune off the new map and
        # see for inspiration:
        #   http://stackoverflow.com/questions/12118695/efficient-way-to-remove-keys-with-empty-values-from-a-dict
        unmatched_new_map = dict((k, v) for k, v in self.lookup.get_old_mapping().iteritems() if v)
        print("Size of ECMAP at end is %d " % len(unmatched_new_map))
        pprint.pprint(unmatched_new_map)

    def pprint_new_feature_map(self):
        pprint.pprint((self.lookup.get_old_raw_data()['0']))

    def update_new_feature_map(self, matching_key, row):
        # Probably to be used later .. now just atatch it to a map
        full_code, par_code, par_name, dun_code, dun_name, dm_code, dm_name, population = row
        # DEBUG: Curious case of SEBEMBAN; should not replace NAMA_DM for update
        # if matching_key == "SEBEMBAN":
        #    pprint.pprint(row)
        #    exit()
        self.lookup.get_old_raw_data()[matching_key]['properties'].update(
            NAME=full_code if full_code else '',
            PAR_BARU=par_code if par_code else '',
            NAMA_PAR=par_name if par_name else '',
            DUN_BARU=dun_code if dun_code else '',
            NAMA_DUN=dun_name if dun_name else '',
            DM_BARU=dm_code if dm_code else '',
            PENGUNDI_BARU=population if population else ''
        )

    def get_new_feature_map(self):
        return self.lookup.get_old_raw_data()

    def add_row_to_new_feature_map(self, row):
        # Add the new row with the information on hand
        new_row = copy.deepcopy(CurrentShapefile.template_node)
        # Get out the data again .. abetter way to do it??
        full_code, par_code, par_name, dun_code, dun_name, dm_code, dm_name, population = row

        # Modify the node; fill in the unknowns ..
        new_row['properties'].update(
            NAME=full_code,
            NAMA_DM=dm_name.upper(),
            PAR_BARU=par_code if par_code else '',
            NAMA_PAR=par_name if par_name else '',
            DUN_BARU=dun_code if dun_code else '',
            NAMA_DUN=dun_name if dun_name else '',
            DM_BARU=dm_code if dm_code else '',
            PENGUNDI_BARU=population if population else '',
            PAR_LAMA='',
            DUN_LAMA='',
            DM_LAMA='',
            PENGUNDI='',
            NAMA_LAMA=''
        )
        # DEBUG the strange case of SEBEMBAN
        # if dm_name.upper() == "SEBEMBAN":
        #    pprint.pprint(new_row)
        #    exit()
        # Append the row to existing ...
        self.lookup.get_old_raw_data()[dm_name] = new_row
        # DEBUG: Was not updating ['properties'] above originally!!
        # pprint.pprint(self.lookup.get_old_raw_data()[dm_name])
        # pprint.pprint(self.lookup.get_old_raw_data()['0'])


class ModifiedShapefile:
    # The new filename should be derived from a naming nomenclature; from another ticket
    # for now; hard code it ..
    shape_filename = './results/16-Swk-New-DM'
    sink_driver = None
    sink_schema = None

    def __init__(self, sink_driver, sink_schema, filename=None):
        # Modify the schema to your liking; assuming it is default like
        #   Perak; which not be the case
        # TODO: Put in the checks to make sure it complies if it does not already exist .
        sink_schema['properties']['NAMA_LAMA'] = 'str:80'
        # sink_schema['properties'][u'PAR_LAMA'] = 'str:80'
        # sink_schema['properties'][u'DUN_LAMA'] = 'str:15'
        # sink_schema['properties'][u'DM_LAMA'] = 'str:40'
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
    def _keyify(x):
        # Needs to have custom compare function; so that we get the correct order as per Int
        # http://stackoverflow.com/questions/2548000/sorting-a-dictionary-having-keys-as-string-of-numbers-in-python

        try:
            xi = int(x)
        except ValueError:
            return 'S{0}'.format(x)
        else:
            return 'I{0:0{1}}'.format(xi, 10)

    @staticmethod
    def writeshapefile(new_feature_map):
        print("Write to Shapefile!!")
        # Iterate through each row
        # Somehow the EPSG:3857 is not set; dunno why ..
        # Below is for review
        sink = fiona.open(
            ModifiedShapefile.shape_filename + '.shp', 'w',
            crs=from_epsg(3857),
            driver=ModifiedShapefile.sink_driver,
            schema=ModifiedShapefile.sink_schema,
        )
        # Below is for WIP
        sink_wip = fiona.open(
            ModifiedShapefile.shape_filename + '-wip.shp', 'w',
            crs=from_epsg(3857),
            driver=ModifiedShapefile.sink_driver,
            schema=ModifiedShapefile.sink_schema,
        )

        final_wip_map = {}
        for norm_dm in sorted(new_feature_map, key=ModifiedShapefile._keyify):
            # pprint.pprint(new_feature_map[norm_dm])
            # If the PAR_LAMA is N/A; empty the field and write to WIP
            # To cleanse it as well?  before writing it down to the shapefile ..
            # DEBUG the strange case of SEBEMBAN; teh leftover node
            # if norm_dm == '273':
            #    pprint.pprint(new_feature_map[norm_dm]['properties'])
            #    exit()
            # DEBUG: If PAR_LAMA is empty; only write to WIP
            # print("PAR_LAMA ==> " + new_feature_map[norm_dm]['properties']['PAR_LAMA'])
            new_feature_map[norm_dm]['properties'] = ModifiedShapefile.create_new_properties_node(
                new_feature_map[norm_dm]['properties'])
            # pprint.pprint(new_feature_map[norm_dm]['properties'])
            try:
                # TODO: ALWAYS write to WIP if Review Flag set
                # TODO: Create a new Sink (Review)
                #    sink_review.write(new_feature_map[norm_dm])
                #   else is below normal flow ..
                # For Base Shapefile leave out the new nodes
                if new_feature_map[norm_dm]['properties']['PAR_LAMA']:
                    # DEBUGGING for Windows segfault on Unicode
                    # if int(new_feature_map[norm_dm]['id']) == 815:
                    #    pprint.pprint(new_feature_map[norm_dm]['properties'])
                    sink.write(new_feature_map[norm_dm])
                else:
                    # Fill up final_wip_map for use in issue #10; keyed to NAME
                    full_dm_name = new_feature_map[norm_dm]['properties']['NAME']
                    # Cleanse node as per required in issue #6
                    new_feature_map[norm_dm]['properties'].update(
                        NAME='',
                        NAMA_DM=''
                    )
                    # Do a shallow copy as it is not used anywhere else later
                    final_wip_map[full_dm_name] = new_feature_map[norm_dm]

            except ValueError as ve:
                print(">>>>>>>>>" + ve.message)
                pprint.pprint(new_feature_map[norm_dm]['properties'])
                # exit()

        # Loop through the collected WIP nodes ..
        # DEBUG: To see FINAL WIP MAP
        # print("*** FINAL WIP MAP ************")
        # pprint.pprint(final_wip_map)
        for full_dm_name_key in sorted(final_wip_map, key=ModifiedShapefile._keyify):
            print("NEW/Unmatched DM! ==> " + full_dm_name_key)
            sink_wip.write(final_wip_map[full_dm_name_key])

        sink.close()
        sink_wip.close()

    @staticmethod
    def create_new_properties_node(old_properties_node):
        new_properties_node = copy.deepcopy(CurrentShapefile.template_node['properties'])
        try:
            new_properties_node.update(
                NAME=old_properties_node['NAME'] if old_properties_node.has_key('NAME') else '',
                NAMA_DM=old_properties_node['NAMA_DM'] if old_properties_node.has_key('NAMA_DM') else '',
                PAR_LAMA=old_properties_node['PAR_LAMA'] if old_properties_node.has_key('PAR_LAMA') else '',
                DUN_LAMA=old_properties_node['DUN_LAMA'] if old_properties_node.has_key('DUN_LAMA') else '',
                DM_LAMA=old_properties_node['DM_LAMA'] if old_properties_node.has_key('DM_LAMA') else '',
                PENGUNDI=old_properties_node['PENGUNDI'] if old_properties_node.has_key('PENGUNDI') else '',
                KODPAR=old_properties_node['PAR_BARU'] if old_properties_node.has_key('PAR_BARU') else '',
                PAR_BARU=old_properties_node['NAMA_PAR'] if old_properties_node.has_key('NAMA_PAR') else '',
                KODDUN=old_properties_node['DUN_BARU'] if old_properties_node.has_key('DUN_BARU') else '',
                DUN_BARU=old_properties_node['NAMA_DUN'] if old_properties_node.has_key('NAMA_DUN') else '',
                KODDM=old_properties_node['DM_BARU'] if old_properties_node.has_key('DM_BARU') else '',
                DM_BARU=old_properties_node['NAMA_DM'] if old_properties_node.has_key('NAMA_DM') else '',
                PENGUNDI_B=old_properties_node['PENGUNDI_BARU'] if old_properties_node.has_key('PENGUNDI_BARU') else '',
                NAMA_LAMA=old_properties_node['NAMA_LAMA'] if old_properties_node['NAMA_LAMA'] else ''
            )
        except KeyError as e:
            print("**** ERROR ****: " + e.message)
            pprint.pprint(old_properties_node)
            # new_properties_node[e]='N/A'

        return new_properties_node


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
                # print("DM is " + dm_name)

                try:
                    matching_key = current_shape_file.find_dm_match(dm_name)
                    # DEBUG:
                    # pprint.pprint(matching_key)
                    # Lookup and modify .. this first entry
                    current_shape_file.update_new_feature_map(matching_key, row)
                    # break
                except KeyError as e:
                    print("DM is " + dm_name + ": " + full_code)
                    print("KeyError: " + e.message)
                    current_shape_file.add_row_to_new_feature_map(row)
                except IndexError as e:
                    print("DM is " + dm_name + ": " + full_code)
                    print("IndexError: " + e.message)
                    current_shape_file.add_row_to_new_feature_map(row)


        # Clean up an all matched DMs in the new_map
        current_shape_file.prune_new_map()
