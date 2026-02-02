######################################################################################################################
# Restructures and recodes source datasets into fields and attribute values that match NEPOS.
# The script works by source with a function for each source and nested functions within for different steps/fields.
# This script also contains a function to create singlepart features, which is necessary before spatial matching.
#
# The input to each dataset function is a source dataset projected into NEPOS CRS
# The output of each dataset function is a version of the source dataset restructured and recoded to match
# NEPOS structure and values.
#
# NOTE: Prior to running this script, it is expected that raw source data were copied into the workspace GDB.
# For most sources, this is a basic copy/paste from one GDB to another, or importing a shapefile to the GDB.
# However for multistate datasets, it is expected that the NE data has been extracted specifically.
# Also, for PADUS, you have to combine the fee and easement layers into one. Previously I did this all manually
# but maybe should write a script for it??
#
# Planning note: The names recoding is very unwieldy and repetitive... should develop a global function for each
# state source, then call those within aggregate datasets like TNC, NCED, and PADUS. The name functions for those
# datasets can include more code, but call the state functions first to handle a bunch of stuff, then see what's left.
# State functions will need to to include a field parameter because the fields in agg datasets will be different.
# Or perhaps generalize even further, and create lists or dictionaries of all the ways a certain owner is spelled,
# and then apply the correct name is a name is found in one of those lists?
#
#
# Lucy Lee, 12/2025
######################################################################################################################
import time
start_time = time.time()
import arcpy
import traceback
import pandas as pd
import os
import sys

# Geodatabase containing all source datasets
# Projected and preprocessed multipart source layers will also be sent here
# UPDATE THIS PATH FOR NEXT UPDATE
arcpy.env.workspace = 'D:/Lee/POS/Update_2023/Data/new_data_sources.gdb/'
arcpy.env.overwriteOutput = True

# Function to print elapsed time running script
def print_elapsed_time():
    end_time = time.time()
    duration_in_seconds = round(end_time - start_time, 0)
    duration_in_minutes = round((end_time - start_time) / 60.0, 2)
    if duration_in_seconds < 60.0:
        print('Time elapsed: {} seconds'.format(duration_in_seconds))
    elif duration_in_minutes > 60.0:
        duration_in_hours = round(duration_in_minutes / 60.0, 2)
        print('Time elapsed: {} hours'.format(duration_in_hours))
    else:
        print('Time elapsed: {} minutes'.format(duration_in_minutes))

# Function to project raw source data into NEPOS CRS
# Suggest projecting first so that you do any further preprocessing
# on the projected data, and retain a copy of the raw source data
# in the workspace GDB
def project_data(data):
    # NEPOS CRS
    crs = arcpy.SpatialReference(102039)
    
    # Project data and append _albers to filename
    result = f"{data}_albers"
    arcpy.management.Project(data, result, crs)
    print(f"Projected {data} to NEPOS CRS...")

    return(data)   # Return the file name for further preprocessing

# Prep MassGIS data
def prep_massgis(data):
    ########## Define functions ###########
    # Add / alter fields -- this function should be called first since recoding functions
    # rely on new field names
    def add_alter_fields():
        arcpy.management.AddField(data, 'State', 'TEXT', field_length=2)
        arcpy.management.CalculateField(data, 'State', "'MA'")     # No idea why double quotes is needed here, but it throws an error if not
        arcpy.management.AlterField(data, 'SITE_NAME', 'AreaName', 'AreaName')
        arcpy.management.AddField(data, 'FeeOwner', 'TEXT', field_length=150)   # We need longer length than the FEE_OWNER field has
        arcpy.management.CalculateField(data, 'FeeOwner', '!FEE_OWNER!')
        arcpy.management.AddField(data, 'FeeOwnType', 'TEXT', field_length=3)
        arcpy.management.AddField(data, 'FeeOwnCat', 'TEXT', field_length=7)
        arcpy.management.AddField(data, 'FeeOwnCatComments', 'TEXT', field_length=200)
        arcpy.management.AddField(data, 'IntHolder1', 'TEXT', field_length=150)
        arcpy.management.AddField(data, 'IntHolder1Type', 'TEXT', field_length=3)
        arcpy.management.AddField(data, 'IntHolder2', 'TEXT', field_length=150)
        arcpy.management.AddField(data, 'IntHolder2Type', 'TEXT', field_length=3)
        arcpy.management.AddField(data, 'ProtType', 'TEXT', field_length=15)
        arcpy.management.AddField(data, 'ProtTypeComments', 'TEXT', field_length=200)
        arcpy.management.AddField(data, 'YearProt', 'SHORT')
        arcpy.management.AddField(data, 'YearProtComments', 'TEXT', field_length=200)
        arcpy.management.AddField(data, 'FeeYear', "SHORT")
        arcpy.management.AddField(data, "EaseYear", "SHORT")
        arcpy.management.AddField(data, 'GapStatus', 'SHORT')
        arcpy.management.AddField(data, 'PubAccess', 'TEXT', field_length=7)
        arcpy.management.AlterField(data, 'COMMENTS', 'Comments', 'Comments')
        arcpy.management.AddField(data, 'ProtDuration', 'TEXT', field_length=4)
        arcpy.management.AlterField(data, 'OS_ID', 'UID', 'UID')
        arcpy.management.AddField(data, 'Purpose', 'TEXT', field_length=30)
        print('Added and altered fields, including calculating State as MA')

    def calc_acres():
        arcpy.management.CalculateGeometryAttributes(data, [["acres", "AREA"]], area_unit = "ACRES_US")
        print("Calculated acres...")

    # Recode owner / interest holder names in FeeOwner, IntHolder1, and IntHolder2 fields
    # This code is not exhaustive but includes major entities, particularly state and federal gov
    def recode_org_names(field):
        with arcpy.da.UpdateCursor(data, field) as cur:
            for row in cur:
                # Empty
                if row[0] is None:
                    if field == 'FeeOwner':
                        row[0] = 'Unknown'      # Only populate 'Unknown' for fee owners
                    else:
                        continue                # If null interest holder field, skip and go to next row
                # State names
                elif row[0] == 'DCR - Division of State Parks and Recreation':
                    row[0] = 'MA DCR - Division of State Parks and Recreation'
                elif row[0] == 'Department of Fish and Game':
                    row[0] = 'MA Department of Fish and Game'
                elif row[0] == 'DCR - Division of Water Supply Protection' or row[0] == 'DCR- Division of Water Supply Protection':
                    row[0] = 'MA DCR - Division of Water Supply Protection'
                elif row[0] == 'Department of Capital Asset Management':
                    row[0] = 'MA Department of Capital Asset Management'
                elif row[0] == 'Massachusetts Highway Department':
                    row[0] = 'MA DOT - Highway Division'
                elif row[0] == 'MASSACHUSETTS PORT AUTHORITY' or row[0].upper() == 'MASSPORT':
                    row[0] = 'Massachusetts Port Authority'
                elif row[0] == 'DCR - Division of State Parks and Recreation / Department of Fish and Game':
                    row[0] = 'MA DCR - Division of State Parks and Recreation / MA Department of Fish and Game'
                elif row[0] == 'Department of Agricultural Resources':
                    row[0] = 'MA Department of Agricultural Resources'
                elif row[0] == 'Board of Regional Community Colleges':
                    row[0] = 'MA Board of Regional Community Colleges'
                elif row[0] == 'Department of Developmental Services':
                    row[0] = 'Commonwealth of Massachusetts Department of Development Services'
                elif row[0] == 'Department of Transportation':
                    row[0] = 'MA Department of Transportation'
                elif row[0] == 'Secretary of the Commonwealth':
                    row[0] = 'Secretary of the Commonwealth of Massachusetts'
                elif row[0] == 'Health and Human Services':
                    row[0] = 'MA Executive Office of Health and Human Services'
                elif row[0] == 'Massachusetts Department of Public Works':
                    row[0] = 'MA Department of Public Works'
                elif row[0] == 'Executive Office of Energy and Environmental Affairs':
                    row[0] = 'MA Executive Office of Energy and Environmental Affairs'
                elif row[0] == 'Massachusetts Water Resource Authority':
                    row[0] = 'Massachusetts Water Resources Authority'
                # Federal names
                elif row[0] == 'United States Department of the Interior':
                    row[0] = 'US Department of the Interior'
                elif row[0] == 'Army Corps of Engineers':
                    row[0] = 'US DOD - Army Corps of Engineers'
                elif row[0] == 'National Park Service':
                    row[0] = 'US DOI - National Park Service'
                elif row[0] == 'UNITED STATES DEPARTMENT OF VETERANS ADMINISTRATION':
                    row[0] = 'US Department of Veterans Administration'
                elif row[0] == 'United States Air Force':
                    row[0] = 'US DOD - Air Force'
                elif row[0] == 'United States Coast Guard' or row[0] == 'US Coast Guard':
                    row[0] = 'US DOD - Coast Guard'
                elif row[0] == 'United States Department of Defense':
                    row[0] = 'US Department of Defense'
                elif row[0] == 'NMFS':
                    row[0] = 'NOAA - National Marine Fisheries Service'
                elif row[0] == 'NOAA':
                    row[0] = 'National Oceanic and Atmospheric Administration'
                elif row[0] == 'US Fish and Wildlife Service' or row[0] == 'United States Fish and Wildlife Service':
                    row[0] = 'US DOI - Fish and Wildlife Service'
                elif row[0] == 'Natural Resources Conservation Service':
                    row[0] = 'USDA - Natural Resources Conservation Service'
                elif row[0] == 'United States Department of Agriculture':
                    'US Department of Agriculture'
                elif row[0] == 'United States Forest Service':
                    row[0] = 'USDA - Forest Service'
                # Municipal names
                elif row[0] == 'BRISTOL COUNTY AGRICULTURAL HIGH SCHOOL' or row[0] == 'BRISTOL COUNTY AGRICULTURAL SCHOOL':
                    row[0] = 'Bristol County Agricultural High School'
                elif row[0] == 'Nantucket Island Land Bank':
                    row[0] = 'Nantucket Islands Land Bank'
                elif row[0] == 'SALEM AND BEVERLY WATER SUPPLY BOARD':
                    row[0] = 'Salem-Beverly Water Supply Board'
                # Private names
                elif row[0] == 'A D Makepeace Company' or row[0] == 'A. D. Makepeace Company' or row[0] == 'MAKEPEACE CO':
                    row[0] = 'AD Makepeace Company'
                elif row[0] == 'Ames Rife and Pistol Club' or row[0].upper() == 'AMES RIFLE AND PISTOL CLUB':
                    row[0] = 'Ames Rifle and Pistol Club'
                elif row[0] == 'ASSOCIATION OF JEWISH PHILANTHROPIES INC' or row[0] == 'Association of Jewish Philanthropies Inc':
                    row[0] = 'Association of Jewish Philanthropies'
                elif row[0].upper() == 'AQUARION WATER COMPANTY INC' or row[0].upper() == 'AQUARION WATER COMPANY' or row[0].upper() == 'AQUARION WATER COMPANY INC':
                    row[0] = 'Aquarion Water Company'
                elif row[0] == 'B AND N LANDS LLC' or row[0] == 'B and N Lands LLC':
                    row[0] = 'B&N Lands LLC'
                elif row[0] == 'Barrington Land Conservancy Trust' or row[0] == 'Barrington RI Land Conservation Trust':
                    row[0] = 'Barrington Land Conservation Trust'
                elif row[0] == 'BAY CLUB COMMUNITY ASSOCIATION INC' or row[0] == 'Bay Club Community Association Inc.':
                    row[0] = 'Bay Club Community Association'
                elif row[0] == 'Blackston Valley Boys and Girls Club, Inc.' or row[0] == 'Blackstone Valley Boys and Girls Club Inc':
                    row[0] = 'Blackstone Valley Boys and Girls Club'
                elif row[0] == 'Boston & Maine Railroad' or row[0] == 'BOSTON AND MAINE RAILROAD':
                    row[0] = 'Boston and Maine Railroad'
                elif row[0] == 'BOSTON SYMPHONY ORCHESTRA':
                    row[0] = 'Boston Symphony Orchestra'
                elif row[0] == 'Boxford Trails Association/Boxford Open Lands Trust':
                    row[0] = 'Boxford Trails Association/Boxford Open Land Trust'
                elif row[0] == 'BOY AND GIRL SCOUTS OF KINGSTON INC' or row[0] == 'Boy and Girl Scouts of Kingston Inc':
                    row[0] = 'Boy & Girl Scouts of Kingston'
                elif row[0] == 'BRAINTREE LIMITED PARTNERSHIP INC' or row[0] == 'Braintree Limited Partnership Inc.':
                    row[0] = 'Braintree Limited Partnership'
                elif row[0] == 'Cape Cod Museum of Natural History Inc':
                    row[0] = 'Cape Cod Museum of Natural History'
                elif row[0] == 'CHARLES RIVER COUNTRY CLUB' or row[0] == 'CHARLES RIVER COUNTRY CLUB INC':
                    row[0] = 'Charles River Country Club'
                elif row[0] == 'CHEQUESSET YACHT AND COUNTRY CLUB':
                    row[0] = 'Chequesset Yacht and Country Club'
                elif row[0] == 'Combined Jewish Philanthropies of Greater Boston Inc' or row[0] == 'Combined Jewish Philanthropies of Greater Boston *':
                    row[0] = 'Combined Jewish Philanthropies of Greater Boston'
                elif row[0] == 'COMMONS AT HOPKINGTON ASSOCIATION' or row[0] == 'COMMONS AT HOPKINTON ASSOCIATION':
                    row[0] = 'Commons at Hopkinton Association'
                elif row[0] == 'DESIGN HOUSING INC':
                    row[0] = 'Design Housing Inc.'
                elif row[0] == 'Dudley Conservation Land Trust ':
                    row[0] = "Dudley Conservation Land Trust"
                elif row[0] == 'EAST CHOP ASSOCIATION' or row[0] == 'East Chop Association Inc' or row[0] == 'EAST CHOP ASSOCIATION INC':
                    row[0] = 'East Chop Association'
                elif row[0] == 'ELMWOOD CERMTERY ASSOCIATION' or row[0] == 'ELMWOOD CEMETERY ASSOCIATION':
                    row[0] = 'Elmwood Cemetery Association'
                elif row[0] == 'FAIRVIEW FISH AND GAME' or row[0] == 'FAIRVIEW FISH AND GAME CLUB':
                    row[0] = 'Fairview Fish and Game Club'
                elif row[0] == 'Falmouth Rod and Gun Club Inc' or row[0] == 'Falmouth Rod And Gun Club Inc':
                    row[0] = 'Falmouth Rod and Gun Club'
                elif row[0] == 'GIRL SCOUT COUNCIL OF SE MASS':
                    row[0] = 'Girl Scouts of Southeastern Massachusetts'
                elif row[0] == 'GREEN MEADOW ASSOCIATION INC' or row[0] == 'Green Meadow Association Inc.':
                    row[0] = 'Green Meadow Association'
                elif row[0] == 'HALE RERSERVATION INC' or row[0].upper() == 'HALE RESERVATION INC':
                    row[0] = 'Hale Reservation'
                elif row[0] == 'HAMPDEN COUNTRY CLUB' or row[0] == 'HAMPDEN COUNTRY CLUB INC.':
                    row[0] = 'Hampden Country Club'
                elif row[0] == 'HARVARD COLLEGE PRESIDENT & FELLOWS':
                    row[0] = 'President and Fellows of Harvard College'
                elif row[0].upper() == 'HULL FOREST PRODUCTS INC' or row[0] == 'Hull Forest Projects Inc' or row[0] == 'HULL WOOD PRODUCTS':
                    row[0] = 'Hull Forest Products'
                elif row[0] == 'Hull Forestlands' or row[0] == 'HULL FORESTLANDS LP':
                    row[0] = 'Hull Forestlands LP'
                elif row[0] == 'INDIAN MEADOW GOLF INC' or row[0] == 'INDIAN MEADOWS GOLF INC':
                    row[0] = 'Indian Meadows Golf Club'
                elif row[0] == 'Katharine Nordell Lloyd Center for Environmental Studies Inc':
                    row[0] = 'Lloyd Center for the Environment'
                elif row[0] == 'MARSHFIELD ROD AND GUN CLUB' or row[0] == 'MARSHFIELD ROD AND GUN CLUB INC':
                    row[0] = 'Marshfield Rod and Gun Club'
                elif row[0] == 'Mattapoisett  Land Trust':
                    row[0] = 'Mattapoisett Land Trust'
                elif row[0] == 'Nantucket Island Land Bank':
                    row[0] = 'Nantucket Islands Land Bank'
                elif row[0] == 'New England Power' or row[0] == 'NEW ENGLAND POWER COMPANY' or row[0] == 'New England Power Complany' or row[0] == 'New England Power Corporation':
                    row[0] = 'New England Power Company'
                elif row[0] == 'Nimrod League of Holden Inc':
                    row[0] = 'Nimrod League of Holden'
                elif row[0] == 'PROSPECT HILL CEMETERY ASSN':
                    row[0] = 'Prospect Hill Cemetery Association'
                elif row[0] == 'Society For The Preservation Of New England Antiquities':
                    row[0] = 'Society for the Preservation of New England Antiquities'
                elif row[0] == 'SOUTH BARRE R&G INC' or row[0] == 'SOUTH BARRE ROD/GUN INC':
                    row[0] = 'South Barre Rod and Gun Club'
                elif row[0] == 'SOUTH SHORE NATURAL SCIENCE CENTER INC' or row[0] == 'South Shore Natural Science Center Inc':
                    row[0] = 'South Shore Natural Science Center'
                elif row[0] == "St. Jospeh's Abbey":
                    row[0] = "St. Joseph's Abbey"
                elif row[0] == 'Tufts Univeristy':
                    row[0] = 'Tufts University'
                elif row[0] == 'W. D. Cowls Inc.' or row[0] == 'W D Cowls Inc':
                    row[0] = 'W.D. Cowls Inc.'
                elif row[0] == 'WOODS HOLE MARINE BIOLOGICAL LABORATORY':
                    row[0] = 'Woods Hole Marine Biological Laboratory'
                elif row[0] == 'X':
                    row[0] = 'Unknown'
                elif row[0] == 'YMCA':
                    row[0] = 'Young Mens Christian Association'
                cur.updateRow(row)
        del row, cur
        print('Recoded values in {}'.format(field))

    # Function to recode org types -- function takes a list of 2 fields
    # First is the MassGIS field and second is the new empty field to populate w/ recoded values
    # e.g. [OWNER_TYPE, FeeOwnType] or [OLI_1_TYPE, IntHolder1Type] or [OLI_2_TYPE, IntHolder2Type]
    # This code classifies too many owners as PFP -- there should be a general PVT category!
    def recode_org_type(fields):
        with arcpy.da.UpdateCursor(data, fields) as cur:
            for row in cur:
                if row[0] is None:                       # If org type is Null...
                    if fields[1] == 'FeeOwnType':        # And if the fields are about owners (not int holders)...
                        row[1] = 'UNK'                   # Then null means owner is Unknown
                    else:
                        continue                         # If int org type is null, skip and go to next row
                elif row[0] == 'B':
                    row[1] = 'QP'
                elif row[0] == 'C' or row[0] == 'M':
                    row[1] = 'LOC'
                elif row[0] == 'F':
                    row[1] = 'FED'
                elif row[0] == 'G' or row[0] == 'L' or row[0] == 'N':
                    row[1] = 'PNP'
                elif row[0] == 'O':
                    row[1] = 'OTH'
                elif row[0] == 'P':
                    row[1] = 'PFP'
                elif row[0] == 'S':
                    row[1] = 'STP'
                elif row[0] == 'X':
                    row[1] = 'UNK'
                cur.updateRow(row)
        del row, cur
        print('Recoded values from field {} into field {}'.format(fields[0], fields[1]))

    # Correcting org types of some organizations to better match NEPOS
    # Function takes a list of two fields such as [FeeOwner, FeeOwnType]
    # or [IntHolder1, IntHolder1Type]
    def correct_oth_org_types(fields):
        with arcpy.da.UpdateCursor(data, fields) as cur:
            for row in cur:
                if row[1] == 'OTH':
                    if row[0] is None:
                        row[1] = 'UNK'
                    elif 'high school' in row[0].lower() or 'school district' in row[0].lower():
                        row[1] = 'LOC'
                    elif 'water supply board' in row[0].lower():
                        row[1] = 'LOC'
                    elif row[0] == 'Boy Scouts of America':
                        row[1] = 'PVT'
                    elif row[0].lower() == 'unknown':
                        row[1] = 'UNK'
                    elif row[0] == 'Massachusetts Port Authority':
                        row[1] = 'STP'
                    cur.updateRow(row)
                    
    
    def get_joint_fee_owners(): 
        c = 0
        with arcpy.da.UpdateCursor(data, ['FeeOwner', 'OLI_1_INT', 'OLI_1_ORG', 'OLI_2_INT', 'OLI_2_ORG', 'OLI_3_INT', 'OLI_3_ORG',
                                          'FeeOwnType', 'OLI_1_TYPE', 'OLI_2_TYPE', 'OLI_3_TYPE', 'FeeOwnCatComments']) as cur:
            for row in cur:
                fee_owners = [row[0]]   # Initiate list of fee owners beginning with the FeeOwner field
                fee_owner_types = [row[7]]   # Do the same for fee owner type
                if row[1] == 'FEE' and row[2] is not None:
                    fee_owners.append(row[2])
                    fee_owner_types.append(row[8])
                if row[3] == 'FEE' and row[4] is not None:
                    fee_owners.append(row[4])
                    fee_owner_types.append(row[9])
                if row[5] == 'FEE' and row[6] is not None:
                    fee_owners.append(row[6])
                    fee_owner_types.append(row[10])
                unique_fee_owners = list(set(fee_owners))
                # Do some processing of the fee owner types before getting unique values
                # Because while we preprocessed FeeOwnType based on FeeOwner, we did not do this
                # for the OLI_TYPE fields
                fee_owner_types[:] = ['LOC' if x in ['C', 'M'] else x for x in fee_owner_types]
                fee_owner_types[:] = ['PNP' if x in ['G', 'N', 'L'] else x for x in fee_owner_types]
                fee_owner_types[:] = ['QP' if x == 'B' else x for x in fee_owner_types]
                fee_owner_types[:] = ['FED' if x == 'F' else x for x in fee_owner_types]
                fee_owner_types[:] = ['OTH' if x == 'O' else x for x in fee_owner_types]
                fee_owner_types[:] = ['PFP' if x == 'P' else x for x in fee_owner_types]
                fee_owner_types[:] = ['STP' if x == 'S' else x for x in fee_owner_types]
                fee_owner_types[:] = ['UNK' if x == 'X' else x for x in fee_owner_types]
                unique_fee_owner_types = list(set(fee_owner_types))
                if len(unique_fee_owners) > 1:
                    row[0] = ' & '.join(unique_fee_owners)
                    c = c + 1
                if len(unique_fee_owner_types) > 1:
                    row[7] = 'OTH'
                    if len(unique_fee_owner_types) == 2:
                        row[11] = f'Jointly owned by {unique_fee_owner_types[0]} & {unique_fee_owner_types[1]}'
                    elif len(unique_fee_owner_types) == 3:
                        row[11] = f'Jointly owned by {unique_fee_owner_types[0]}, {unique_fee_owner_types[1]} & {unique_fee_owner_types[2]}'
                cur.updateRow(row)
        print(f'Updated FeeOwner for {c} rows with joint owners...')
    
    def assign_fee_own_cat():
        with arcpy.da.UpdateCursor(data, ['FeeOwnType', 'FeeOwnCat']) as cur:
            for row in cur:
                if row[0] in ['LOC', 'STP', 'FED', 'QP']:
                    row[1] = 'Public'
                elif row[0] in ['PNP', 'PFP']:
                    row[1] = 'Private'
                elif row[0] == 'OTH':
                    row[1] = 'Other'
                elif row[0] == 'UNK':
                    row[1] = 'Unknown'
                cur.updateRow(row)
        print('Assigned FeeOwnCat...')

    # Interest holders and protection type will be assigned together
    # Joint fee owners have already been handled so they don't need to be part of this function
    # Note that the original codes from OLI_INT fields are used and then reclassified into NEPOS
    # values after assignment
    # Also note that Fee and Ease is classified in its own UpdateCursor for simplicity
    # IMPORTANT! This code does not handle all possible combinations of interest types
    # It only handles what is present in the data and as such should be reviewed before each use
    # to ensure there are no new combinations present in the data. You can do this by querying the data
    # based on the high level groupings (e.g. OLI_1_INT is not null AND OLI_2_INT is not null and OLI_3_INT is null)
    # and seeing what combinations of data are present in each. For instance as of 2025 there is no DR present
    # when only OLI_1 and OLI_2 are populated. You can also just run the code as is and then look for rows
    # where ProtType = Null which suggests something was missed, then edit the code as needed.
    def assign_prot_type_int_holders():
        easements = ['APR', 'CAPR', 'CR', 'EASE', 'HPR', 'PAR', 'WPR', 'WR']   # All easements except ROW
        with arcpy.da.UpdateCursor(data, ['OLI_1_INT', 'OLI_2_INT', 'OLI_3_INT', 
                                          'FeeOwnType', 'ProtType', 'ProtTypeComments',
                                          'IntHolder1', 'IntHolder1Type', 'IntHolder2', 'IntHolder2Type',
                                          'OLI_1_ORG', 'OLI_1_TYPE', 'OLI_2_ORG', 'OLI_2_TYPE', 'OLI_3_ORG', 'OLI_3_TYPE']) as cur:
            for row in cur:
                # First rows where OLI_1_INT is null
                if row[0] is None:
                    row[4] = 'Fee'        # Fee is not an interest type, we assume any Null interest types are fee
                # Then rows where there is only OLI_1_INT
                elif row[0] is not None and row[1] is None:
                    if row[0] in easements:
                        row[4] = 'Ease'
                        row[5] = f'Easement is {row[0]}'
                        row[6] = row[10]
                        row[7] = row[11]
                    elif row[0] == 'LH':
                        if row[11] == 'P':   # If the lease holder type is P (PFP)
                            row[4] = 'Fee'
                            row[5] = f'{row[10]} holds lease but primary protection is fee'
                        else:
                            row[4] = 'Lease' 
                            row[6] = row[10]
                            row[7] = row[11]
                    elif row[0] == 'DR':
                        row[4] = 'DR'
                        row[6] = row[10]
                        row[7] = row[11]
                    elif row[0] == 'FEE':   # We already assigned joint FeeOwner in previous function
                        row[4] = 'Fee'      # But we still need to assign the ProtType
                    elif row[0] == 'LIC':
                        row[4] = 'Other'
                        row[6] = row[10]
                        row[7] = row[11]
                    elif row[0] == 'ROW':
                        if row[3] in ['PFP', 'PVT', 'UNK']:   # If fee owner is private or unknown
                            row[4] = 'ROW'
                            row[6] = row[10]
                            row[7] = row[11]
                        else:
                            row[4] = 'Fee'
                            row[5] = f'{row[10]} holds ROW but primary protection is fee'
                # Then rows where there are only OLI_1_INT and OLI_2_INT
                elif row[0] is not None and row[1] is not None and row[2] is None:
                    if row[0] in easements and row[1] in easements:
                        row[4] = 'Ease'
                        row[6] = row[10]
                        row[7] = row[11]
                        row[8] = row[12]
                        row[9] = row[13]
                        if row[0] == row[1]:
                            row[5] = f'Jointly held {row[0]}'
                        else:
                            row[5] = f'IntHolder1 holds {row[0]}, IntHolder2 holds {row[1]}'
                    # If ROW is the second interest, we don't count this as its not part of the conservation protection
                    elif row[0] in easements and row[1] == 'ROW':
                        row[4] = 'Ease'
                        row[5] = f'IntHolder1 holds {row[0]}; also subject to ROW held by {row[12]}'
                        row[6] = row[10]
                        row[7] = row[11]
                    elif row[0] in easements and row[1] == 'LH':
                        row[4] = 'Ease'
                        row[5] = f'IntHolder1 holds {row[0]}; {row[12]} holds lease'
                        row[6] = row[10]
                        row[7] = row[11]
                    elif row[0] == 'FEE' and row[1] in easements:
                        row[4] = 'Ease'
                        row[6] = row[12]   # IntHolder1 is OLI_2_ORG
                        row[7] = row[13]
                    elif row[0] == 'FEE' and row[1] == 'FEE':
                        row[4] = 'Fee'
                    elif row[0] == 'LH' and row[1] == 'LH':
                        row[4] = 'Lease'
                        row[6] = row[10]
                        row[7] = row[11]
                        row[8] = row[12]
                        row[9] = row[13]
                    # If ROW is listed first, we still don't count it if there's a proper easement
                    elif row[0] == 'ROW' and row[1] in easements:
                        row[4] = 'Ease'
                        row[5] = f'IntHolder1 holds {row[1]}; also subject to ROW held by {row[10]}'
                        row[6] = row[12]    # IntHolder1 is OLI_2_ORG
                        row[7] = row[13]
                # Then rows where all 3 OLI_INT fields are populated
                elif row[0] is not None and row[1] is not None and row[2] is not None:
                    if row[0] in easements and row[1] in easements and row[2] in easements:
                        row[4] = 'Ease'
                        if row[0] == row[1] == row[2]:
                            row[5] = f'Jointly held {row[0]}, {row[14]} is also an interest holder'
                            row[6] = row[10]
                            row[7] = row[11]
                            row[8] = row[12]
                            row[9] = row[13]
                        elif row[0] == row[1]:
                            row[5] = f'Jointly held {row[0]}; also subject to {row[2]} held by {row[14]}'
                            row[6] = row[10]
                            row[7] = row[11]
                            row[8] = row[12]
                            row[9] = row[13]
                        elif row[0] == row[2]:
                            row[5] = f'Jointly held {row[0]}; also subject to {row[1]} held by {row[12]}'
                            row[6] = row[10]
                            row[7] = row[11]
                            row[8] = row[14]   # IntHolder2 is OLI_3_ORG
                            row[9] = row[15]
                        elif row[1] == row[2]:
                            row[5] = f'IntHolder1 holds {row[0]}, IntHolder2 holds {row[1]} jointly with {row[14]}'
                            row[6] = row[10]
                            row[7] = row[11]
                            row[8] = row[12]
                            row[9] = row[13]
                        elif row[0] != row[1] != row[2]:
                            row[5] = f'IntHolder1 holds {row[0]}, IntHolder2 holds {row[1]}; also subject to {row[2]} held by {row[14]}'
                            row[6] = row[10]
                            row[7] = row[11]
                            row[8] = row[12]
                            row[9] = row[13]
                    elif row[0] in easements and row[1] in easements and row[2] == 'DR':
                        row[4] = 'Ease'
                        row[6] = row[10]
                        row[7] = row[11]
                        row[8] = row[12]
                        row[9] = row[13]
                        if row[0] == row[1]:
                            row[5] = f'Jointly held {row[0]}; also subject to {row[2]} held by {row[14]}'
                        else:
                            row[5] = f'IntHolder1 holds {row[0]}, IntHolder2 holds {row[1]}; also subject to {row[2]} held by {row[14]}'
                    elif row[0] in easements and row[1] in easements and row[2] == 'ROW':
                        row[4] = 'Ease'
                        if row[0] == row[1]:
                            row[5] = f'Jointly held {row[0]}; also subject to ROW held by {row[14]}'
                            row[6] = row[10]
                            row[7] = row[11]
                            row[8] = row[12]
                            row[9] = row[13]
                    elif row[0] in easements and row[1] == 'DR' and row[2] == 'DR':
                        row[4] = 'Ease'
                        row[5] = f'IntHolder1 holds {row[0]}; also subject to {row[1]} jointly held by {row[12]} & {row[14]}'
                        row[6] = row[10]
                        row[7] = row[11]
                    elif row[0] in easements and row[1] in easements and row[2] == 'FEE':
                        row[4] = 'Ease'
                        row[6] = row[10]
                        row[7] = row[11]
                        row[8] = row[12]
                        row[9] = row[13]
                        if row[0] == row[1]:
                            row[5] = f'Jointly held {row[0]}'
                        else:
                            row[5] = f'IntHolder1 holds {row[0]}, IntHolder2 holds {row[1]}'
                    elif row[0] == 'FEE' and row[1] in easements and row[2] in easements:
                        row[4] = 'Ease'
                        row[6] = row[12]   # IntHolder1 is OLI_2_ORG
                        row[7] = row[13]
                        row[8] = row[14]   # IntHolder2 is OLI_3_ORG
                        row[9] = row[15]
                        if row[1] == row[2]:
                            row[5] = f'Jointly held {row[1]}'
                        else:
                            row[5] = f'IntHolder1 holds {row[1]}, IntHolder2 holds {row[2]}'
                    elif row[0] == 'FEE' and row[1] == 'FEE' and row[2] == 'FEE':
                        row[4] = 'Fee'
                cur.updateRow(row)
        del row, cur
        print('Assigned ProtType...')

        # Clean up IntHolder1Type and IntHolder2Type
        with arcpy.da.UpdateCursor(data, 'IntHolder1Type') as cur:
            for row in cur:
                if row[0] in ['G', 'N', 'L']:
                    row[0] = 'PNP'
                elif row[0] == 'S':
                    row[0] = 'STP'
                elif row[0] == 'F':
                    row[0] = 'FED'
                elif row[0] in ['C', 'M']:
                    row[0] = 'LOC'
                elif row[0] == 'B':
                    row[0] = 'QP'
                elif row[0] == 'O':
                    row[0] = 'OTH'
                elif row[0] == 'X':
                    row[0] = 'UNK'
                elif row[0] == 'P':
                    row[0] = 'PFP'
                cur.updateRow(row)
        del row, cur
        print('Recoded IntHolder1Type...')

        with arcpy.da.UpdateCursor(data, 'IntHolder2Type') as cur:
            for row in cur:
                if row[0] in ['G', 'N', 'L']:
                    row[0] = 'PNP'
                elif row[0] == 'S':
                    row[0] = 'STP'
                elif row[0] == 'F':
                    row[0] = 'FED'
                elif row[0] in ['C', 'M']:
                    row[0] = 'LOC'
                elif row[0] == 'B':
                    row[0] = 'QP'
                elif row[0] == 'O':
                    row[0] = 'OTH'
                elif row[0] == 'X':
                    row[0] = 'UNK'
                elif row[0] == 'P':
                    row[0] = 'PFP'
                cur.updateRow(row)
        del row, cur
        print('Recoded IntHolder2Type...')

        # Assign 'Fee and Ease' ProtType category based on owner type and prot type
        c = 0
        with arcpy.da.UpdateCursor(data, ['FeeOwnType', 'ProtType']) as cur:
            for row in cur:
                if row[0] in ['PNP', 'QP', 'FED', 'STP', 'LOC'] and row[1] == 'Ease':
                    row[1] = 'Fee and Ease'
                    cur.updateRow(row)
                    c = c + 1
        print(f'Assigned Fee and Ease for {c} rows...')

    # Function to recode year. CAL_DATE_R is used first, and then FY_Funding is used if there is a value
    # there but not for CAL_DATE_R. If that happens, or if the years of CAL_DATE_R and FY_Funding do not match,
    # a comment is left in YearProtComments. A discrepancy between CAL_DATE_R and FY_Funding can indicate
    # multiple protection mechanisms. Note that CAL_DATE_R is a Date field and FY_Funding is Short.
    def recode_year():
        with arcpy.da.UpdateCursor(data, ['CAL_DATE_R', 'FY_Funding', 'YearProt', 'YearProtComments']) as cur:
            for row in cur:
                # Populate YearProt from either CAL_DATE_R or FY_Funding if only date available
                if row[0] is None:
                    if row[1] is not None:
                        row[2] = row[1]
                        row[3] = 'From FY_Funding'
                    else:
                        row[2] = 0
                elif row[0] is not None:
                    row[2] = str(row[0])[0:4]

                # Check for discrepancy between CAL_DATE_R and FY_Funding if both are avail
                if row[0] is not None and row[1] is not None:
                    if str(row[0])[0:4] != str(row[1]):
                        row[3] = 'Different years for CAL_DATE_R and FY_Funding'
                cur.updateRow(row)
        del row, cur
        print('Populated year fields and related comments')
    
    def assign_fee_ease_year():
        with arcpy.da.UpdateCursor(data, ['YearProt', 'YearProtComments', 'ProtType', 'FeeYear', 'EaseYear']) as cur:
            for row in cur:
                if row[1] != 'From FY_Funding':    # We don't want to take dates from funding since those may or may not align with initial protection
                    if row[2] == 'Fee':
                        row[3] = row[0]
                    elif row[2] == 'Ease':
                        row[4] = row[0]
                    cur.updateRow(row)
        print('Populated FeeYear and EaseYear...')

    def assign_gap():
        with arcpy.da.UpdateCursor(data, ['LEV_PROT', 'GapStatus']) as cur:
            for row in cur:
                if row[0] == 'P':
                    row[1] = 3        # This is the minimum -- we can't know beyond this from the data
                elif row[0] == 'N' or row[0] == 'L' or row[0] == 'T':    # L and T don't fit well into GapCodes. Gap 3 is permanent protection and Gap 4 is no protection
                    row[1] = 4                                           # Putting them in Gap 4 because the most important thing is that the protection is not permanent.
                elif row[0] == 'X':
                    row[1] = 0
                cur.updateRow(row)
        del row, cur
        print('Assigned GapStatus based on LEV_PROT')

    def recode_access():
        with arcpy.da.UpdateCursor(data, ['PUB_ACCESS', 'PubAccess']) as cur:
            for row in cur:
                if row[0] == 'Y':
                    row[1] = 'Yes'
                elif row[0] == 'L':
                    row[1] = 'Limited'
                elif row[0] == 'N':
                    row[1] = 'No'
                elif row[0] == 'X':
                    row[1] = 'Unknown'
                cur.updateRow(row)
        del row, cur
        print('Recoded public access')

    def assign_duration():
        with arcpy.da.UpdateCursor(data, ['LEV_PROT', 'ProtDuration', 'ProtType']) as cur:
            for row in cur:
                if row[0] == 'P':
                    row[1] = 'PERM'
                elif row[0] == 'T' or row[2] == 'Lease':
                    row[1] = 'TEMP'
                elif row[0] == 'L' or row[0] == 'N' or row[0] == 'X' or row[0] is None:
                    row[1] = 'UNK'
                cur.updateRow(row)
        del row, cur
        print('Assigned ProtDuration based on LEV_PROT')

    def assign_purpose():
        with arcpy.da.UpdateCursor(data, ['PRIM_PURP', 'Purpose']) as cur:
            for row in cur:
                if row[0] == 'R':
                    row[1] = 'Recreation'
                elif row[0] == 'C':
                    row[1] = 'Conservation'
                elif row[0] == 'B':
                    row[1] = 'Recreation & Conservation'
                elif row[0] == 'H':
                    row[1] = 'Historical/Cultural'
                elif row[0] == 'A':
                    row[1] = 'Agriculture'
                elif row[0] == 'W':
                    row[1] = 'Water Supply Protection'
                elif row[0] == 'S':
                    row[1] = 'Scenic'
                elif row[0] == 'F':
                    row[1] = 'Flood Control'
                elif row[0] == 'U':
                    row[1] = 'Underwater'
                elif row[0] == 'O':
                    row[1] = 'Other'
                elif row[0] == 'X':
                    row[1] = 'Unknown'
                cur.updateRow(row)
        print('Assigned Purpose...')

    def delete_fields():
        # Fields to delete - this is basically every original field except the altered fields which no longer exist
        deletes = ['TOWN_ID', 'POLY_ID', 'OWNER_ABRV', 'OWNER_TYPE', 'MANAGER', 'MANAGR_ABRV', 'MANAGR_TYPE',
                   'PRIM_PURP', 'PUB_ACCESS', 'LEV_PROT', 'OLI_1_ABRV', 'OLI_1_TYPE', 'OLI_2_ABRV', 'OLI_2_INT',
                   'OLI_3_ORG', 'OLI_3_ABRV', 'OLI_3_TYPE', 'OLI_3_INT', 'GRANTPROG1', 'GRANTTYPE1', 'GRANTPROG2',
                   'GRANTTYPE2', 'PROJ_ID1', 'PROJ_ID2', 'PROJ_ID3', 'EOEAINVOLVE', 'ARTICLE97', 'FY_FUNDING',
                   'GIS_ACRES', 'DEED_ACRES', 'OS_DEED_BOOK', 'OS_DEED_PAGE', 'ASSESS_ACRE', 'ASSESS_MAP',
                   'ASSESS_BLK', 'ASSESS_LOT', 'ASSESS_SUB', 'ALT_SITE_NAME', 'ATT_DATE', 'BASE_MAP', 'SOURCE_TYPE',
                   'LOC_ID', 'DCAM_ID', 'FEESYM', 'INTSYM', 'OS_ID', 'CAL_DATE_R', 'FORMAL_SITE_NAME', 'CR_REF',
                   'OS_TYPE', 'EEA_CR_ID', 'OLI_2_TYPE', 'SOURCE_MAP', 'FEE_OWNER', 'OLI_1_ORG', 'OLI_1_INT',
                   'OLI_2_ORG']

        arcpy.management.DeleteField(data, deletes)
        print('Deleted unneeded fields')

    ############ Call functions ###########
    try:
        add_alter_fields()
        calc_acres()
        recode_org_names('FeeOwner')
        recode_org_type(['OWNER_TYPE', 'FeeOwnType'])
        get_joint_fee_owners()
        correct_oth_org_types(['FeeOwner', 'FeeOwnType'])
        assign_fee_own_cat()
        assign_prot_type_int_holders()
        recode_org_names('IntHolder1')
        recode_org_names('IntHolder2')
        recode_year()
        assign_fee_ease_year()
        assign_gap()
        recode_access()
        assign_duration()
        assign_purpose()
    except Exception:
        print(traceback.format_exc())
    else:
        print('Successfully assigned values! Now deleting fields...')
        delete_fields()       # Only delete fields if the 'try' block is successful
    finally:
        print_elapsed_time()


def prep_tnc(data):
    ###### FUNCTIONS TO PREPROCESS DATA ######
    # Call this function first to prepare fields
    def add_alter_fields():
        arcpy.management.AddField(data, 'UID', 'TEXT', field_length=250)
        arcpy.management.CalculateField(data, 'UID', 'str(!OBJID0126!)')
        arcpy.management.AlterField(data, 'STATE_PROV', 'State', 'State')
        arcpy.management.AlterField(data, 'AREA_NAME', 'AreaName', 'AreaName')
        arcpy.management.AlterField(data, 'FEE_OWNER', 'FeeOwner', 'FeeOwner')
        arcpy.management.AlterField(data, 'FEE_ORGTYP', 'FeeOwnType', 'FeeOwnType')
        arcpy.management.AddField(data, "FeeOwnCat", "TEXT", field_length = 7)
        arcpy.management.AlterField(data, 'INT_HOLDER', 'IntHolder1', 'IntHolder1')
        arcpy.management.AlterField(data, 'INT_ORGTYP', 'IntHolder1Type', 'IntHolder1Type')
        arcpy.management.AddField(data, "IntHolder2", "TEXT", field_length = 150)
        arcpy.management.AddField(data, "IntHolder2Type", "TEXT", field_length = 3)
        arcpy.management.AlterField(data, 'YEAR_EST', 'YearProt', 'YearProt')
        arcpy.management.AddField(data, "FeeYear", "SHORT")
        arcpy.management.AddField(data, "EaseYear", "SHORT")
        arcpy.management.AlterField(data, 'PUB_ACCESS', 'PubAccess', 'PubAccess')
        arcpy.management.AlterField(data, 'GAP_STATUS', 'GapStatus', 'GapStatus')
        arcpy.management.AddField(data, 'ProtType', 'TEXT', field_length=15)
        arcpy.management.AddField(data, 'ProtDuration', 'TEXT', field_length=5)
        print('Added and altered fields')

    # Recode owner and int holder org types -- FeeOwnType and IntHolder1Type are both altered fields
    # that just need to be recoded. Note that TNC only provides one int holder column.
    # Since we are recoding within already populated fields, we only need to address values we want to change.
    def recode_org_type(field):
        with arcpy.da.UpdateCursor(data, field) as cur:
            for row in cur:
                if row[0] is None or row[0] == '' or row[0] == ' ':
                    if field == 'FeeOwnType':      # Only set empty rows to UNK for owner type
                        row[0] = 'UNK'
                    elif field == 'IntHolder1Type':
                        row[0] = None             # Set to null if interest holder
                elif row[0] == 'DIST':
                    row[0] = 'LOC'
                elif row[0] == 'JNT':
                    row[0] = 'OTH'
                elif row[0] == 'NGO':
                    row[0] = 'PNP'
                elif row[0] == 'STAT':
                    row[0] = 'STP'
                elif row[0] == 'TRIB':
                    row[0] = 'TRB'
                elif row[0] == 'DESG':      # There is only 1 row with this value, and the fee owner is 7 Lakes Alliance
                    row[0] = 'PNP'          # which is definitely PNP. If this changes, this code may need to be (re)moved.
                cur.updateRow(row)
        del row, cur
        print('Recoded org types in {}'.format(field))
    
    def assign_fee_own_cat():
        with arcpy.da.UpdateCursor(data, ["FeeOwnType", "FeeOwnCat"]) as cur:
            for row in cur:
                if row[0] in ["PNP", "PVT", "TRB"]:
                    row[1] = "Private"
                elif row[0] in ["FED", "STP", "LOC"]:
                    row[1] = "Public"
                elif row[0] == "OTH":
                    row[1] = "Other"
                elif row[0] == "UNK":
                    row[1] = "Unknown"
                cur.updateRow(row)
        print("Assigned FeeOwnCat...")

    # Recoding access in existing field -- 'Unknown' already exists and doesn't need to be recoded
    def recode_access():
        with arcpy.da.UpdateCursor(data, 'PubAccess') as cur:
            for row in cur:
                if row[0] is None:
                    row[0] = 'Unknown'
                elif row[0] == 'Open Access':
                    row[0] = 'Yes'
                elif row[0] == 'Limited Access':
                    row[0] = 'Limited'
                elif row[0] == 'No Access':
                    row[0] = 'No'
                cur.updateRow(row)
        del row, cur
        print('Recoded PubAccess')

    # Recoding GAP in existing field -- only need to recode the values representing unknown
    def recode_gap():
        with arcpy.da.UpdateCursor(data, 'GapStatus') as cur:
            for row in cur:
                if row[0] is None or row[0] == 9:
                    row[0] = 0
                cur.updateRow(row)
        del row, cur
        print('Recoded unknown GAP codes')

    # Recoding INT_TYPE into new field ProtType. Keeping these fields separate so we can use some of the
    # INT_TYPEs not retained in NEPOS to inform ProtDuration. Since we are populating a new field we need
    # to address all values of INT_TYPE, even those we don't need to change.
    # This code must be run after FeeOwnType is recoded using recode_org_type() 
    def recode_prot_type():
        with arcpy.da.UpdateCursor(data, ['INT_TYPE', 'ProtType']) as cur:
            for row in cur:
                if row[0] is None or row[0] == '' or row[0] == 'Unknown':
                    row[1] = 'Unknown'
                elif row[0] == 'Conservation Easement':
                    row[1] = 'Ease'
                elif row[0][:17] == 'Deed Restrictions':
                    row[1] = 'DR'
                elif row[0] == 'Fee Ownership' or row[0] == 'Transfer - Fee Ownership':
                    row[1] = 'Fee'
                elif row[0] == 'Fee Ownership and Conservation Easement':
                    row[1] = 'Fee and Ease'
                elif row[0] == 'Life Estate' or row[0] == 'Management Lease or Agreement' or row[0] == 'Reverter' or row[0] == 'Timber Lease or Agreement' or row[0] == 'Other':
                    row[1] = 'Other'
                elif row[0] == 'Right of Way Tract':
                    row[1] = 'ROW'
                cur.updateRow(row)
        del row, cur
        print('Recoded INT_TYPE values in ProtType...')

        with arcpy.da.UpdateCursor(data, ['FeeOwnType', 'ProtType']) as cur:
            for row in cur:
                if row[0] in ['PNP', 'LOC', 'STP', 'FED'] and row[1] == 'Ease':
                    row[1] = 'Fee and Ease'
                    cur.updateRow(row)
        print('Assigned Fee and Ease ProtType...')

    # Assigning ProtDuration based on GapStatus. Based on fields available in TNC data, we can only distinguish
    # b/w permanent and unknown duration
    def assign_duration():
        with arcpy.da.UpdateCursor(data, ['GapStatus', 'ProtDuration']) as cur:
            for row in cur:
                if 1 <= row[0] <= 3 or row[0] == 39:    # Line corrected 2024-04-05 (after MA)
                    row[1] = 'PERM'
                else:
                    row[1] = 'UNK'
                cur.updateRow(row)
        del row, cur
        print('Assigned ProtDuration based on GapStatus')

    def assign_fee_ease_year():
        with arcpy.da.UpdateCursor(data, ["ProtType", "YearProt", "FeeYear", "EaseYear"]) as cur:
            for row in cur:
                if row[0] == "Ease" and row[1] > 0:
                    row[3] = row[1]
                elif row[0] == "Fee" and row[1] > 0:
                    row[2] = row[1]
                cur.updateRow(row)
        print("Assigned FeeYear and EaseYear...")

    # Recode FeeOwner and IntHolder1 names (in separate function calls). Both these fields are altered
    # and populated - just need to address values we want to change. Two fields should be entered:
    # [NAMEFIELD, STATEFIELD] -- this is important because not all state entities have a state name/abrv
    # included and we use the state field to prevent us from mixing up state agencies
    #### THIS FUNCTION IS NOT COMPLETE!!! NEED TO DO OTHER STATE AGENCIES!!! #####
    def recode_names(fields):
        with arcpy.da.UpdateCursor(data, fields) as cur:
            for row in cur:
                # Set empty names to Null (currently they are just a space) if int holder
                # or unknown if fee owner
                if row[0] == '' or row[0] == ' ':
                    if fields[0] == 'FeeOwner':
                        row[0] = 'Unknown'
                        cur.updateRow(row)
                        continue
                    elif fields[0] == 'IntHolder1':
                        row[0] = None
                        cur.updateRow(row)
                        continue
                # Federal names
                elif row[0] == 'Army Corps of Engineers':
                    row[0] = 'US DOD - Army Corps of Engineers'
                elif row[0] == 'Federal Aviation Administration':
                    row[0] = 'US DOT - Federal Aviation Administration'
                elif row[0] == 'Maine Army National Guard':
                    row[0] = 'US DOD - National Guard'
                elif row[0] == 'National Park Service A/T' or row[0] == 'US National Park Service':
                    row[0] = 'US DOI - National Park Service'
                elif row[0] == 'NMFS':
                    row[0] = 'NOAA - National Marine Fisheries Service'
                elif row[0] == 'NOAA':
                    row[0] = 'National Oceanic and Atmospheric Administration'
                elif row[0] == 'US Coast Guard' or row[0] == 'US Department of Coast Guard':
                    row[0] = 'US DOD - Coast Guard'
                elif row[0] == 'US Department of Interior':
                    row[0] = 'US Department of the Interior'
                elif row[0] == 'US Department of the Air Force':
                    row[0] = 'US DOD - Air Force'
                elif row[0] == 'US Federal':
                    row[0] = 'United States of America'
                elif row[0] == 'US Fish and Wildlife Service':
                    row[0] = 'US DOI - Fish and Wildlife Service'
                elif row[0] == "US Veteran's Administration":
                    row[0] = 'US Department of Veterans Administration'
                elif row[0] == 'USDA Forest Service':
                    row[0] = 'USDA - Forest Service'
                elif row[0] == 'US Department of the Navy':
                    row[0] = 'US DOD - Navy'
                elif row[0] == 'US Natural Resources Conservation Service' or row[0] == 'USDA Natural Resource Conservation Service':
                    row[0] = 'USDA - Natural Resources Conservation Service'
                elif row[0] == 'USDA Grassland Reserve Program':
                    row[0] = 'USDA - Farm Service Agency'     # This is the agency administering the GRP
                if row[1] == 'MA':
                    # MA state names
                    if row[0] == 'Board of Regional Community Colleges':
                        row[0] = 'MA Board of Regional Community Colleges'
                    elif row[0] == 'DCR - Division of State Parks and Recreation' or row[0] == 'DCR   Division of State Parks and Recreation':
                        row[0] = 'MA DCR - Division of State Parks and Recreation'
                    elif row[0] == 'DCR - Division of State Parks and Recreation / Department of Fish and Game':
                        row[0] = 'MA DCR - Division of State Parks and Recreation / MA Department of Fish and Game'
                    elif row[0] == 'DCR - Division of Water Supply Protection':
                        row[0] = 'MA DCR - Division of Water Supply Protection'
                    elif row[0] == 'Department of Agricultural Resources' or row[0] == 'Department of Agricultural Reosurces' or row[0] == 'Department of Agricultural Resouces' \
                            or row[0] == 'Department of Agriculture' or row[0] == 'Massachusetts Department of Agricultural Resources':
                        row[0] = 'MA Department of Agricultural Resources'
                    elif row[0] == 'Department of Capital Asset Management':
                        row[0] = 'MA Department of Capital Asset Management'
                    elif row[0] == 'Department of Corrections':
                        row[0] = 'MA Department of Corrections'
                    elif row[0] == 'Department of Developmental Services':
                        row[0] = 'MA Department of Developmental Services'
                    elif row[0] == 'Department of Fish and Game':
                        row[0] = 'MA Department of Fish and Game'
                    elif row[0] == 'Department of Transportation':
                        row[0] = 'MA Department of Transportation'
                    elif row[0] == 'Health and Human Services':
                        row[0] = 'MA Executive Office of Health and Human Services'
                    elif row[0] == 'Massachusetts Bay Transit Authority':
                        row[0] = 'Massachusetts Bay Transportation Authority'
                    elif row[0] == 'Massachusetts Department of Public Works':
                        row[0] = 'MA Department of Public Works'
                    elif row[0] == 'Massachusetts Highway Department':
                        row[0] = 'MA DOT - Highway Division'
                    elif row[0] == 'Secretary of the Commonwealth':
                        row[0] = 'Secretary of the Commonwealth of Massachusetts'
                    elif row[0] == 'Executive Office of Environmental Affairs':
                        row[0] = 'MA Executive Office of Energy and Environmental Affairs'
                    elif row[0] == 'MA Division of Fish and Wildlife':
                        row[0] = 'MA DFG - Division of Fish and Wildlife'
                    # MA local names
                    elif row[0] == 'BRISTOL COUNTY AGRICULTURAL HIGH SCHOOL' or row[0] == 'BRISTOL COUNTY AGRICULTURAL SCHOOL':
                        row[0] = 'Bristol County Agricultural High School'
                    elif row[0] == 'Nantucket Island Land Bank':
                        row[0] = 'Nantucket Islands Land Bank'
                    elif row[0] == 'SALEM AND BEVERLY WATER SUPPLY BOARD':
                        row[0] = 'Salem-Beverly Water Supply Board'
                # Private names
                elif row[0] == 'A D Makepeace Company' or row[0] == 'A. D. Makepeace Company':
                    row[0] = 'AD Makepeace Company'
                elif row[0] == 'Acushnet River Reserve Inc':
                    row[0] = 'Acushnet River Reserve'
                elif row[0] == 'AMERICAN LEGION':
                    row[0] = 'American Legion'
                elif row[0] == 'Ames Rife and Pistol Club' or row[0].upper() == 'AMES RIFLE AND PISTOL CLUB':
                    row[0] = 'Ames Rifle and Pistol Club'
                elif row[0].upper() == 'ANGLE TREE STONE ROD AND GUN CLUB':
                    row[0] = 'Angle Tree Stone Rod and Gun Club'
                elif row[0] == 'Appalachain Mountain Club':
                    row[0] = 'Appalachian Mountain Club'
                elif row[0].upper() == 'AQUARION WATER COMPANTY INC' or row[
                    0].upper() == 'AQUARION WATER COMPANY' or row[0].upper() == 'AQUARION WATER COMPANY INC' or row[0] == 'Aquarion Water Company of Connecticut':
                    row[0] = 'Aquarion Water Company'
                elif row[0] == 'ARBELLA LAND CO':
                    row[0] = 'Arbella Land Company'
                elif row[0] == 'ARTEMISIA REALTY TRUST':
                    row[0] = 'Artemisia Realty Trust'
                elif row[0][0:20].upper() == 'ASPETUCK LAND TRUST' or row[0] == 'Aspectuck Land Trust Inc':
                    row[0] = 'Aspetuck Land Trust'
                elif row[0] == 'ASSET CONSTRUCTION COMPANY':
                    row[0] = 'Asset Construction Company'
                elif row[0] == 'ASSOCIATION OF JEWISH PHILANTHROPIES INC' or row[0] == 'Association of Jewish Philanthropies Inc':
                    row[0] = 'Association of Jewish Philanthropies'
                elif row[0].upper() == 'AVCO CORP':
                    row[0] = 'Avco Corp'
                elif row[0] == 'B and N Lands LLC':
                    row[0] = 'B&N Lands LLC'
                elif row[0][0:21] == 'Barnstable Land Trust':
                    row[0] = 'Barnstable Land Trust'
                elif row[0] == 'Barrington Land Conservancy Trust' or row[0] == 'Barrington RI Land Conservation Trust':
                    row[0] = 'Barrington Land Conservation Trust'
                elif row[0] == 'BAY CLUB COMMUNITY ASSOCIATION INC' or row[0] == 'Bay Club Community Association Inc.':
                    row[0] = 'Bay Club Community Association'
                elif row[0][0:21].upper() == 'BEAR HILL ASSOCIATION':
                    row[0] = 'Bear Hill Association'
                elif row[0] == 'Berkshire Natural Resources Council Inc' or row[0] == 'BNRC':
                    row[0] = 'Berkshire Natural Resources Council'
                elif row[0] == 'Bethlehem Land Trust, Inc.':
                    row[0] = 'Bethlehem Land Trust'
                elif row[0] == 'Blackston Valley Boys and Girls Club, Inc.' or row[0] == 'Blackstone Valley Boys and Girls Club Inc':
                    row[0] = 'Blackstone Valley Boys and Girls Club'
                elif row[0] == 'BORDEN COLONY':
                    row[0] = 'Borden Colony'
                elif row[0] == 'Boston & Maine Railroad' or row[0] == 'BOSTON AND MAINE RAILROAD':
                    row[0] = 'Boston and Maine Railroad'
                elif row[0] == 'BOSTON FOUNDATION CONSERVATION FUND INC' or row[0] == 'Boston Foundation Conservation Fund Inc.':
                    row[0] = 'Boston Foundation Conservation Fund'
                elif row[0] == 'BOSTON SYMPHONY ORCHESTRA':
                    row[0] = 'Boston Symphony Orchestra'
                elif row[0] == 'Boxford Trails Association/Boxford Open Lands Tru*' or row[0] == 'BOXFORD TRAIL ASSOCIATION/BOXFORD OPEN LAND TRUST*':
                    row[0] = 'Boxford Trails Association/Boxford Open Land Trust'
                elif row[0] == 'BOY AND GIRL SCOUTS OF KINGSTON INC':
                    row[0] = 'Boy & Girl Scouts of Kingston'
                elif row[0] == 'BRAINTREE LIMITED PARTNERSHIP INC' or row[0] == 'Braintree Limited Partnership Inc.':
                    row[0] = 'Braintree Limited Partnership'
                elif row[0] == 'BRANDEIS UNIVERSITY':
                    row[0] = 'Brandeis University'
                elif row[0] == 'BREWSER CONSERVATION TRUST':
                    row[0] = 'Brewster Conservation Trust'
                elif row[0] == 'Bridgewater Land Trust, Inc.':
                    row[0] = 'Bridgewater Land Trust'
                elif row[0] == 'BRISTOL COUNTY AGRICULTURAL HIGH SCHOOL' or row[0] == 'BRISTOL COUNTY AGRICULTURAL SCHOOL':
                    row[0] = 'Bristol County Agricultural High School'
                elif row[0] == 'BRITTANY ESTATES TRUST':
                    row[0] = 'Brittany Estates Trust'
                elif row[0] == 'Brookine Conservation Land Trust':
                    row[0] = 'Brookline Conservation Land Trust'
                elif row[0] == 'Canton Land Conservation Trust, Inc.':
                    row[0] = 'Canton Land Conservation Trust'
                elif row[0] == 'CAPE COD MUSEUM OF NATURAL HISTORY INC' or row[0] == 'Cape Cod Museum of Natural History Inc.':
                    row[0] = 'Cape Cod Museum of Natural History'
                elif row[0] == 'Carlisle Conservation Foundation, Inc.':
                    row[0] = 'Carlisle Conservation Foundation'
                elif row[0] == 'CATHOLIC CEMETERY ASSOCIATION':
                    row[0] = 'Catholic Cemetery Association'
                elif row[0] == 'CHARLES RIVER COUNTRY CLUB' or row[0] == 'CHARLES RIVER COUNTRY CLUB INC':
                    row[0] = 'Charles River Country Club'
                elif row[0] == 'Chebeague and Cumberland Land Trust, Inc.' or row[0] == 'Cumberland Mainland and Island Trust':
                    row[0] = 'Chebeague and Cumberland Land Trust'
                elif row[0] == 'CHEQUESSET YACHT AND COUNTRY CLUB':
                    row[0] = 'Chequesset Yacht and Country Club'
                elif row[0] == 'CHRIST CHURCH':
                    row[0] = 'Christ Church'
                elif row[0] == 'Colchester Fish & Game Club Inc.':
                    row[0] = 'Colchester Fish and Game Club'
                elif row[0] == 'COLD SPRING FARM ASSOCIATION':
                    row[0] = 'Cold Spring Farm Association'
                elif row[0] == 'Combined Jewish Philanthropies of Greater Boston Inc' or row[0] == 'Combined Jewish Philanthropies of Greater Boston *':
                    row[0] = 'Combined Jewish Philanthropies of Greater Boston'
                elif row[0] == 'COMMONS AT HOPKINGTON ASSOCIATION' or row[0] == 'COMMONS AT HOPKINTON ASSOCIATION':
                    row[0] = 'Commons at Hopkinton Association'
                elif row[0] == 'Cornwall Conservation Trust, Inc.':
                    row[0] = 'Cornwall Conservation Trust'
                elif row[0] == 'CUDDIGAN REALTY INC':
                    row[0] = 'Cuddigan Realty Incorporated'
                elif row[0] == 'CUNNINGHAM FOUNDATION OF MILTON':
                    row[0] = 'Cunningham Foundation of Milton'
                elif row[0] == 'DAVNA CORP':
                    row[0] = 'Davna Corp'
                elif row[0] == 'DESIGN HOUSING INC':
                    row[0] = 'Design Housing Inc.'
                elif row[0] == 'Dudley Conservation Land Trust ':
                    row[0] = 'Dudley Conservation Land Trust'
                elif row[0] == 'EAST CHOP ASSOCIATION' or row[0] == 'East Chop Association Inc' or row[0] == 'EAST CHOP ASSOCIATION INC':
                    row[0] = 'East Chop Association'
                elif row[0] == 'East Haddam Land Trust, Inc.':
                    row[0] = 'East Haddam Land Trust'
                elif row[0] == 'East Quabbin Land Trust Inc' or row[0] == 'East Quabbin Land Trust, Inc.':
                    row[0] = 'East Quabbin Land Trust'
                elif row[0] == 'EGREMONT COUNTRY CLB' or row[0] == 'EGREMONT COUNTRY CLUB':
                    row[0] = 'Egremont Country Club'
                elif row[0] == 'ELMWOOD CERMTERY ASSOCIATION' or row[0] == 'ELMWOOD CEMETERY ASSOCIATION':
                    row[0] = 'Elmwood Cemetery Association'
                elif row[0] == 'ENDICOTT COLLEGE':
                    row[0] = 'Endicott College'
                elif row[0] == 'ESSEX COUNTY SPORTSMANS ASSOCIATION':
                    row[0] = 'Essex County Sportsmans Association'
                elif row[0] == 'Fairhaven Acushnet Land Preservation Trust':
                    row[0] = 'Fairhaven-Acushnet Land Preservation Trust'
                elif row[0] == 'FAIRVIEW FISH AND GAME' or row[0] == 'FAIRVIEW FISH AND GAME CLUB':
                    row[0] = 'Fairview Fish and Game Club'
                elif row[0] == 'Faith Bible Free Church of Woodstock':
                    row[0] = 'Faith Bible Evangelical Free Church of Woodstock'
                elif row[0] == 'Falmouth Conservation Trust':
                    row[0] = 'Falmouth Land Trust'
                elif row[0] == 'FIN FUR AND FEATHER CLUB' or row[0] == 'Fin, Fur & Feather Club, Inc.':
                    row[0] = 'Fin, Fur, and Feather Club'
                elif row[0] == 'GIRL SCOUT COUNCIL OF SE MASS':
                    row[0] = 'Girl Scouts of Southeastern Massachusetts'
                elif row[0] == 'Granby Land Trust, Inc.':
                    row[0] = 'Granby Land Trust'
                elif row[0] == 'GREEN MEADOW ASSOCIATION INC' or row[0] == 'Green Meadow Association Inc.':
                    row[0] = 'Green Meadow Association'
                elif row[0] == 'HALE RERSERVATION INC' or row[0].upper() == 'HALE RESERVATION INC':
                    row[0] = 'Hale Reservation'
                elif row[0] == 'HAMPDEN COUNTRY CLUB' or row[0] == 'HAMPDEN COUNTRY CLUB INC.':
                    row[0] = 'Hampden Country Club'
                elif row[0] == 'HARVARD PRES AND FELLOWS':
                    row[0] = 'Harvard University'
                elif row[0].upper() == 'HULL FOREST PRODUCTS INC' or row[0] == 'Hull Forest Projects Inc' or row[0] == 'HULL WOOD PRODUCTS':
                    row[0] = 'Hull Forest Products'
                elif row[0] == 'Hull Forestlands':
                    row[0] = 'Hull Forestlands LP'
                elif row[0] == 'INDIAN MEADOW GOLF INC' or row[0] == 'INDIAN MEADOWS GOLF INC':
                    row[0] = 'Indian Meadows Golf Club'
                elif row[0] == 'Jack O-Lantern Trust':
                    row[0] = 'Jack-O-Lantern Trust'
                elif row[0] == 'Katharine Nordell Lloyd Center for Environmental *' or row[
                    0] == 'Katharine Nordell Lloyd Center for Environmental Studies Inc':
                    row[0] = 'Lloyd Center for the Environment'
                elif row[0] == 'Lincoln Ridge Conservation Trust/Farrar Pond Cons' or row[
                    0] == 'LINCOLN RIDGE CONSERVATION TRUST/FARRAR POND CONSERVATION TRUST':
                    row[0] = 'Lincoln Ridge Conservation Trust/Farrar Pond Conservation Trust'
                elif row[0] == 'Little Compton Ag. Conservancy Trust':
                    row[0] = 'Little Compton Agricultural Conservancy Trust'
                elif row[0] == 'MARSHFIELD COUNTRY CLUB INC' or row[0] == 'Marshfield Country Club Inc':
                    row[0] = 'Marshfield Country Club'
                elif row[0] == 'MARSHFIELD ROD AND GUN CLUB' or row[0] == 'MARSHFIELD ROD AND GUN CLUB INC':
                    row[0] = 'Marshfield Rod and Gun Club'
                elif row[0] == "Marthas Vineyard Land Bank":
                    row[0] = "Martha's Vineyard Land Bank"
                elif row[0] == 'Middlebury Land Trust, Inc.':
                    row[0] = 'Middlebury Land Trust'
                elif row[0] == 'Montague Economic Development Industrial Corporat*':
                    row[0] = 'Montague Economic Development Industrial Corporation'
                elif row[0] == 'Nantucket Conservation Foundation Inc' or row[0] == 'Nantucket Conservation Foundation, Inc.':
                    row[0] = 'Nantucket Conservation Foundation'
                elif row[0] == 'Nantucket Island Land Bank':
                    row[0] = 'Nantucket Islands Land Bank'
                elif row[0] == 'New England Power' or row[0] == 'NEW ENGLAND POWER COMPANY' or row[
                    0] == 'New England Power Complany' or row[0] == 'New England Power Corporation':
                    row[0] = 'New England Power Company'
                elif row[0] == 'NIMROD LEAGUE OF HOLDEN' or row[0] == 'Nimrod League of Holden Inc':
                    row[0] = 'Nimrod League of Holden'
                elif row[0] == 'Norfolk Land Trust, Inc.':
                    row[0] = 'Norfolk Land Trust'
                elif row[0] == 'Plum Creek':
                    row[0] = 'Plum Creek Timber Company'
                elif row[0] == 'Snake Meadow Club Inc' or row[0] == 'Snake Meadow Club, Inc.':
                    row[0] = 'Snake Meadow Club'
                elif row[0] == 'Society for the Preservation of New England Antiq*' or row[0] == 'Society for the Preservation of New England Antiqu':
                    row[0] = 'Society for the Preservation of New England Antiquities'
                elif row[0] == 'Society for the Protection of NH Forests':
                    row[0] = 'Society for the Protection of New Hampshire Forests'
                elif row[0] == 'SOUTH BARRE R&G INC' or row[0] == 'SOUTH BARRE ROD/GUN INC':
                    row[0] = 'South Barre Rod and Gun Club'
                elif row[0] == 'South Central Regional Water Authority':
                    row[0] = 'South Central Connecticut Regional Water Authority'
                elif row[0] == 'SOUTH SHORE NATURAL SCIENCE CENTER INC' or row[0] == 'South Shore Natural Science Center, Inc.':
                    row[0] = 'South Shore Natural Science Center'
                elif row[0] == "St. Jospeh's Abbey":
                    row[0] = "St. Joseph's Abbey"
                elif row[0] == 'The 300 Committee Land Trust Inc':
                    row[0] = 'The 300 Committee Land Trust'
                elif row[0] == 'Tufts Univeristy':
                    row[0] = 'Tufts University'
                elif row[0] == 'Weantinoge Heritage Land Trust' or row[0] == 'Weantinoge Heritage Land Trust, Inc.' or row[0] == 'Weantinoge Heritage, Inc.':
                    row[0] = 'Northwest Connecticut Land Conservancy'
                elif row[0] == 'Wildlands Trust Inc.':
                    row[0] = 'Wildlands Trust'
                elif row[0] == 'WOODS HOLE MARINE BIOLOGICAL LABORATORY':
                    row[0] = 'Woods Hole Marine Biological Laboratory'
                elif row[0] == 'York Land Trust, Inc.':
                    row[0] = 'York Land Trust'
                # Unknown names
                elif row[0] == 'N/A':
                    if fields[0] == 'FeeOwner':
                        row[0] = 'Unknown'        # Only set Unknown if it's FeeOwner (not interest holder)
                cur.updateRow(row)
        del row, cur
        print('Standardized names in {}'.format(fields[0]))

    def delete_fields():
        # Just need to delete fields that were not altered - except SOURCE which we want to keep
        deletes = ['INT_TYPE', 'ST_DESIG', 'DESIGNAT', 'YEAR_SRC', 'GIS_ACRES', 'AUTHOR', 'MOD_DATE',
                   'SUBM_FILE', 'GAPPRIVAL', 'OBJID0126']
        arcpy.management.DeleteField(data, deletes)
        print('Deleted unnecessary fields')

    ####### CALL FUNCTIONS ########
    try:
        add_alter_fields()
        recode_org_type('FeeOwnType')
        assign_fee_own_cat()
        recode_org_type('IntHolder1Type')
        recode_access()
        recode_gap()
        recode_prot_type()
        assign_duration()    # Rerun with corrected code 2024-04-05
        assign_fee_ease_year()
        recode_names(['FeeOwner', 'State'])     # Need State field when recoding state agency names (just to be careful)
        recode_names(['IntHolder1', 'State'])
    except Exception:
        print(traceback.format_exc())
    else:
        print('Successfully assigned values! Now deleting fields...')
        #delete_fields()  # Only delete fields if the 'try' block is successful
    finally:
        print_elapsed_time()


# IntHolder2 should check for '' and set to Null
def prep_nced(data):

    def add_alter_fields():
        arcpy.management.AddField(data, 'UID', 'TEXT', field_length=250)
        arcpy.management.CalculateField(data, 'UID', 'str(!unique_id!)')
        arcpy.management.AddField(data, 'State2', 'TEXT', field_length=2)
        arcpy.management.AlterField(data, 'sitename', 'AreaName', 'AreaName')
        arcpy.management.AlterField(data, 'esmthldr', 'IntHolder1', 'IntHolder1')
        arcpy.management.AlterField(data, 'eholdtype', 'IntHolder1Type', 'IntHolder1Type')
        arcpy.management.AddField(data, "FeeOwner", "TEXT", field_length=150)
        arcpy.management.AlterField(data, 'owntype', 'FeeOwnType', 'FeeOwnType')
        arcpy.management.AddField(data, "FeeOwnCat", "TEXT", field_length=7)
        arcpy.management.AlterField(data, 's_emthd1', 'IntHolder2', 'IntHolder2')
        arcpy.management.AddField(data, 'IntHolder2Type', 'TEXT', field_length=3)
        arcpy.management.AddField(data, "ProtType", "TEXT", field_length=15)
        # Because of annoying issues with ArcGIS and capitalization in fields,
        # we can't alter pubaccess into PubAccess. We need to add a new field PubAccess1,
        # recode pubaccess into this field, and then alter PubAccess1 once pubaccess is deleted
        arcpy.management.AddField(data, 'PubAccess1', "TEXT", field_length=7)
        arcpy.management.AlterField(data, 'duration', 'ProtDuration', 'ProtDuration')
        arcpy.management.AlterField(data, 'year_est', 'YearProt', 'YearProt')
        arcpy.management.AddField(data, "EaseYear", "SHORT")
        arcpy.management.AddField(data, "FeeYear", "SHORT")  # Even though can't be used, needed for append_rows.py
        arcpy.management.AddField(data, 'GapStatus', 'SHORT')   # NCED gap field is text, so adding new numeric one
        arcpy.management.AlterField(data, "datapvdr", "Source", "Source")
        print('Added and altered fields...')
    
    def add_calculate_acres():
        arcpy.management.CalculateGeometryAttributes(data, [["acres", "AREA"]], area_unit = "ACRES_US")
        print("Calculated acres...")
    
    # Frustratingly, there seem to be different rules for different tools about whether
    # field names are case sensitive. Altering the "state" field to "State" does not work (only changes alias).
    # But can't add "State" field either since it 'already exists'. In order to append data to NEPOS
    # the state field must match case (ie, be 'State'). So we need to add a new state field,
    # copy existing state field, then delete old state field, and rename new state field. Ridiculous!
    def populate_state():
        arcpy.management.CalculateField(data, "State2", "!state!")
        arcpy.management.DeleteField(data, "state")
        arcpy.management.AlterField(data, "State2", "State", "State")
        print("Reconfigured state field...")

    # Similar to TNC, we'll use the state field to better control recoding of state agency names
    # THis function does not work with co-holder field...
    def recode_names():
        with arcpy.da.UpdateCursor(data, ['IntHolder1', 'IntHolder1Type', 'State']) as cur:
            for row in cur:
                if row[0] is None or row[0] == ' ':
                    continue
                # MA-specific names
                if row[2] == 'MA':
                    # State agencies
                    if row[1] == 'STP':
                        if row[0] == 'DCR   Division of State Parks and Recreation':
                            row[0] = 'MA DCR - Division of State Parks and Recreation'
                        elif row[0] == 'Department of Environmental Protection':
                            row[0] = 'MA Department of Environmental Protection'
                        elif row[0] == 'Massachusetts Department of Agricultural Resources':
                            row[0] = 'MA Department of Agricultural Resources'
                        elif row[0] == 'Massachusetts Department of Conservation and Recreation':
                            row[0] = 'MA Department of Conservation and Recreation'
                        elif row[0] == 'Massachusetts Department of Fish and Game':
                            row[0] = 'MA Department of Fish and Game'
                        elif row[0] == 'Massachusetts Executive Office of Energy and Environmental Affairs':
                            row[0] = 'MA Executive Office of Energy and Environmental Affairs'
                    # For municipal names, we're adding Town / City of and removing the ', MA' at the end
                    # Towns -- NEED TO MAKE SURE TO ACCOUNT FOR TOWN NAMES THAT ARE ALSO COUNTY NAMES
                    # SO WE DON"T ACCIDENTALLY MESS UP COUNTY OWNERS -- Franklin, Essex, Worcester, Hampden
                    if row[1] == 'LOC':
                        # First handle typos and errors
                        if row[0] == 'Haverill Conservation Commission, MA':
                            row[0] = 'City of Haverhill Conservation Commission'
                        elif row[0] == 'Lunenbury, MA':
                            row[0] = 'Town of Lunenburg'
                        elif row[0] == 'Meadow City Conservation Coalition, MA':    # This is a 501c3 not a municipality
                            row[0] = row[0][:-4]
                            row[1] = 'PNP'
                        elif row[0] == 'Plymouth, NH':    # These 3 "NH" rows are actually in MA
                            row[0] = 'Town of Plymouth'
                        elif row[0] == 'Amherst, NH':
                            row[0] = 'Town of Amherst'
                        elif row[0] == 'Pembroke, NH':
                            row[0] = 'Town of Pembroke'
                        elif row[0][:5] == 'Acton' or row[0][:8] == 'Acushnet' or row[0][:7] == 'Amherst' or row[0][:7] == 'Andover' \
                                or row[0][:9] == 'Arlington' or row[0][:10] == 'Ashburnham' or row[0][:5] == 'Ashby' or row[0][:7] == 'Ashland' \
                                or row[0][:5] == 'Athol' or row[0][:4] == 'Ayer' or row[0][:5] == 'Barre' or row[0][:7] == 'Bedford' \
                                or row[0][:11] == 'Belchertown' or row[0][:10] == 'Bellingham' or row[0][:7] == 'Belmont' \
                                or row[0][:7] == 'Berkley' or row[0][:6] == 'Berlin' or row[0][:9] == 'Billerica' or row[0][:6] == 'Bolton' \
                                or row[0][:6] == 'Bourne' or row[0][:10] == 'Boxborough' or row[0][:7] == 'Boxford' or row[0][:9] == 'Braintree' \
                                or row[0][:8] == 'Brewster' or row[0][:11] == 'Bridgewater' or row[0][:9] == 'Brimfield' \
                                or row[0][:10] == 'Brookfield' or row[0][:9] == 'Brookline' or row[0][:8] == 'Buckland' \
                                or row[0][:10] == 'Burlington' or row[0][:8] == 'Carlisle' or row[0][:10] == 'Charlemont' \
                                or row[0][:7] == 'Chatham' or row[0][:8] == 'Chilmark' or row[0][:7] == 'Clinton' or row[0][:8] == 'Cohasset' \
                                or row[0][:7] == 'Concord' or row[0][:6] == 'Conway' or row[0][:7] == 'Danvers' or row[0][:9] == 'Dartmouth' \
                                or row[0][:5] == 'Dennis' or row[0][:7] == 'Dighton' or row[0][:7] == 'Douglas' or row[0][:5] == 'Dover' \
                                or row[0][:6] == 'Dracut' or row[0][:6] == 'Dudley' or row[0][:9] == 'Dunstable' or row[0][:7] == 'Duxbury' \
                                or row[0][:6] == 'Easton' or row[0][:9] == 'Edgartown' or row[0][:5] == 'Essex' or row[0][:9] == 'Fairhaven' \
                                or row[0][:8] == 'Falmouth' or row[0][:10] == 'Foxborough' or row[0][:10] == 'Framingham' \
                                or row[0][:10] == 'Georgetown' or row[0][:4] == 'Gill' or row[0][:7] == 'Grafton' or row[0][:9] == 'Granville' \
                                or row[0][:16] == 'Great Barrington' or row[0][:6] == 'Groton' or row[0][:6] == 'Hadley' \
                                or row[0][:8] == 'Hamilton' or row[0][:7] == 'Hampden' or row[0][:7] == 'Hanover' \
                                or row[0][:8] == 'Hardwick' or row[0][:7] == 'Harvard' or row[0][:7] == 'Harwich' or row[0][:8] == 'Hatfield' \
                                or row[0][:5] == 'Heath' or row[0][:7] == 'Hingham' or row[0][:6] == 'Holden' or row[0][:9] == 'Holliston' \
                                or row[0][:8] == 'Hopedale' or row[0][:9] == 'Hopkinton' or row[0][:6] == 'Hudson' or row[0][:4] == 'Hull' \
                                or row[0][:7] == 'Ipswich' or row[0][:8] == 'Kingston' or row[0][:9] == 'Lakeville' \
                                or row[0][:9] == 'Lancaster' or row[0][:3] == 'Lee' or row[0][:9] == 'Leicester' \
                                or row[0][:8] == 'Leverett' or row[0][:9] == 'Lexington' or row[0][:6] == 'Leyden' \
                                or row[0][:7] == 'Lincoln' or row[0][:9] == 'Littleton' or row[0][:6] == 'Ludlow' \
                                or row[0][:9] == 'Lynnfield' or row[0][:21] == 'Manchester-by-the-Sea' or row[0][:9] == 'Mansfield' \
                                or row[0][:10] == 'Marblehead' or row[0][:6] == 'Marion' or row[0][:10] == 'Marshfield' \
                                or row[0][:7] == 'Mashpee' or row[0][:12] == 'Mattapoisett' or row[0][:7] == 'Maynard' \
                                or row[0][:8] == 'Medfield' or row[0][:6] == 'Mendon' or row[0][:8] == 'Merrimac' or \
                                row[0][:13] == 'Middleborough' or row[0][:9] == 'Middleton' or row[0][:6] == 'Milton' or \
                                row[0][:6] == 'Monson' or row[0][:9] == 'Nantucket' or row[0][:6] == 'Natick' or row[0][:7] == 'Needham' \
                                or row[0][:15] == 'New Marlborough' or row[0][:8] == 'Newbury ' or row[0][:7] == 'Norfolk' \
                                or row[0][:13] == 'North Andover' or row[0][:18] == 'North Attleborough' or row[0][:16] == 'North Brookfield' \
                                or row[0][:12] == 'Northborough' or row[0][:11] == 'Northbridge' or row[0][:10] == 'Northfield' \
                                or row[0][:6] == 'Norton' or row[0][:7] == 'Norwell' or row[0][:10] == 'Oak Bluffs' or \
                                row[0][:6] == 'Orange' or row[0][:7] == 'Orleans' or row[0][:6] == 'Oxford' or row[0][:6] == 'Palmer' \
                                or row[0][:6] == 'Pelham' or row[0][:9] == 'Pepperell' or row[0][:9] == 'Petersham' or \
                                row[0][:11] == 'Phillipston' or row[0][:10] == 'Plainville' or row[0][:8] == 'Plymouth' \
                                or row[0][:9] == 'Princeton' or row[0][:12] == 'Provincetown' or row[0][:8] == 'Randolph' \
                                or row[0][:7] == 'Raynham' or row[0][:7] == 'Reading' or row[0][:8] == 'Rehoboth' or \
                                row[0][:9] == 'Rochester' or row[0][:8] == 'Rockland' or row[0][:8] == 'Rockport' or \
                                row[0][:6] == 'Rowley' or row[0][:9] == 'Royalston' or row[0][:7] == 'Russell' or row[0][:7] == 'Rutland' \
                                or row[0][:9] == 'Salisbury' or row[0][:8] == 'Sandwich' or row[0][:8] == 'Scituate' or \
                                row[0][:7] == 'Seekonk' or row[0][:6] == 'Sharon' or row[0][:9] == 'Shelburne' or \
                                row[0][:8] == 'Sherborn' or row[0][:7] == 'Shirley' or row[0][:10] == 'Shrewsbury' or \
                                row[0][:10] == 'Shutesbury' or row[0][:12] == 'South Hadley' or row[0][:11] == 'Southampton' \
                                or row[0][:12] == 'Southborough' or row[0][:9] == 'Southwick' or row[0][:7] == 'Spencer' \
                                or row[0][:11] == 'Stockbridge' or row[0][:9] == 'Stoughton' or row[0][:4] == 'Stow' or \
                                row[0][:7] == 'Sudbury' or row[0][:6] == 'Sutton' or row[0][:10] == 'Swampscott' or \
                                row[0][:7] == 'Swansea' or row[0][:9] == 'Templeton' or row[0][:9] == 'Tewksbury' or \
                                row[0][:7] == 'Tisbury' or row[0][:9] == 'Topsfield' or row[0][:5] == 'Truro' or \
                                row[0][:12] == 'Tyngsborough' or row[0][:5] == 'Upton' or row[0][:8] == 'Uxbridge' or \
                                row[0][:9] == 'Wakefield' or row[0][:7] == 'Walpole' or row[0][:4] == 'Ware' or \
                                row[0][:7] == 'Wareham' or row[0][:7] == 'Warwick' or row[0][:7] == 'Wayland' or \
                                row[0][:7] == 'Wayland' or row[0][:7] == 'Webster' or row[0][:9] == 'Wellesley' or \
                                row[0][:9] == 'Wellfleet' or row[0][:7] == 'Wendell' or row[0][:6] == 'Wenham' or \
                                row[0][:13] == 'West Boylston' or row[0][:15] == 'West Brookfield' or row[0][:12] == 'West Newbury' \
                                or row[0][:12] == 'West Tisbury' or row[0][:11] == 'Westborough' or row[0][:8] == 'Westford' \
                                or row[0][:11] == 'Westhampton' or row[0][:11] == 'Westminster' or row[0][:6] == 'Westport' \
                                or row[0][:7] == 'Whately' or row[0][:9] == 'Wilbraham' or row[0][:12] == 'Williamstown' \
                                or row[0][:10] == 'Wilmington' or row[0][:10] == 'Winchendon' or row[0][:10] == 'Winchester' \
                                or row[0][:8] == 'Wrentham' or row[0][:8] == 'Yarmouth':
                            row[0] = f'Town of {row[0][:-4]}'
                        # Cities
                        elif row[0][:8] == 'Amesbury' or row[0][:9] == 'Attleboro' or row[0][:10] == 'Barnstable' or row[0][:7] == 'Beverly' \
                                or row[0][:6] == 'Boston' or row[0][:8] == 'Brockton' or row[0][:11] == 'Easthampton' or row[0][:9] == 'Fitchburg' \
                                or row[0][:10] == 'Gloucester' or row[0][:10] == 'Greenfield' or row[0][:7] == 'Holyoke' \
                                or row[0][:8] == 'Lawrence' or row[0][:10] == 'Leominster' or row[0][:11] == 'Marlborough' \
                                or row[0][:7] == 'Melrose' or row[0][:7] == 'Methuen' or row[0][:11] == 'New Bedford' or \
                                row[0][:11] == 'Newburyport' or row[0][:6] == 'Newton' or row[0][:11] == 'Northampton' \
                                or row[0][:7] == 'Peabody' or row[0][:10] == 'Pittsfield' or row[0][:6] == 'Quincy' or \
                                row[0][:11] == 'Springfield' or row[0][:7] == 'Taunton' or row[0][:9] == 'Westfield' or \
                                row[0][:8] == 'Weymouth' or row[0][:6] == 'Woburn' or row[0][:9] == 'Worcester':
                            row[0] = f'City of {row[0][:-4]}'
                        elif row[0][:7] == 'Town of':
                            row[0] = row[0][:-4]
                cur.updateRow(row)

    def recode_org_types(field):
        with arcpy.da.UpdateCursor(data, field) as cur:
            for row in cur:
                if row[0] == 'DIST':
                    row[0] = 'LOC'
                elif row[0] == 'JNT':
                    row[0] = 'OTH'
                elif row[0] == 'NGO':
                    row[0] = 'PNP'
                elif row[0] == 'STAT':
                    row[0] = 'STP'
                cur.updateRow(row)
        del row, cur
        print('Recoded org types in {}'.format(field))
    
    def assign_owner():
        with arcpy.da.UpdateCursor(data, ["State", "FeeOwnType", "FeeOwner"]) as cur:
            for row in cur:
                if row[1] == "PVT":
                    row[2] = "Private"
                elif row[1] == "UNK":
                    row[2] = "Unknown"
                elif row[1] == "PNP":
                    row[2] = "Unknown Non-governmental Organization"
                elif row[1] == "LOC":
                    row[2] = "Unknown Local Government"
                elif row[1] == "OTH":
                    row[2] = "Other"
                elif row[1] == "FED":
                    row[2] = "United States of America"
                elif row[1] == "STP":
                    if row[0] == "RI":
                        row[2] = "State of Rhode Island"
                    elif row[0] == "CT":
                        row[2] = "State of Connecticut"
                    elif row[0] == "MA":
                        row[2] = "Commonwealth of Massachusetts"
                    elif row[0] == "VT":
                        row[2] = "State of Vermont"
                    elif row[0] == "NH":
                        row[2] = "State of New Hampshire"
                    elif row[0] == "ME":
                        row[2] = "State of Maine"
                cur.updateRow(row)
    
    def assign_fee_owner_cat():
        with arcpy.da.UpdateCursor(data, ["FeeOwnType", "FeeOwnCat"]) as cur:
            for row in cur:
                if row[0] in ["FED", "STP", "LOC"]:
                    row[1] = "Public"
                elif row[0] == "OTH":
                    row[1] = "Other"
                elif row[0] == "UNK":
                    row[1] = "Unknown"
                else:
                    row[1] = "Private"
                cur.updateRow(row)
        print("Assigned fee owner category...")

    def set_empty_to_null(field):
        with arcpy.da.UpdateCursor(data, field) as cur:
            for row in cur:
                if row[0] == '' or row[0] == ' ':
                    row[0] = None
                    cur.updateRow(row)
        print(f"Set empty values to Null for {field}...")

    def populate_gap():
        arcpy.management.CalculateField(data, 'GapStatus', 'int(!gapcat!)')
        print('Copied GAP values to numeric field')

    def recode_access():
        with arcpy.da.UpdateCursor(data, ['pubaccess', 'PubAccess1']) as cur:
            for row in cur:
                if row[0] == 'OA':
                    row[1] = 'Yes'
                elif row[0] == 'RA':
                    row[1] = 'Limited'
                elif row[0] == 'XA':
                    row[1] = 'No'
                elif row[0] == 'UK':
                    row[1] = 'Unknown'
                cur.updateRow(row)
        del row, cur
        print('Recoded PubAccess')

    def populate_easeyear():
        with arcpy.da.UpdateCursor(data, ["YearProt", "EaseYear"]) as cur:
            for row in cur:
                if row[0] > 0:
                    row[1] = row[0]
                    cur.updateRow(row)
        print("Populated EaseYear...")
    
    def assign_prot_type():
        with arcpy.da.UpdateCursor(data, ["FeeOwnType", "ProtType"]) as cur:
            for row in cur:
                if row[0] in ["FED", "STP", "LOC", "PNP"]:
                    row[1] = "Fee and Ease"
                else:
                    row[1] = "Ease"
                cur.updateRow(row)
        print("Populated ProtType...")

    def delete_fields():
        deletes = ['security', 's_emthd2', 'purpose', 'term', 'mon_est', 'day_est', 'rep_acres', 'gis_acres', 'pct_diff',
                   'iucncat', 'dataagg', 'dataentry', 'datasrc', 'source_uid', 'conflict', 'stacked', 'comments',
                   'eholduid1', 'eholduid2', 'eholduid3', 'report_href', 'county', 'created_user', 'created_date',
                   'last_edited_user', 'last_edited_date', 'gapcat', 'pubaccess', 'unique_id']
        arcpy.management.DeleteField(data, deletes)
        print('Deleted fields')

        # Now that pubaccess is deleted, we can alter PubAccess1 to be PubAccess
        arcpy.management.AlterField(data, 'PubAccess1', 'PubAccess', 'PubAccess')

    try:
        add_alter_fields()
        populate_state()
        recode_org_types('FeeOwnType')
        assign_owner()
        assign_fee_owner_cat()
        recode_org_types('IntHolder1Type')
        recode_names()
        set_empty_to_null("IntHolder2")
        assign_prot_type()
        populate_gap()
        recode_access()
        populate_easeyear()
    except Exception:
        print(traceback.format_exc())
    else:
        print('Successfully assigned values! Now deleting fields...')
        delete_fields()  # Only delete fields if the 'try' block is successful
    finally:
        # Regardless of whether 'try' and 'else' blocks are successful, print elapsed time
        end_time = time.time()
        duration_in_minutes = round((end_time - start_time) / 60.0, 2)
        if duration_in_minutes > 60.0:
            duration_in_hours = round(duration_in_minutes / 60.0, 2)
            print('Time elapsed: {} hours'.format(duration_in_hours))
        else:
            print('Time elapsed: {} minutes'.format(duration_in_minutes))


def prep_padus(data):

    def add_alter_fields():
        arcpy.management.AlterField(data, "State_Nm", "State", "State")
        arcpy.management.AlterField(data, 'Unit_Nm', 'AreaName', 'AreaName')
        arcpy.management.AlterField(data, 'Loc_Own', 'FeeOwner', 'FeeOwner')
        arcpy.management.AlterField(data, 'Own_Type', 'FeeOwnType', 'FeeOwnType')
        arcpy.management.AddField(data, "FeeOwnCat", "TEXT", field_length = 7)
        arcpy.management.AlterField(data, 'Category', 'ProtType', 'ProtType')
        arcpy.management.AlterField(data, 'EsmtHldr', 'IntHolder1', 'IntHolder1')
        arcpy.management.AlterField(data, 'EHoldTyp', 'IntHolder1Type', 'IntHolder1Type')
        arcpy.management.AddField(data, "IntHolder2", "TEXT", field_length = 150)
        arcpy.management.AddField(data, "IntHolder2Type", "TEXT", field_length = 3)
        arcpy.management.AddField(data, 'YearProt', 'SHORT')
        arcpy.management.AddField(data, "FeeYear", "SHORT")
        arcpy.management.AddField(data, "EaseYear", "SHORT")
        arcpy.management.AddField(data, 'GapStatus', 'SHORT')
        arcpy.management.AlterField(data, 'Pub_Access', 'PubAccess', 'PubAccess')
        arcpy.management.AlterField(data, 'Duration', 'ProtDuration', 'ProtDuration')
        arcpy.management.AlterField(data, "Agg_Src", "Source", "Source")
        arcpy.management.AddField(data, "UID", "TEXT", field_length = 25)
        arcpy.management.CalculateField(data, "UID", "!OBJECTID!")
        print("Added and altered fields...")

    # Because these are altered fields with values already present, we just
    # need to recode the values that don't align with NEPOS schema
    def recode_org_types(field):
        with arcpy.da.UpdateCursor(data, field) as cur:
            for row in cur:
                if row[0] == 'DIST':
                    row[0] = 'LOC'
                elif row[0] == 'JNT':
                    row[0] = 'OTH'
                elif row[0] == 'NGO':
                    row[0] = 'PNP'
                elif row[0] == 'STAT':
                    row[0] = 'STP'
                elif row[0] == 'TRIB':
                    row[0] = 'TRB'
                cur.updateRow(row)
        del row, cur
        print('Recoded org types in {}'.format(field))
    
    def assign_fee_own_cat():
        with arcpy.da.UpdateCursor(data, ["FeeOwnType", "FeeOwnCat"]) as cur:
            for row in cur:
                if row[0] in ["FED", "STP", "LOC"]:
                    row[1] = "Public"
                elif row[0] in ["PNP", "PVT"]:
                    row[1] = "Private"
                elif row[0] == "OTH":
                    row[1] = "Other"
                elif row[0] == "TRB":
                    row[1] = "Tribal"
                else:
                    row[1] = "Unknown"
                cur.updateRow(row)

    def recode_access():
        with arcpy.da.UpdateCursor(data, 'PubAccess') as cur:
            for row in cur:
                if row[0] == 'OA':
                    row[0] = 'Yes'
                elif row[0] == 'RA':
                    row[0] = 'Limited'
                elif row[0] == 'XA':
                    row[0] = 'No'
                elif row[0] == 'UK':
                    row[0] = 'Unknown'
                cur.updateRow(row)
        del row, cur
        print('Recoded PubAccess')

    def populate_gap():
        arcpy.management.CalculateField(data, 'GapStatus', 'int(!GAP_Sts!)')
        print('Copied GAP values to numeric field')

    def recode_prot_type():
        with arcpy.da.UpdateCursor(data, 'ProtType') as cur:
            for row in cur:
                if row[0] == 'Easement':
                    row[0] = 'Ease'
                cur.updateRow(row)
        del row, cur
        print('Recoded ProtType')

    def populate_year_prot():
        with arcpy.da.UpdateCursor(data, "Date_Est") as cur:
            for row in cur:
                if row[0] == '' or row[0] == ' ' or row[0] is None:
                    row[0] = '0'
                    cur.updateRow(row)

        arcpy.management.CalculateField(data, 'YearProt', 'int(!Date_Est!)')
        print('Copied date established values to numeric field')
    
    def assign_fee_ease_year():
        with arcpy.da.UpdateCursor(data, ["ProtType", "YearProt", "FeeYear", "EaseYear"]) as cur:
            for row in cur:
                if row[0] == "Ease" and row[1] > 0:
                    row[3] = row[1]
                elif row[0] == "Fee" and row[1] > 0:
                    row[2] = row[1]
                cur.updateRow(row)
        print("Assigned FeeYear and EaseYear...")

    def recode_duration():
        with arcpy.da.UpdateCursor(data, 'ProtDuration') as cur:
            for row in cur:
                if row[0] is None:
                    row[0] = 'UNK'
                    cur.updateRow(row)
        print('Set Null ProtDuration to UNK')
    
    def delete_fields():
        deletes = ["Own_Name", "Mang_Type", "Mang_Name", "Loc_Mang", "Des_Tp", "Loc_Ds",
                   "Loc_Nm", "GIS_Src", "Src_Date", "GIS_Acres", "Source_PAID", "WDPA_Cd",
                   "Access_Src", "Access_Dt", "GAP_Sts", "GAPCdSrc", "GAPCdDt", "IUCN_Cat",
                   "IUCNCtSrc", "IUCNCtDt", "Date_Est", "Comments", "AreaRptd", "Conflict", "BoundCF",
                   "Security", "Stacked", "CoHeld", "Term", "NCED_UID", "FeatClass"]
        arcpy.management.DeleteField(data, deletes)
        print("Deleted fields...")

    try:
        add_alter_fields()
        recode_org_types('FeeOwnType')
        assign_fee_own_cat()
        recode_org_types('IntHolder1Type')
        recode_prot_type()
        populate_gap()
        recode_access()
        populate_year_prot()
        assign_fee_ease_year()
        recode_duration()
    except Exception:
        print(traceback.format_exc())
    else:
        delete_fields()
    finally:
        # Regardless of whether 'try' and 'else' blocks are successful, print elapsed time
        end_time = time.time()
        duration_in_minutes = round((end_time - start_time) / 60.0, 2)
        if duration_in_minutes > 60.0:
            duration_in_hours = round(duration_in_minutes / 60.0, 2)
            print('Time elapsed: {} hours'.format(duration_in_hours))
        else:
            print('Time elapsed: {} minutes'.format(duration_in_minutes))


# Before running this script, project to NEPOS CRS!
def prep_maine(data):

    def add_alter_fields():
        arcpy.management.AddField(data, 'UID', 'TEXT', field_length=25)
        arcpy.management.CalculateField(data, 'UID', 'str(!CL_UNIQUEI!)') # Note this field varies depending on whether FGDB or SHP
        arcpy.management.AddField(data, 'State', 'TEXT', field_length=2)
        arcpy.management.CalculateField(data, 'State', "'ME'")
        arcpy.management.AlterField(data, 'PROJECT', 'AreaName', 'AreaName')
        arcpy.management.AddField(data, 'FeeOwner', 'TEXT', field_length=150)
        arcpy.management.AddField(data, 'FeeOwnType', 'TEXT', field_length=3)
        arcpy.management.AddField(data, 'FeeOwnCat', 'TEXT', field_length=7)
        arcpy.management.AddField(data, 'FeeOwnCatComments', 'TEXT', field_length=200)
        arcpy.management.AddField(data, 'ProtType', 'TEXT', field_length=15)
        arcpy.management.AddField(data, 'ProtTypeComments', 'TEXT', field_length=200)
        arcpy.management.AddField(data, 'IntHolder1', 'TEXT', field_length=150)
        arcpy.management.AddField(data, 'IntHolder1Type', 'TEXT', field_length=3)
        arcpy.management.AddField(data, 'IntHolder2', 'TEXT', field_length=150)
        arcpy.management.AddField(data, 'IntHolder2Type', 'TEXT', field_length=3)
        arcpy.management.AddField(data, 'YearProt', 'SHORT')
        arcpy.management.AddField(data, 'FeeYear', 'SHORT')
        arcpy.management.AddField(data, 'EaseYear', 'SHORT')
        arcpy.management.AddField(data, 'WildYear', 'SHORT')
        arcpy.management.AddField(data, 'GapStatus', 'SHORT')
        arcpy.management.AlterField(data, 'PUB_ACCESS', 'PubAccess', 'PubAccess')
        arcpy.management.AddField(data, 'ProtDuration', 'TEXT', field_length=4)
        print('Added and altered fields...')

    # NOTE: For 2024/2025 update, different code was used that didn't allow for retrieving easement types to
    # populate ProtTypeComments. Code has been updated but not tested for the next update. It should work...
    def assign_prot_type():
        # List of all potential easement types present in the data
        easements = ['Conservation Easement', 'Third Party Easement', 'Public Access Easement', 'Access Easement', 'Easement Enforcer']

        # Now that CONS1_TYPE and CONS2_TYPE are a little cleaner, we use both to populate ProtType
        with arcpy.da.UpdateCursor(data, ['CONS1_TYPE', 'CONS2_TYPE', 'ProtType', 'ProtTypeComments']) as cur:
            for row in cur:
                # Use both CONS1_TYPE and CONS2_TYPE to determine if a PA is fee, ease, or both
                if row[0] == 'Fee' and row[1] in easements:
                    row[2] = 'Fee and Ease'
                    row[3] = f'Easement is {row[1]}'
                elif row[0] == 'Fee' and row[1] not in easements:
                    row[2] = 'Fee'
                elif row[0] in easements and row[1] == 'Fee':
                    row[2] = 'Fee and Ease'
                    row[3] = f'Easement is {row[0]}'
                elif row[0] in easements and row[1] in easements:
                    row[2] = 'Ease'
                    row[3] = f'IntHolder1 holds {row[0]}, IntHolder2 holds {row[1]}'
                elif row[0] in easements and row[1] != 'Fee':
                    row[2] = 'Ease'
                    row[3] = f'Easement is {row[0]}'
                # For remaining protection types, we just consult CONS1_TYPE
                elif row[0] == 'Lease':
                    row[2] = 'Lease'
                elif row[0] == 'Management Transfer Agreement' or row[0] == 'Project Agreement' or row[0] == 'Restricted' or row[0] == 'Other':
                    row[2] = 'Other'
                elif row[0] == 'Unknown':
                    row[2] = 'Unknown'
                cur.updateRow(row)
        del row, cur
        print('Populated ProtType!')

    # This code should correct Water District lands which are classified as OTH in MEGIS data but in ours should be LOC
    def assign_fee_owner_name_type():
        with arcpy.da.UpdateCursor(data, ['CONS1_TYPE', 'HOLD1_NAME', 'HOLD1_TYPE',
                                          'CONS2_TYPE', 'HOLD2_NAME', 'HOLD2_TYPE', 
                                          'FeeOwner', 'FeeOwnType', 'FeeOwnCatComments']) as cur:
            for row in cur:
                # If CONS1_TYPE is fee and CONS2_TYPE is not, FeeOwner = HOLD1_NAME and FeeOwnType = HOLD1_TYPE
                if row[0] == 'Fee' and row[3] != 'Fee':
                    row[6] = row[1]
                    row[7] = row[2]
                # And vice versa
                elif row[3] == 'Fee' and row[0] != 'Fee':
                    row[6] = row[4]
                    row[7] = row[5]
                # If CONS1_TYPE and CONS2_TYPE both = fee, we compare the names to see if they are different
                elif row[0] == 'Fee' and row[3] == 'Fee':
                    # If HOLD1_NAME and HOLD2_NAME are the same, we just use one
                    if row[1] == row[4]:
                        row[6] = row[1]
                        row[7] = row[2]
                    # If HOLD1_NAME is not Null and HOLD2_NAME is Null, we use HOLD1_NAME and HOLD1_TYPE
                    elif row[1] is not None and row[4] is None:
                        row[6] = row[1]
                        row[7] = row[2]
                    # And vice versa
                    elif row[1] is None and row[4] is not None:
                        row[6] = row[4]
                        row[7] = row[5]
                    elif row[1] != row[4]:     # If they are different, we combine them
                        row[6] = row[1] + ' & ' + row[4]
                        if row[2] == row[5]:   # And check to see if the two owners are the same type
                            row[7] = row[2]    # If so, it doesn't matter whether HOLD1_TYPE or HOLD2_TYPE is used
                        elif row[2] is not None and row[5] is not None and row[2] != row[5]:
                            row[7] = 'OTH'     # If not, we mark as OTH
                # If neither CONS1_TYPE nor CONS2_TYPE are fee, we assume a private fee owner
                else:
                    row[6] = 'Private'
                    row[7] = 'PVT'
                    row[8] = 'Private ownership assumed due to absence of fee protection'
                cur.updateRow(row)
        del row, cur
        print('Populated FeeOwner and FeeOwnType...')

    def assign_fee_own_cat():
        with arcpy.da.UpdateCursor(data, ['FeeOwnType', 'FeeOwnCat', 'FeeOwnCatComments']) as cur:
            for row in cur:
                if row[0] in ['LOC', 'STP', 'FED']:
                    row[1] = 'Public'
                elif row[0] in ['PNP', 'PVT']:
                    row[1] = 'Private'
                elif row[0] == 'OTH':
                    row[1] = 'Other'
                elif row[0] == 'UNK':
                    row[1] = 'Unknown'
                cur.updateRow(row)
        print('Populated FeeOwnCat...')
                

    # Recode HOLD1_TYPE and HOLD2_TYPE in situ to make populating IntHolder1Type and IntHolder2Type simpler
    # arg fields should be a list of two fields such as [HOLD1_TYPE, HOLD1_NAME] or [HOLD2_TYPE, HOLD2_NAME]
    # Name field is needed to distinguish between PNP and PVT private holders
    def recode_holder_type(fields):
        with arcpy.da.UpdateCursor(data, fields) as cur:
            for row in cur:
                if row[0] == 'Federal':
                    row[0] = 'FED'
                elif row[0] == 'Municipal':
                    row[0] = 'LOC'
                elif row[0] == 'Other':
                    row[0] = 'OTH'
                elif row[0] == 'State':
                    row[0] = 'STP'
                # If HOLD1_TYPE is Private, these are mostly PNP except for a few so we parse based on name
                # This is why we include the name fields in the cursor
                # Before each update, interest holder names should be checked and any other PVT names should be added
                elif row[0] == 'Private':
                    if (row[1] != 'Unknown' and row[1] is not None and 
                        row[1] != "Granite Falls Homeowners' Association" and row[1] != 'GNGLL' and 
                        row[1] != 'Higgins Beach Association' and row[1] != 'Small Point Association'):
                        row[0] = 'PNP'
                    else:
                        row[0] = 'PVT'
                # If HOLD1_TYPE is empty, set to UNK
                elif fields[0] == 'HOLD1_TYPE' and row[0] == ' ':
                    row[0] = 'UNK'
                cur.updateRow(row)
        del row, cur
        print('Recoded interest holder types...')

    # This code is not complete and should be added to to better handle all combinations of CONS1_TYPE and CONS2_TYPE
    # NOTE: There are some instances where ProtType is Unknown and there is an interest holder... perhaps
    def assign_int_holder_name_type():
        with arcpy.da.UpdateCursor(data, ['CONS1_TYPE', 'HOLD1_NAME', 'HOLD1_TYPE',
                                          'CONS2_TYPE', 'HOLD2_NAME', 'HOLD2_TYPE',
                                          'IntHolder1', 'IntHolder1Type', 'IntHolder2', 'IntHolder2Type']) as cur:
            for row in cur:
                # If CONS1_TYPE is not fee and there is something in HOLD1_NAME, IntHolder1 = HOLD1_NAME and IntHolder1Type = HOLD1_TYPE
                if row[0] != 'Fee' and row[1] is not None and row[1] != ' ' and row[1] != '':
                    row[6] = row[1]
                    row[7] = row[2]
                    # If CONS2_TYPE is also not fee or unknown, we populate IntHolder2 and IntHolder2Type from HOLD2
                    if row[3] != 'Fee' and row[3] is not None and row[3] != 'Forever Wild' and row[3] != 'Other':
                        row[8] = row[4]
                        row[9] = row[5]
                # If CONS1_TYPE is Fee and CONS2_TYPE is Easement, IntHolder1 = HOLD2_NAME and IntHolder1Type = HOLD2_TYPE
                # Note that we are only taking Easement Cons type for populating interest holder here
                # Because if a PA is protected in fee, we don't care about a non-easement interest such as an MTA
                elif row[0] == 'Fee' and row[3] == 'Easement':
                    row[6] = row[4]
                    row[7] = row[5]
                cur.updateRow(row)
        del row, cur
        print('Populated IntHolder1 and IntHolder2 names and types!')

    # Populating YearProt as a new field because ACQ_YEAR is a text field
    # All dates look good except for a few redundant no data values
    def assign_year_prot():
        with arcpy.da.UpdateCursor(data, ['ACQ_YEAR', 'YearProt']) as cur:
            for row in cur:
                if row[0] is None or row[0] == '' or row[0] == ' ' or row[0] == '1801':
                    row[1] = 0
                else:
                    row[1] = int(row[0])
                cur.updateRow(row)
        del row, cur
        print('Populated YearProt!')

    def assign_gap_status():
        with arcpy.da.UpdateCursor(data, ['GAP_STATUS', 'GapStatus']) as cur:
            for row in cur:
                # Unknown values get set to GAP of 0
                if row[0] is None or row[0] == ' ' or row[0][0] == '9':
                    row[1] = 0
                elif row[0][0] == '1':
                    row[1] = 1
                elif row[0][0] == '2':
                    row[1] = 2
                elif row[0][0:2] == '39':
                    row[1] = 39
                elif row[0][0] == '3':
                    row[1] = 3
                elif row[0][0] == '4':
                    row[1] = 4
                cur.updateRow(row)
        del row, cur
        print('Populated GapStatus!')

    # Public access is 'Limited' if permission is need or access is for members only
    # Public access being generally allowed, even if only on trails, is considered to allow public access
    def recode_pub_access():
        with arcpy.da.UpdateCursor(data, 'PubAccess') as cur:
            for row in cur:
                if row[0] is None:
                    row[0] = 'Unknown'
                elif row[0][:7] == 'Allowed' or row[0] == 'Water access' or row[0] == 'Restricted to trail area only' \
                        or row[0] == 'Restricted: access restricted to certain geographic areas':
                    row[0] = 'Yes'
                elif row[0][:11].lower() == 'not allowed' or row[0] == 'No public access':
                    row[0] = 'No'
                elif row[0][-19:] == 'permission required':
                    row[0] = 'Limited'
                else:
                    row[0] = 'Unknown'
                cur.updateRow(row)
        del row, cur
        print('Recoded PubAccess!')

    def assign_duration():
        with arcpy.da.UpdateCursor(data, ['GapStatus', 'ProtDuration']) as cur:
            for row in cur:
                if 1 <= row[0] <= 3 or row[0] == 39:
                    row[1] = 'PERM'
                else:
                    row[1] = 'UNK'
                cur.updateRow(row)
        del row, cur
        print('Populated ProtDuration!')

    def assign_fee_ease_year():
        with arcpy.da.UpdateCursor(data, ['ProtType', 'YearProt', 'FeeYear', 'EaseYear']) as cur:
            for row in cur:
                if row[0] == 'Fee' and row[1] > 0:
                    row[2] = row[1]
                elif row[0] == 'Ease' and row[1] > 0:
                    row[3] = row[1]
                cur.updateRow(row)
        print('Populated FeeYear and EaseYear...')

    # type is either 'gdb' or 'shp' reflecting file type
    def delete_fields(type):
        # For GDB Version
        gdb_fields = ['PARCEL_NAME', 'DESIGNATION', 'CONS1_TYPE', 'HOLD1_NAME', 'HOLD1_TYPE', 'CONS2_TYPE', 'HOLD2_NAME',
                  'HOLD2_TYPE', 'ECO_RESERVE', 'ACQ_YEAR', 'ACQ_DATE', 'RPT_AC', 'PURPOSE1', 'PURPOSE2', 'EDITOR',
                  'BPL_ID', 'IFW_ID', 'LMF_ID', 'DEPT_ID', 'FMPROCSS', 'FMUPDDAT', 'NOTE_', 'CALC_AC', 'REVIEW',
                  'GAP_STATUS', 'IUCN_STATUS', 'GlobalID', 'TAX_MAP_TOWN', 'TAX_MAP_BOOK_LOT', 'CL_UNIQUEID']
        # For shapefile version
        shp_fields = ['PARCEL_NAM', 'DESIGNATIO', 'CONS1_TYPE', 'HOLD1_NAME', 'HOLD1_TYPE', 'CONS2_TYPE', 'HOLD2_NAME',
                  'HOLD2_TYPE', 'ECO_RESERV', 'ACQ_YEAR', 'ACQ_DATE', 'RPT_AC', 'PURPOSE1', 'PURPOSE2', 'EDITOR',
                  'BPL_ID', 'IFW_ID', 'LMF_ID', 'DEPT_ID', 'FMPROCSS', 'FMUPDDAT', 'NOTE_', 'CALC_AC', 'REVIEW',
                  'GAP_STATUS', 'IUCN_STATU', 'GlobalID', 'TAX_MAP_TO', 'TAX_MAP_BO', 'CL_UNIQUEI']
        
        if type == 'gdb':
            fields = gdb_fields
        elif type == 'shp':
            fields = shp_fields

        arcpy.management.DeleteField(data, fields)
        print('Deleted fields no longer needed')

    try:
        add_alter_fields()
        # assign_prot_type runs before owner/int holder functions b/c it cleans CONS1_TYPE and CONS2_TYPE
        assign_prot_type()
        # recode_holder_type runs before owner/int holder names b/c it cleans HOLD1_TYPE and HOLD2_TYPE
        recode_holder_type(['HOLD1_TYPE', 'HOLD1_NAME'])
        recode_holder_type(['HOLD2_TYPE', 'HOLD2_NAME'])
        # With cons types and holder types preprocessed a bit, can populate owner/int holder names and types
        assign_fee_owner_name_type()
        assign_fee_own_cat()
        assign_int_holder_name_type()
        assign_year_prot()
        assign_gap_status()
        recode_pub_access()
        assign_duration()
        assign_fee_ease_year()
    except Exception:
        print(traceback.format_exc())
    else:
        delete_fields()
    finally:
        print_elapsed_time()


# Rhode Island local
# This code doesn't work well with the IntHolderType fields... RI Local has two int holder fields (name) but
# only one field for int holder type. There was a lot of misclassified IntHolder1 names as a result of this
# In the future, this code should probably be updated to classify int holders by name...
# This code could be improved when identifying "fee and ease" PAs -- there is pretty often no Int Holder but
# EOwnTyp is populated. Perhaps 'Fee and Ease' should not be defined at this stage and should be done later in NEPOS
# Also, it might be better for this dataset to NOT calculate 'Fee and Ease' here, because the protection type
# can be linked to the year. E.g., if RI Local PROTYP is FEE, we can reasonably assume the associated year is the fee year
# This is especialy useful since RI Local and RI State are separate, and we can glean info on FeeYear and EaseYear
# from the respective sources based on the protection type in each
def prep_ri_local(data):
    # The existing fee owner field is too short to handle some of the names we use so need to add as a new field
    def add_alter_fields():
        arcpy.management.AddField(data, "State", "TEXT", field_length=2)
        arcpy.management.CalculateField(data, "State", "'RI'")
        arcpy.management.AlterField(data, "Site", "AreaName", "AreaName")
        arcpy.management.AddField(data, "FeeOwner", "TEXT", field_length=150)
        arcpy.management.CalculateField(data, "FeeOwner", "!Fee_Own!")
        arcpy.management.AlterField(data, "FOwnTyp", "FeeOwnType", "FeeOwnType")
        arcpy.management.AddField(data, "FeeOwnCat", "TEXT", field_length=7)
        arcpy.management.AddField(data, "IntHolder1", "TEXT", field_length=150)
        arcpy.management.CalculateField(data, "IntHolder1", "!Eas_Own_1!")
        arcpy.management.AlterField(data, "EOwnTyp", "IntHolder1Type", "IntHolder1Type")
        arcpy.management.AddField(data, "IntHolder2", "TEXT", field_length=150)
        arcpy.management.CalculateField(data, "IntHolder2", "!Eas_Own_2!")
        arcpy.management.AddField(data, "IntHolder2Type", "TEXT", field_length=3)
        arcpy.management.AddField(data, "ProtType", "TEXT", field_length=15)
        arcpy.management.CalculateField(data, "ProtType", "!PROTYP!")
        arcpy.management.AddField(data, "ProtTypeComments", "TEXT", field_length=200)
        arcpy.management.AddField(data, "YearProt", "SHORT")
        arcpy.management.AddField(data, "FeeYear", "SHORT")
        arcpy.management.AddField(data, "EaseYear", "SHORT")
        arcpy.management.AlterField(data, "PUBACC", "PubAccess", "PubAccess")
        arcpy.management.AddField(data, "GapStatus", "SHORT")
        arcpy.management.AddField(data, "ProtDuration", "TEXT", field_length=5)
        arcpy.management.AddField(data, "UID", "TEXT", field_length=10)
        arcpy.management.CalculateField(data, "UID", "str(!OBJECTID!)")   # No unique ID field so have to use OBJECTID
        print('Added / altered / calculated fields...')

    def recode_year():
        with arcpy.da.UpdateCursor(data, ["P_Year", "YearProt"]) as cur:
            for row in cur:
                if row[0] is None:
                    row[1] = 0
                elif row[0].upper() == 'UNK' or row[0] == ' ' or row[0] == '' or row[0] == 'NF':
                    row[1] = 0
                else:
                    row[1] = int(row[0])
                cur.updateRow(row)
        print('Populated YearProt...')

    def recode_access():
        with arcpy.da.UpdateCursor(data, "PubAccess") as cur:
            for row in cur:
                if row[0] is None:
                    row[0] = 'Unknown'
                elif row[0] == 'LIM':
                    row[0] = 'Limited'
                elif row[0] == 'NO':
                    row[0] = 'No'
                elif row[0] == 'YES':
                    row[0] = 'Yes'
                cur.updateRow(row)
        print('Updated PubAccess...')

    # For use with both FeeOwnType and IntHolder1Type
    # Only need to update the values that don't align with our schema since altering an existing field
    def recode_org_types(field):
        with arcpy.da.UpdateCursor(data, field) as cur:
            for row in cur:
                # For NA or null values, we only set to UNK for FeeOwnType
                # For IntHolder1Type these are set to null
                if (row[0] == 'NA' or row[0] is None) and field == 'FeeOwnType':
                    row[0] = 'UNK'
                elif (row[0] == 'NA' or row[0] is None) and field == 'IntHolder1Type':
                    row[0] = None
                elif row[0] == 'LTR' or row[0] == 'NGO':
                    row[0] = 'PNP'
                elif row[0] == 'PRV':
                    row[0] = 'PVT'
                elif row[0] == 'STA':
                    row[0] = 'STP'
                # Most UTL owners are local (districts) but some may need correction
                elif row[0] == 'UTL' or row[0] == 'MUN':
                    row[0] = 'LOC'
                cur.updateRow(row)
        print('Updated {}...'.format(field))
    
    def assign_fee_own_cat():
        with arcpy.da.UpdateCursor(data, ['FeeOwnType', 'FeeOwnCat']) as cur:
            for row in cur:
                if row[0] in ['LOC', 'STP', 'FED']:
                    row[1] = 'Public'
                elif row[0] in ['PVT', 'PNP', 'PFP']:
                    row[1] = 'Private'
                else:
                    row[1] = 'Unknown'
                cur.updateRow(row)
        print('Assigned FeeOwnCat...')

    # For use with FeeOwner, IntHolder1, and IntHolder2
    # Just doing the federal, state, major private owners
    def recode_names(field):
        with arcpy.da.UpdateCursor(data, field) as cur:
            for row in cur:
                # Various no data values for IntHolder1 and IntHolder2 set to null
                if row[0] is None and field[:3] == 'Int':
                    row[0] = None
                elif row[0] is None and field == 'FeeOwner':
                    row[0] = 'Unknown'
                elif (row[0] == ' ' or row[0] == 'NA' or row[0] == 'NF' or
                        row[0].lower() == 'none' or row[0] == 'N/A') and field[:3] == 'Int':
                    row[0] = None
                # And 'unknown' for FeeOwner
                elif (row[0] == ' ' or row[0] == 'NA' or row[0] == 'NF') and field == 'FeeOwner':
                    row[0] = 'Unknown'
                # Federal
                elif row[0] == 'United States of America (NPS)' or row[0] == 'United States National Park Service':
                    row[0] = 'US DOI - National Park Service'
                elif row[0] == 'United States DOI Federal Lands to Parks':
                    row[0] = 'US Department of the Interior'
                elif row[0] == 'United States of America (USFWS)' or row[0] == 'United States Fish & Wildlife Service':
                    row[0] = 'US DOI - Fish and Wildlife Service'
                elif (row[0] == 'NRCS' or row[0] == 'Natural Resources Conservation Service' 
                      or row[0] == 'Natural Resource Conservation Service'):
                    row[0] = 'USDA - Natural Resources Conservation Service'
                elif row[0] == 'United States of America (Army Corps)':
                    row[0] = 'US DOD - Army Corps of Engineers'
                elif row[0] == 'United States Department of Agriculture' or row[0] == 'USDA':
                    row[0] = 'US Department of Agriculture'
                elif row[0] == 'NOAA CELCP Deed Restriction':
                    row[0] = 'NOAA - Coastal and Estuarine Land Conservation Program'
                # State
                elif row[0] == 'State of Rhode Island (RIDEM)' or row[0] == 'RIDEM':
                    row[0] = 'RI Department of Environmental Management'
                elif row[0] == 'CRMC' or row[0] == 'Coastal Resources Management Council':
                    row[0] = 'RI Coastal Resources Management Council'
                elif row[0] == 'State of Rhode Island (ALPC)':
                    row[0] = 'RI Agricultural Lands Preservation Commission'
                elif row[0] == 'State of Rhode Island (RIDOT)':
                    row[0] = 'RI Department of Transportation'
                # Private
                elif row[0] == 'TNC':
                    row[0] = 'The Nature Conservancy'
                cur.updateRow(row)
            print('Updated org names...')

    # IntHolder2Type needs to be assigned based on IntHolder2 name because this field is not provided in RI Local
    def assign_intholder2_type():
        with arcpy.da.UpdateCursor(data, ["IntHolder2", "IntHolder2Type"]) as cur:
            for row in cur:
                if row[0] is None:
                    row[1] = None
                elif (row[0][-10:] == 'Land Trust' or row[0][-11:] == 'Conservancy' or row[0][:7] == 'Audubon' 
                      or row[0] == 'Little Compton Ag. Conservancy Trust'):
                    row[1] = 'PNP'
                elif row[0][:2] == 'RI' or row[0] == 'State of Rhode Island':
                    row[1] = 'STP'
                elif row[0][:7] == 'Town of' or row[0][-18:] == 'Water Supply Board':
                    row[1] = 'LOC'
                elif row[0][:2] == 'US':
                    row[1] = 'FED'
                elif row[0] == 'ProvWater':
                    row[1] = 'PFP'
                cur.updateRow(row)
        print('Assigned IntHolder2Type...')

    # NOTE: The values in this field are not particularly useful... there seems to be no logic
    # Here we just consolidate values in alignment with NEPOS schema
    def recode_prot_type():
        with arcpy.da.UpdateCursor(data, "ProtType") as cur:
            for row in cur:
                # Only 1 row has PUB and it's clearly fee - think this value is a mistake
                if row[0] == 'CIN' or row[0] == 'FEE' or row[0] == 'PUB':
                    row[0] = 'Fee'
                elif row[0] == 'DRS':
                    row[0] = 'DR'
                elif row[0] == 'EAS':
                    row[0] = 'Ease'
                elif row[0] is None:
                    row[0] = 'Unknown'
                cur.updateRow(row)
        print('Updated ProtType...')
    
    def assign_prot_type_comments():
        with arcpy.da.UpdateCursor(data, ["SubDivisionOS", "ProtTypeComments"]) as cur:
            for row in cur:
                if row[0] == 'Y':
                    row[1] = 'Subdivision OS'
                    cur.updateRow(row)
        print('Populated ProtTypeComments for subdivision OS')
    
    # This function should be run after recode_prot_type() but before assign_fee_ease()
    # Since we are assuming that if ProtType = 'Fee', the year goes with fee even if an IntHolder is also present
    def assign_fee_ease_year():
        with arcpy.da.UpdateCursor(data, ['YearProt', 'ProtType', 'FeeYear', 'EaseYear']) as cur:
            for row in cur:
                if row[0] > 0 and row[1] == 'Fee':
                    row[2] = row[0]
                elif row[0] > 0 and row[1] == 'Ease':
                    row[3] = row[0]
                cur.updateRow(row)
        print('Assigned FeeYear and EaseYear...')

    # Because the values in PROTYPE are not useful, we need to check FeeOwnType, IntHolder1Type, and ProtType
    # to determine whether something is fee and easement
    # IMPORTANT: This code should be run AFTER assigning FeeYear and EaseYear, because we assign these based
    # on the original ProtType
    def assign_fee_ease():
        with arcpy.da.UpdateCursor(data, ["FeeOwnType", "IntHolder1Type", "ProtType"]) as cur:
            for row in cur:
                # If the protection type if Fee but there is a PNP or public interest holder,
                # set to Fee and Ease
                if row[2] == 'Fee' and row[1] in ['PNP', 'LOC', 'STP', 'FED']:
                    row[2] = 'Fee and Ease'
                # Same but in reverse
                elif row[2] == 'Ease' and row[0] in ['LOC', 'STP', 'FED', 'PNP']:
                    row[2] = 'Fee and Ease'
                cur.updateRow(row)
        print('Assigned Fee and Ease...')
    
    # GapStatus and ProtDuration are both unknown since these are not in the data
    def assign_gap_and_duration():
        arcpy.management.CalculateField(data, 'GapStatus', "0")
        arcpy.management.CalculateField(data, 'ProtDuration', "'UNK'")
        print('Populated unknown fields...')

    def delete_fields():
        deletes = ["DEM_ID", "GIS_Acre", "P_Year", "Com_Name", "EASTYP", "PURP", "DATSRC", "InStaCon",
                   "SubDivisionOS", "ParcQC", "Fee_Own", "Eas_Own_1", "Eas_Own_2", "PROTYP"]
        arcpy.management.DeleteField(data, deletes)
        print('Deleted fields')

    try:
        add_alter_fields()
        recode_year()
        recode_access()
        recode_names('FeeOwner')
        recode_names('IntHolder1')
        recode_names('IntHolder2')
        recode_org_types('FeeOwnType')
        assign_fee_own_cat()
        recode_org_types('IntHolder1Type')
        assign_intholder2_type()
        recode_prot_type()        # Recode existing PROTYPE values
        assign_prot_type_comments()
        assign_fee_ease_year()    # Use these to assign FeeYear and EaseYear
        assign_fee_ease()         # Check for rows that should be 'Fee and Ease'
        assign_gap_and_duration()
    except Exception:
        print(traceback.format_exc())
    else:
        print('Successfully updated fields! Deleting fields...')
        delete_fields()
    finally:
        print_elapsed_time()

# Rhode Island state
def prep_ri_state(data):
    def add_alter_fields():
        arcpy.management.AddField(data, "State", "TEXT", field_length=2)
        arcpy.management.CalculateField(data, "State", "'RI'")
        arcpy.management.AlterField(data, "DEM_AREA", "AreaName", "AreaName")
        arcpy.management.AddField(data, "FeeOwner", "TEXT", field_length=150)
        arcpy.management.AddField(data, "FeeOwnType", "TEXT", field_length=3)
        arcpy.management.AddField(data, "FeeOwnCat", "TEXT", field_length=7)
        arcpy.management.AddField(data, "IntHolder1", "TEXT", field_length=150)
        arcpy.management.AddField(data, "IntHolder1Type", "TEXT", field_length=3)
        # Although IntHolder2 won't be populated, we add it to match NEPOS structure since
        # this field has a Source_ field associated with it. This is important for the append_rows.py
        # script and the AddRows tool
        arcpy.management.AddField(data, "IntHolder2", "TEXT", field_length=150)
        arcpy.management.AlterField(data, "ACQ_TYPE", "ProtType", "ProtType")
        arcpy.management.AddField(data, "ProtTypeComments", "TEXT", field_length=200)
        arcpy.management.AlterField(data, "Pub_Access", "PubAccess", "PubAccess")
        arcpy.management.AddField(data, "YearProt", "SHORT")
        arcpy.management.AddField(data, "FeeYear", "SHORT")
        arcpy.management.AddField(data, "EaseYear", "SHORT")
        arcpy.management.AddField(data, 'GapStatus', 'SHORT')
        arcpy.management.AddField(data, 'ProtDuration', 'TEXT', field_length=5)
        arcpy.management.AddField(data, "UID", "TEXT", field_length=10)
        arcpy.management.CalculateField(data, "UID", "!OBJECTID!")
        print('Added and altered fields...')

    def assign_year():
        with arcpy.da.UpdateCursor(data, ["Year", "YearProt"]) as cur:
            for row in cur:
                if row[0] is None:
                    row[1] = 0
                else:
                    row[1] = int(row[0])
                cur.updateRow(row)
        print('Populated YearProt!')

    def recode_access():
        with arcpy.da.UpdateCursor(data, "PubAccess") as cur:
            for row in cur:
                if row[0] is None:
                    row[0] = 'Unknown'
                elif row[0] == 'YES':
                    row[0] = 'Yes'
                elif row[0] == 'NO':
                    row[0] = 'No'
                elif row[0] == 'LIM':
                    row[0] = 'Limited'
                cur.updateRow(row)
        print('Recoded PubAccess...')

    # RI state distinguishes between conservation and recreation easements which we can note in ProtTypeComments
    # Also, farm preservation can be distinguished based on the PrimUse field
    # Only need to update values that don't already match NEPOS scheme (e.g., not Lease or Other)
    def recode_prot_type():
        with arcpy.da.UpdateCursor(data, ["ProtType", "ProtTypeComments", "PrimUse"]) as cur:
            for row in cur:
                if row[0] is None:
                    row[0] = 'Unknown'
                elif row[0] == 'Conservation Easement':
                    row[0] = 'Ease'
                    row[1] = 'Easement is CE'
                elif row[0] == 'Recreation Easement':
                    row[0] = 'Ease'
                    row[1] = 'Easement is Recreation Easement'
                elif row[0] == 'Deed to Development Rights':
                    row[0] = 'Ease'
                    if row[2] == 'Agricultural Land Preservation':
                        row[1] = 'Deed to Development Rights - Agricultural Land Preservation'
                    else:
                        row[1] = 'Deed to Development Rights'
                elif row[0] == 'Fee Title':
                    row[0] = 'Fee'
                elif row[0] == 'Deed Restriction':
                    row[0] = 'DR'
                cur.updateRow(row)
        print('Recoded ProtType...')
    
    def assign_fee_ease_year():
        with arcpy.da.UpdateCursor(data, ['YearProt', 'ProtType', 'FeeYear', 'EaseYear']) as cur:
            for row in cur:
                if row[0] > 0 and row[1] == 'Fee':
                    row[2] = row[0]
                elif row[0] > 0 and row[1] == 'Ease':
                    row[3] = row[0]
                cur.updateRow(row)
        print('Assigned FeeYear and EaseYear...')

    def assign_names_types():
        with arcpy.da.UpdateCursor(data, ["ProtType", "FeeOwner", "FeeOwnType", "FeeOwnCat", "IntHolder1", "IntHolder1Type"]) as cur:
            for row in cur:
                if row[0] == 'Fee':
                    row[1] = 'RI Department of Environmental Management'
                    row[2] = 'STP'
                    row[3] = 'Public'
                elif row[0] == 'Ease' or row[0] == 'DR' or row[0] == 'Lease' or row[0] == 'Other':
                    row[4] = 'RI Department of Environmental Management'
                    row[5] = 'STP'
                    row[1] = 'Unknown'   # All FeeOwner fields are Unknown
                    row[2] = 'UNK'
                    row[3] = 'Unknown'
                cur.updateRow(row)
        print('Assigned FeeOwner and IntHolder1 based on ProtType...')
    
    # GAP status is unknown since this information is not provided - we generally get this from TNC where available
    def assign_gap():
        arcpy.management.CalculateField(data, 'GapStatus', "0")
        print('Assigned GapStatus...')

    # ProtDuration is also unknown, and this information is gotten from TNC as well (based on GapStatus)
    def assign_duration():
        arcpy.management.CalculateField(data, 'ProtDuration', "'UNK'")
        print('Assigned ProtDuration...')

    def delete_fields():
        deletes = ["DEM_DIV", "NAME", "HUNTING", "FISHING", "Poly_Src", "Acres", "BondFund", "NPS_6f",
                   "PrimUse", "ManArea", "DEM_Managed", "DeedAcres", "DEM_ID", "Year"]
        arcpy.management.DeleteField(data, deletes)

    try:
        add_alter_fields()
        assign_year()
        recode_access()
        recode_prot_type()
        assign_fee_ease_year()
        assign_names_types()
        assign_gap()
        assign_duration()
    except Exception:
        print(traceback.format_exc())
    else:
        print('Successfully updated fields! Deleting unneeded columns...')
        delete_fields()
    finally:
        print_elapsed_time()


# New Hampshire
def prep_nh(data):
    def add_alter_fields():
        arcpy.management.AddField(data, "State", "TEXT", field_length=2)
        arcpy.management.CalculateField(data, "State", "'NH'")
        arcpy.management.AlterField(data, "name", "AreaName", "AreaName")
        arcpy.management.AddField(data, "FeeOwner", "TEXT", field_length=150)
        arcpy.management.AddField(data, "FeeOwnType", "TEXT", field_length=3)
        arcpy.management.AddField(data, "FeeOwnCat", "TEXT", field_length=7)
        arcpy.management.AddField(data, "FeeOwnCatComments", "TEXT", field_length=200)
        arcpy.management.AddField(data, "ProtType", "TEXT", field_length=15)
        arcpy.management.AddField(data, "ProtTypeComments", "TEXT", field_length=200)
        arcpy.management.AddField(data, "IntHolder1", "TEXT", field_length=150)
        arcpy.management.AddField(data, "IntHolder1Type", "TEXT", field_length=3)
        arcpy.management.AddField(data, "IntHolder2", "TEXT", field_length=150)
        arcpy.management.AddField(data, "IntHolder2Type", "TEXT", field_length=3)
        arcpy.management.AddField(data, "YearProt", "SHORT")
        arcpy.management.AddField(data, "YearProtComments", "TEXT", field_length=200)
        arcpy.management.AddField(data, "EaseYear", "SHORT")
        arcpy.management.AddField(data, "FeeYear", "SHORT")
        arcpy.management.AddField(data, "GapStatus", "SHORT")
        arcpy.management.AddField(data, "PubAccess", "TEXT", field_length=7)
        arcpy.management.AddField(data, "ProtDuration", "TEXT", field_length=5)
        # The TID field is not a true UID field so need to use OBJECTID
        arcpy.management.AddField(data, "UID", "TEXT", field_length=20)
        arcpy.management.CalculateField(data, "UID", "!OBJECTID!")
        print('Added and altered fields...')

    # Populating YearProt with the earliest year of protection available
    # YearProtComments is populated if there are two different dates provided
    def assign_year_prot():
        # First we need to clean up DATEREC1 and DATEREC2 - check data for missing or bad values
        with arcpy.da.UpdateCursor(data, ["daterec1", "daterec2"]) as cur:
            for row in cur:
                if row[0] == ' ':
                    row[0] = '0'
                if row[0] == ' 2021':
                    row[0] = '2021'
                if row[1] == ' ':
                    row[1] = '0'
                cur.updateRow(row)
        print('Cleaned up DATEREC1 and DATEREC2...')

        # Then we can use those columns to populate YearProt
        with arcpy.da.UpdateCursor(data, ["daterec1", "daterec2", "YearProt", "YearProtComments"]) as cur:
            for row in cur:
                # If both DATEREC fields are 0, YearProt is also 0
                if row[0] == '0' and row[1] == '0':
                    row[2] = 0
                # If either DATEREC field is 0, we take year from the one that isn't zero
                elif row[0] == '0' and row[1] != '0':
                    row[2] = int(row[1][:4])
                elif row[0] != '0' and row[1] == '0':
                    row[2] = int(row[0][:4])
                # If there are two dates, we compare them to see if they are the same
                elif row[0] != '0' and row[1] != '0':
                    # If they are the same, we populate YearProt and make a note
                    if row[0][:4] == row[1][:4]:
                        row[2] = int(row[0][:4])
                        row[3] = 'Two dates provided, same year'
                    # If they are different, we take the earlier one and make a note about the other date
                    elif row[0][:4] != row[1][:4]:
                        year1 = int(row[0][:4])
                        year2 = int(row[1][:4])
                        if year1 < year2:
                            row[2] = year1
                            row[3] = f'Second date provided - {year2}'
                        elif year2 < year1:
                            row[2] = year2
                            row[3] = f'Second date provided - {year1}'
                cur.updateRow(row)
        print('Populated YearProt and YearProtComments...')

    # For ProtDuration, we can use the LEVEL_ field and PPTERMTYPE
    # When LEVEL_ = 1 this is permanently protected - querying of data found that many level 1 polygons have a
    # PPTERMTYPE of U (unknown). It seems like LEVEL_ is more general and includes land trust owned land,
    # land owned by state agencies that have a conservation focus, etc. whereas PPTERMTYPE is specific to PPTYPE
    # and is mostly unknown values
    def recode_prot_duration():
        with arcpy.da.UpdateCursor(data, ['pptermtype', 'level_', 'ProtDuration']) as cur:
            for row in cur:
                if row[0] == 'P' or row[1] == '1':
                    row[2] = 'PERM'
                elif row[0] == 'L':
                    row[2] = 'TEMP'
                elif row[0] == 'U':
                    row[2] = 'UNK'
                cur.updateRow(row)
        print('Assigned ProtDuration...')

    def recode_access():
        with arcpy.da.UpdateCursor(data, ['access', 'PubAccess']) as cur:
            for row in cur:
                if row[0] == 1:
                    row[1] = 'Yes'
                elif row[0] == 2:
                    row[1] = 'Limited'
                elif row[0] == 3:
                    row[1] = 'No'
                elif row[0] == 4 or row[0] == 5:
                    row[1] = 'Unknown'
                cur.updateRow(row)
        print('Assigned PubAccess...')

    # NH has a unique code called '3A' that is like a "quasi-three" rating but there is no legal protection, so
    # we consider it GAP 4
    def assign_gap():
        with arcpy.da.UpdateCursor(data, ['gap_status', 'GapStatus']) as cur:
            for row in cur:
                if row[0] == '1':
                    row[1] = 1
                elif row[0] == '2':
                    row[1] = 2
                elif row[0] == '3':
                    row[1] = 3
                elif row[0] == '3A' or row[0] == '4':
                    row[1] = 4
                elif row[0] == '9':
                    row[1] = 0
                else:
                    row[1] = 0
                cur.updateRow(row)
        print('Assigned GAP...')

    # NH owner code 4 is "other public/quasi-public entity" which can't be classified into one of our owner types
    # but based on the org names in the metadata, seems to be school, water, and fire districts primarily, so we
    # consider is LOC. For rows where a protection type = FO, we will update FeeOwnType to match the fee owner
    # in the assign_owner function. But not all rows have FO as a protection type and our only fee owner
    # info comes from the OWNERTYPE field. So this gets run first, then assign_owner afterwards.
    def assign_owner_type():
        with arcpy.da.UpdateCursor(data, ['ownertype', 'FeeOwnType']) as cur:
            for row in cur:
                if row[0] == 1 or row[0] == 6 or row[0] == 4:
                    row[1] = 'LOC'
                elif row[0] == 2:
                    row[1] = 'FED'
                elif row[0] == 3:
                    row[1] = 'STP'
                elif row[0] == 5:
                    row[1] = 'PVT'     # Lots of these are PNP...
                elif row[0] == 9:
                    row[1] = 'UNK'
                cur.updateRow(row)
        print('Populated FeeOwnType...')

    def assign_owner_category():
        with arcpy.da.UpdateCursor(data, ['FeeOwnType', 'FeeOwnCat']) as cur:
            for row in cur:
                if row[0] in ['LOC', 'STP', 'FED']:
                    row[1] = 'Public'
                elif row[0] in ['PNP', 'PFP', 'PVT']:
                    row[1] = 'Private'
                elif row[0] == 'UNK':
                    row[1] = 'UNK'
                cur.updateRow(row)
        print('Populated FeeOwnCat...')

    # Prior to creating this function, I used the AttributeCodes.xls spreadsheet that comes with the NH data
    # and processed it a bit to match our formatting of state agency names and to combine PPAGENCY and SPAGENCY
    # codes/names into one spreadsheet. That CSV is then joined to the NH data and used to populate owner/holder
    # names.
    def join_names():
        # CSV of PPAGENCY and SPAGENCY names from AttributeCodes.xls, with some reformatted to match NEPOS schema
        names = 'D:/Lee/POS/Update_2023/Data/New_Hampshire_Conservation_Public_Lands/ppagency_spagency_names.csv'
        # Join CSV data of names to the NH data by code
        # Renaming the column after each join to reflect what column it goes with
        arcpy.management.JoinField(data, 'ppagency', names, 'CODE', 'DESC')
        arcpy.management.AlterField(data, 'DESC', 'PPAGENCY_NAME', 'PPAGENCY_NAME')
        arcpy.management.JoinField(data, 'spagency1', names, 'CODE', 'DESC')
        arcpy.management.AlterField(data, 'DESC', 'SPAGENCY1_NAME', 'SPAGENCY1_NAME')
        arcpy.management.JoinField(data, 'spagency2', names, 'CODE', 'DESC')
        arcpy.management.AlterField(data, 'DESC', 'SPAGENCY2_NAME', 'SPAGENCY2_NAME')
        print('Joined names for PPAGENCY, SPAGENCY1, and SPAGENCY2...')

    # To assign owner names, we use all AGENCY and TYPE fields
    # After checking fields, it was confirmed that there were no instances were SPTYPE2 was populated and SPTYPE1
    # was empty, and that 'FO' (fee owned) protection type is present in all protection type fields.
    # Also checked to make sure there are no instances where both SPTYPE1 and SPTYPE2 were FO -- this could change over time
    # Therefore, to populate FeeOwner, we check these combinations of the three protection type fields:
    # - where only PPTYPE is populated and is FO
    # - where both PPTYPE and SPTYPE1 are FO (joint fee owner situation)
    # - where only SPTYPE1 is FO
    # - where only SPTYPE2 is FO
    # - where no rows are FO
    # This function also assigns FeeOwnType again the same way type is assigned to IntHolders
    # This is the easiest way to get more specificity in the private group
    def assign_owner():
        # Use the prepared CSV of org names and codes (from AttributesCodes.xls) to create a dictionary
        # where name is the key and type is the value. See Conservation Lands Standards for more info.
        names = pd.read_csv("D:/Lee/POS/Update_2023/Data/New_Hampshire_Conservation_Public_Lands/ppagency_spagency_names.csv")
        names = names.astype({"CODE": "str"})  # Convert the codes from integer to string
        names['CODE'] = names['CODE'].str.zfill(5)  # Pad with zero on the left so all are length of 5

        # Function to assign type based on the first digit in the organization's code
        def label_type(r):
            if r['CODE'][0] == '0' or r['CODE'][0] == '1' or r['CODE'][0] == '4':
                return 'LOC'
            elif r['CODE'][0] == '2':
                return 'FED'
            elif r['CODE'][0] == '3':
                return 'STP'
            # Most 5 codes are PNP orgs except for a few
            elif r['CODE'][0] == '5':
                if r['CODE'] == '59999' or r['CODE'] == '52900' or r['CODE'] == '50410' or r['CODE'] == '51220' or \
                        r['CODE'] == '51623':
                    return 'PVT'
                else:
                    return 'PNP'

        names['TYPE'] = names.apply(label_type, axis=1)  # Create TYPE column by applying function to all rows

        # Create a dictionary of names (key) and types (value)
        name_type = dict(zip(names.DESC, names.TYPE))

        with arcpy.da.UpdateCursor(data, ['pptype', 'PPAGENCY_NAME', 'sptype1', 'SPAGENCY1_NAME', 'sptype2',
                                          'SPAGENCY2_NAME', 'FeeOwner', 'FeeOwnType', 'FeeOwnCatComments']) as cur:
            for row in cur:
                if row[0] == 'FO' and row[2] == 'FO':
                    # Sometimes SPTYPE1 is populated but SPAGENCY1 is not -- check for this
                    if row[3] is not None:
                        row[6] = f'{row[1]} & {row[3]}'   # Type needs to be checked manually for these rows
                        owner1type = name_type[row[1]]
                        owner2type = name_type[row[3]]
                        if owner1type == owner2type:
                            row[7] = owner1type
                        else:
                            row[7] = 'OTH'
                            row[8] = 'Jointly owned by different owner types'
                    else:
                        row[6] = row[1]
                        row[7] = name_type[row[1]]
                elif row[0] == 'FO':
                    row[6] = row[1]
                    row[7] = name_type[row[1]]
                elif row[0] != 'FO' and row[2] == 'FO' and row[3] is not None:
                    row[6] = row[3]
                    row[7] = name_type[row[3]]
                elif row[0] != 'FO' and row[2] != 'FO' and row[4] == 'FO' and row[5] is not None:
                    row[6] = row[5]
                    row[7] = name_type[row[5]]
                else:
                    if row[7] == 'PVT':     # owner type function should be called first so we can use it to assign private
                        row[6] = 'Private'
                    else:
                        row[6] = 'Unspecified'    # Don't update FeeOwnType here! We'll just use the value from OWNER_TYPE in previous function
                cur.updateRow(row)
        print('Populated FeeOwner...')


    # Protection type is a bit confusing because there are three fields and a dozen values
    # 'Fee and Ease' is only set when one of the fields is FO and the other is one of the easement fields
    # ProtType needs to be consistent with IntHolder fields -- e.g., if NH says there is FO and DR protection,
    # we call that DR because we have the DR interest holder in the data. If we called it FO, it would seem like
    # the interest holder is a mistake. So we assign prot type and int holders in the same function.
    # For future use, it's helpful to run this function and then check rows where ProtType IS NULL
    # These are rows that were not captured in the below conditionals -- you can see what the values are in the 
    # original data and add code to handle those cases to the function
    def assign_prot_type_int_holders():
        # First, set empty rows to Null - check data to make sure there are no new empty values!
        # As of writing this code, there are no empty values in PPTYPE but this should be checked before every update
        with arcpy.da.UpdateCursor(data, ['sptype1', 'sptype2']) as cur:
            for row in cur:
                if row[0] == ' ':
                    row[0] = None
                if row[1] == ' ':
                    row[1] = None
                cur.updateRow(row)
        print('Set empty SPTYPE rows to Null...')

        # All possible NH protection type values that are easements - except AR which we want to note separately
        # so we can note that it's an APR in ProtTypeComments
        easements = ['CE', 'FE', 'HP', 'PE', 'SE']

        with arcpy.da.UpdateCursor(data, ['pptype', 'sptype1', 'sptype2', 'ProtType', 'ProtTypeComments',
                                          'SPAGENCY1_NAME', 'SPAGENCY2_NAME', 'PPAGENCY_NAME',
                                          'IntHolder1', 'IntHolder2']) as cur:
            for row in cur:
                # First handle rows where only PPTYPE is populated - this is vast majority of rows!
                if row[1] is None and row[2] is None:
                    if row[0] == 'FO':
                        row[3] = 'Fee'
                    else:
                        if row[0] in easements:
                            row[3] = 'Ease'
                            row[8] = row[7]
                        elif row[0] == 'DR':
                            row[3] = 'DR'
                            row[8] = row[7]
                        elif row[0] == 'LE':
                            row[3] = 'Lease'
                            row[8] = row[7]
                        elif row[0] == 'RV' or row[0] == 'EI':
                            row[3] = 'Other'
                            row[8] = row[7]
                        elif row[0] == 'SA':
                            row[3] = 'Other'
                            row[4] = 'Set aside OS area of development'
                            row[8] = row[7]
                        elif row[0] == 'RW':
                            row[3] = 'ROW'
                            row[8] = row[7]
                        elif row[0] == 'AR':
                            row[3] = 'Ease'
                            row[4] = 'Easement is an APR'
                            row[8] = row[7]
                        # If only PPTYPE is populated, and it is a non-fee interest there are sometimes instances where SPAGENCY1 is populated
                        # even though SPTYPE1 is not -- in this case, we want to take the extra int holder (likely co-holder)
                        if row[5] is not None:
                            row[9] = row[5]
                # Then rows where just PPTYPE = Fee and only SPTYPE1 is populated
                elif row[0] == 'FO' and row[1] is not None and row[2] is None:
                    if row[1] in easements and row[5] is not None:
                        row[3] = 'Fee and Ease'
                        row[8] = row[5]   # IntHolder1 is SPAGENCY1
                    elif row[1] == 'AR' and row[5] is not None:
                        row[3] = 'Fee and Ease'
                        row[4] = 'Easement is an APR'
                        row[8] = row[5]
                    elif row[1] == 'DR' and row[5] is not None:
                        row[3] = 'DR'
                        row[8] = row[5]
                    elif row[1] == 'LE' and row[5] is not None:
                        row[3] = 'Fee'
                        row[8] = row[5]
                        row[4] = 'IntHolder1 holds a lease but PPTYPE is Fee'
                    else:
                        row[3] = 'Fee'
                # Then rows where PPTYPE != Fee and only SPTYPE1 is populated
                elif row[0] != 'FO' and row[1] is not None and row[2] is None:
                    if row[0] in easements and row[1] == 'FO':
                        row[3] = 'Fee and Ease'
                        row[8] = row[7]    # IntHolder1 is PPAGENCY
                    elif row[0] in easements and row[1] == 'AR':
                        row[3] = 'Ease'
                        row[8] = row[7]
                        row[4] = f'Also subject to APR held by {row[5]}'
                    elif row[0] in easements and row[1] == 'DR':
                        row[3] = 'Ease'
                        row[8] = row[7]
                        row[4] = f'Also subject to DR held by {row[5]}'
                    elif row[0] in easements and row[1] in easements:
                        row[3] = 'Ease'
                        if row[0] == row[1]:  # If they are the same kind of easement this suggests jointly held
                            row[8] = row[7]   # IntHolder1 is PPAGENCY
                            row[9] = row[5]   # IntHolder2 is SPAGENCY1
                        else:                 # If not, we note the second interest and holder in ProtTypeComments
                            row[8] = row[7]
                            row[4] = f'Easement is {row[0]}, also subject to {row[1]} held by {row[5]}'
                    elif row[0] in easements:   # Rows where PPTYPE is an easement and SPTYPE1 is a value we don't care
                        row[3] = 'Ease'         # about (EI, RV, etc.) will execute here
                        row[8] = row[7]
                    elif row[0] == 'AR' and row[1] in easements:
                        row[3] = 'Ease'
                        row[8] = row[7]
                        row[4] = f'Easement is an APR, also subject to {row[1]} held by {row[5]}'
                    # If in future data there are more combinations where AR is PPTYPE, they should be added
                    # before the next conditional
                    elif row[0] == 'AR':
                        row[3] = 'Ease'
                        row[8] = row[7]
                        row[4] = 'Easement is an APR'
                    elif row[0] == 'DR' and row[1] not in easements and row[1] != 'AR':
                        row[3] = 'DR'
                        row[8] = row[7]    # IntHolder1 is PPAGENCY
                        if row[1] == 'DR':     # If PPTYPE and SPTYPE1 are both DR, we populate IntHolder2 with SPAGENCY1
                            row[9] = row[6]
                    elif row[0] == 'DR' and row[1] in easements:
                        row[3] = 'DR'
                        row[8] = row[7]
                        row[4] = f'Also subject to {row[1]} held by {row[5]}'
                    elif row[0] == 'DR':
                        row[3] = 'DR'
                        row[8] = row[7]
                    elif row[0] == 'RW':
                        row[3] = 'ROW'
                        row[8] = row[7]
                    elif row[0] == 'RV':
                        row[3] = 'Other'
                        row[8] = row[7]
                    elif row[0] == 'SA':    # There are some values in SPTYPE1 but they don't have SPAGENCY1 associated...
                        row[3] = 'Other'
                        row[8] = row[7]
                        row[4] = 'Set aside OS area of development'
                # Then rows where all three protection types are populated
                # As of writing this code this is only ~300 rows -- look at the values in the data to see
                # what combinations need to be handled!
                else:
                    # These are values present in this subset of rows for SPTYPE1 and SPTYPE2 that we don't care about
                    # RW, SA could also be added to this list if present in future data
                    dont_care = ['RV', 'EI']
                    # PPTYPE is an easement, SPTYPE1 and SPTYPE2 are values we don't care about
                    if row[0] in easements and row[1] in dont_care and row[2] in dont_care:
                        row[3] = 'Ease'
                        row[8] = row[7]
                    # PPTYPE is an easement, SPTYPE1 we don't care, SPTYPE2 is FO
                    elif row[0] in easements and row[1] in dont_care and row[2] == 'FO':
                        row[3] = 'Fee and Ease'
                        row[8] = row[7]
                    # PPTYPE is an easement, SPTYPE1 is FO, SPTYPE2 we don't care
                    elif row[0] in easements and row[1] == 'FO' and row[2] in dont_care:
                        row[3] = 'Fee and Ease'
                        row[8] = row[7]
                    # PPTYPE is an easement, SPTYPE1 is FO, SPTYPE2 is DR
                    elif row[0] in easements and row[1] == 'FO' and row[2] == 'DR':
                        row[3] = 'Fee and Ease'
                        row[8] = row[7]
                        row[4] = f'Also subject to DR held by {row[6]}'
                    # PPTYPE is an easement, SPTYPE1 we don't care, SPTYPE2 is DR
                    elif row[0] in easements and row[1] in dont_care and row[2] == 'DR':
                        row[3] = 'Ease'
                        row[8] = row[7]
                        row[4] = f'Also subject to DR held by {row[6]}'
                    # PPTYPE is an easement, SPTYPE1 is DR, SPTYPE2 we don't care
                    elif row[0] in easements and row[1] == 'DR' and row[2] in dont_care:
                        row[3] = 'Ease'
                        row[8] = row[7]
                        row[4] = f'Also subject to DR held by {row[5]}'
                    # PPTYPE is an easement, SPTYPE1 is an easement, SPTYPE2 we don't care
                    elif row[0] in easements and row[1] in easements and row[2] in dont_care:
                        row[3] = 'Ease'
                        row[8] = row[7]        # IntHolder1 is PPAGENCY
                        if row[0] == row[1]:   # If PPTYPE and SPTYPE1 are the same,
                            row[9] = row[5]    # we populate IntHolder2 with SPAGENCY1
                        else:                  # Otherwise we make a note in ProtTypeComments
                            row[4] = f'Easement is {row[0]}, also subject to {row[1]} held by {row[5]}'
                    # PPTYPE is an easement, SPTYPE1 is and easement, SPTYPE2 is DR
                    elif row[0] in easements and row[1] in easements and row[2] == 'DR':
                        row[3] = 'Ease'
                        row[8] = row[7]
                        if row[0] == row[1]:
                            row[9] = row[5]
                            row[4] = f'Also subject to DR held by {row[6]}'
                        else:
                            row[4] = f'Easement is {row[0]}, also subject to {row[1]} held by {row[5]} and DR held by {row[6]}'
                    # PPTYPE is an easement, SPTYPE1 is AR, SPTYPE2 is an easement
                    elif row[0] in easements and row[1] == 'AR' and row[2] in easements:
                        row[3] = 'Ease'
                        row[8] = row[7]
                        if row[0] == row[2]:
                            row[9] = row[6]
                            row[4] = f'Also subject to APR held by {row[5]}'
                        else:
                            row[4] = f'Easement is {row[0]}, also subject to {row[2]} held by {row[6]} and APR held by {row[5]}'
                    # PPTYPE is FO, SPTYPE1 and SPTYPE2 are both easements
                    elif row[0] == 'FO' and row[1] in easements and row[2] in easements:
                        row[3] = 'Fee and Ease'
                        row[8] = row[5]
                        if row[1] == row[2]:
                            row[9] = row[6]
                        else:
                            row[4] = f'Easement is {row[1]}, also subject to {row[2]} held by {row[6]}'
                    # PPTYPE is FO, SPTYPE1 is an easement, SPTYPE2 is DR
                    elif row[0] == 'FO' and row[1] in easements and row[2] == 'DR':
                        row[3] = 'Fee and Ease'
                        row[8] = row[5]
                        row[4] = f'Also subject to DR held by {row[6]}'
                    # PPTYPE is FO, SPTYPE1 is an easement, SPTYPE2 we don't care
                    elif row[0] == 'FO' and row[1] in easements and row[2] in dont_care:
                        row[3] = 'Fee and Ease'
                        row[8] = row[5]
                    # PPTYPE is FO, SPTYPE1 is DR, SPTYPE2 is an easement
                    elif row[0] == 'FO' and row[1] == 'DR' and row[2] in easements:
                        row[3] = 'Fee and Ease'
                        row[8] = row[6]
                        row[4] = f'Also subject to DR held by {row[5]}'
                    # PPTYPE is FO, SPTYPE1 is DR, SPTYPE2 we don't care
                    elif row[0] == 'FO' and row[1] == 'DR' and row[2] in dont_care:
                        row[3] = 'DR'
                        row[8] = row[5]
                    # PPTYPE is FO, SPTYPE1 and SPTYPE2 are both DR
                    elif row[0] == 'FO' and row[1] == 'DR' and row[2] == 'DR':
                        row[3] = 'DR'
                        row[8] = row[5]
                        row[9] = row[6]
                    # PPTYPE is FO, SPTYPE1 is FO, SPTYPE2 is DR
                    elif row[0] == 'FO' and row[1] == 'FO' and row[2] == 'DR':
                        row[3] = 'DR'
                        row[8] = row[6]
                    # PPTYPE is FO, SPTYPE1 and SPTYPE2 we don't care
                    elif row[0] == 'FO' and row[1] in dont_care and row[2] in dont_care:
                        row[3] = 'Fee'
                    # PPTYPE is FO, SPTYPE1 we don't care, SPTYPE2 is an easement
                    elif row[0] == 'FO' and row[1] in dont_care and row[2] in easements:
                        row[3] = 'Fee and Ease'
                        row[8] = row[6]
                    # PPTYPE is an easement, SPTYPE1 we  don't care, SPTYPE2 is AR
                    elif row[0] in easements and row[1] in dont_care and row[2] == 'AR':
                        row[3] = 'Ease'
                        row[8] = row[7]   # IntHolder1 is PPAGENCY
                        row[4] = f'Also subject to APR held by {row[6]}'
                cur.updateRow(row)
        print('Assigned ProtType and IntHolders...')

    def assign_int_holder_type():
        # The issue with the name codes is that in the NH GIS data and AttributeCodes.xls, some municipal codes have 4 digits
        # and others have 5. In the Conservation Lands Standards (metadata) all municipal codes are 5 digits, with some
        # padded on the left with a zero. So to join to the GIS data, we need to leave the codes as they come in the
        # spreadsheet. But to use the first digit of the codes to assign a type, we need to pad the municipal codes with 0.
        # We will do that here, then create a dictionary where name is the key and type is the value, so we can
        # use the name that is populated as IntHolder1 or IntHolder2 to get the type from the dictionary
        names = pd.read_csv(
            "D:/Lee/POS/Update_2023/Data/New_Hampshire_Conservation_Public_Lands/ppagency_spagency_names.csv")
        names = names.astype({"CODE": "str"})  # Convert the codes from integer to string
        names['CODE'] = names['CODE'].str.zfill(5)  # Pad with zero on the left so all are length of 5

        # Function to assign type based on the first digit in the organization's code
        def label_type(r):
            if r['CODE'][0] == '0' or r['CODE'][0] == '1' or r['CODE'][0] == '4':
                return 'LOC'
            elif r['CODE'][0] == '2':
                return 'FED'
            elif r['CODE'][0] == '3':
                return 'STP'
            # Most 5 codes are PNP orgs except for a few - this should be checked periodically in the metadata
            # to make sure there are no more new PVT orgs
            elif r['CODE'][0] == '5':
                if r['CODE'] == '59999' or r['CODE'] == '52900' or r['CODE'] == '50410' or r['CODE'] == '51220' or \
                        r['CODE'] == '51623':
                    return 'PVT'
                else:
                    return 'PNP'

        names['TYPE'] = names.apply(label_type, axis=1)  # Create TYPE column by applying function to all rows

        # Create a dictionary of names (key) and types (value)
        name_type = dict(zip(names.DESC, names.TYPE))

        with arcpy.da.UpdateCursor(data, ['IntHolder1', 'IntHolder1Type'], 'IntHolder1 IS NOT NULL') as cur:
            for row in cur:
                row[1] = name_type[row[0]]
                cur.updateRow(row)

        with arcpy.da.UpdateCursor(data, ['IntHolder2', 'IntHolder2Type'], 'IntHolder2 IS NOT NULL') as cur:
            for row in cur:
                row[1] = name_type[row[0]]
                cur.updateRow(row)
        print('Updated IntHolder types...')

    def populate_fee_ease_year():
        with arcpy.da.UpdateCursor(data, ['YearProt', 'ProtType', 'FeeYear', 'EaseYear']) as cur:
            for row in cur:
                if row[0] == 0:
                    continue
                elif row[0] > 0 and row[1] == 'Fee':
                    row[2] = row[0]
                elif row[0] > 0 and row[1] == 'Ease':
                    row[3] = row[0]
                cur.updateRow(row)
        print("Populated FeeYear and EaseYear")


    def delete_fields():
        deletes = ['OBJECTID', 'TID', 'NAMEALT', 'P_NAME', 'P_NAMEALT', 'PPTYPE', 'PPTERMTYPE', 'PPTERM', 'SPTYPE1',
                   'SPTYPE2', 'PPAGENCY', 'PPAGENTYPE', 'SPAGENCY1', 'SPAGENCY2', 'RSIZE', 'CSIZE', 'P_RSIZE', 'P_CSIZE',
                   'PROGRAM', 'LEVEL_', 'MSTATUS', 'SOURCE', 'ACCURACY', 'COBKPG', 'DATEREC1', 'DATEREC2', 'ACCESS',
                   'DATEADDED', 'DATEALTER', 'NOTES1', 'NOTES2', 'NOTES3', 'NOTES4', 'PID', 'GAP_STATUS', 'OWNERTYPE',
                   'PPAGENCY_NAME', 'SPAGENCY1_NAME', 'SPAGENCY2_NAME']
        
        # In 2025, field names in NH data were changed to all lowercase spelling
        # Create a second version of fields to delete that has this change -- in case this changes again in the future back
        # to uppercase, simply swap out the value in arcpy.manamgement.DeleteField
        keep_uppercase = ['PPAGENCY_NAME', 'SPAGENCY1_NAME', 'SPAGENCY2_NAME']  # Names joined from CSV (still uppercase)
        deletes_lowercase = [x.lower() for x in deletes if x not in keep_uppercase]
        deletes_v2 = deletes_lowercase + keep_uppercase   # Combine the lowercase field names from NH and uppercase field names from CSV
        
        # Update the second argument below if need to go back to all uppercase fields
        arcpy.management.DeleteField(data, deletes_v2)
        print('Deleted fields...')

    try:
        add_alter_fields()
        assign_year_prot()
        recode_prot_duration()
        recode_access()
        assign_gap()
        assign_owner_type()
        join_names()
        assign_owner()
        assign_owner_category()
        assign_prot_type_int_holders()
        assign_int_holder_type()
        populate_fee_ease_year()
    except Exception:
        print(traceback.format_exc())
    else:
        print('Successfully preprocessed data, deleting fields...')
        delete_fields()
    finally:
        print_elapsed_time()


# Vermont has many domains - we export the FC to the sources.gdb WITHOUT domains. Values have to be recoded based on
# domain codes anyway, so it is more difficult to work with the data when the true values are hidden behind descriptions.
# Also, without the domains it is easier to preprocess data (e.g., correct empty values that shouldn't be in there anyway).
# Some fields that have domains (e.g., GAP, PUBACCESS) are easier to work with without the domain descriptions because the codes
# are short and few in number. Other fields (e.g., PAGENCY fields) we have done additional work to prepare the agency names
# (see prep_vt_attribute_codes.R) and we can use the codes to join our custom names to the data.
def prep_vt(data):
    def add_alter_fields():
        arcpy.management.AddField(data, "State", "TEXT", field_length=2)
        arcpy.management.CalculateField(data, "State", "'VT'")
        arcpy.management.AlterField(data, "NAME", "AreaName", "AreaName")
        arcpy.management.AddField(data, "FeeOwner", "TEXT", field_length=150)
        arcpy.management.AddField(data, "FeeOwnType", "TEXT", field_length=3)
        arcpy.management.AddField(data, "FeeOwnCat", "TEXT", field_length=7)
        arcpy.management.AddField(data, "FeeOwnCatComments", "TEXT", field_length=200)
        arcpy.management.AddField(data, "ProtType", "TEXT", field_length=15)
        arcpy.management.AddField(data, "ProtTypeComments", "TEXT", field_length=200)
        arcpy.management.AddField(data, "IntHolder1", "TEXT", field_length=150)
        arcpy.management.AddField(data, "IntHolder1Type", "TEXT", field_length=3)
        arcpy.management.AddField(data, "IntHolder2", "TEXT", field_length=150)
        arcpy.management.AddField(data, "IntHolder2Type", "TEXT", field_length=3)
        arcpy.management.AddField(data, "YearProt", "SHORT")
        arcpy.management.AddField(data, "YearProtComments", "TEXT", field_length=200)
        # Apparently fields are not case sensitive, so need  to add GapStatus1 then change
        # the name to GapStatus after deleting the existing GAPSTATUS
        arcpy.management.AddField(data, "GapStatus1", "SHORT")
        arcpy.management.AddField(data, "PubAccess1", "TEXT", field_length=7)  # Same thing here
        arcpy.management.AddField(data, "ProtDuration", "TEXT", field_length=5)
        arcpy.management.AddField(data, "UID", "TEXT", field_length=20)
        arcpy.management.CalculateField(data, "UID", "!OBJECTID!")
        print("Added and altered fields...")

    # Use code below or simply do so in ArcPro to export the PAGENCY and PTYPE domains for processing in R
    # Then the R outputs can be used in other functions below
    def export_domains():
        # UPDATE THIS - GDB with latest VT data
        gdb = "D:\\Lee\\POS\\Update_2023\\Data\\CadastralConserved_PROTECTEDLND\\CadastralConserved_PROTECTEDLND.gdb"
        out_path = "D:\\Lee\\POS\\Update_2023\\Data\\CadastralConserved_PROTECTEDLND"
        out_tbl = f'{out_path}\\vt_pld_pagency.csv'
        arcpy.management.DomainToTable(gdb, "PROTECTEDLND_PAGENCY_1", out_tbl, "Code", "Description")

        out_tbl = f'{out_path}\\vt_pld_ptype.csv'
        arcpy.management.DomainToTable(gdb, "PROTECTEDLND_PTYPE_1", out_tbl, "Code", "Description")
        print('Copied PAGENCY and PTYPE domains to CSV files')

    def assign_gap():
        with arcpy.da.UpdateCursor(data, ['GAPSTATUS', 'GapStatus1']) as cur:
            for row in cur:
                if row[0] is None:
                    row[1] = 0
                else:
                    row[1] = row[0]
                cur.updateRow(row)
        print('Populated GapStatus...')

    # For public access, we use the domain descriptions to classify the codes
    # 1 is public access, 2 and 4 are limited, and 3 is no public access
    # There are also some Null and 0 values to handle
    def assign_access():
        with arcpy.da.UpdateCursor(data, ['PUBACCESS', 'PubAccess1']) as cur:
            for row in cur:
                if row[0] is None or row[0] == 0:
                    row[1] = 'Unknown'
                elif row[0] == 1:
                    row[1] = 'Yes'
                elif row[0] == 2 or row[0] == 4:
                    row[1] = 'Limited'
                elif row[0] == 3:
                    row[1] = 'No'
                cur.updateRow(row)
        print('Populated PubAccess...')

    def assign_year():
        with arcpy.da.UpdateCursor(data, ['DATEAQRD', 'YearProt']) as cur:
            for row in cur:
                if row[0] is None:
                    row[1] = 0
                else:
                    row[1] = str(row[0])[:4]
                cur.updateRow(row)
        print('Populated YearProt...')

    def assign_duration():
        with arcpy.da.UpdateCursor(data, ['GapStatus1', 'ProtDuration']) as cur:
            for row in cur:
                if row[0] in [1, 2, 3, 39]:
                    row[1] = 'PERM'
                else:
                    row[1] = 'UNK'
                cur.updateRow(row)
        print('Populated ProtDuration...')

    # Despite having domains, there are values in the data that don't have a domain code and multiple empty values
    # We clean these up for better joins of the agency names CSV to the data. PAGENCY5 is not used as of 2024.
    def clean_agency_fields():
        with arcpy.da.UpdateCursor(data, 'PAGENCY1') as cur:
            for row in cur:
                if row[0] == '' or row[0] == ' ' or row[0] == 'null':
                    row[0] = None
                    cur.updateRow(row)
        print('Cleaned empty values in PAGENCY1...')

        with arcpy.da.UpdateCursor(data, 'PAGENCY2') as cur:
            for row in cur:
                if row[0] == '' or row[0] == ' ' or row[0] == 'null':
                    row[0] = None
                    cur.updateRow(row)
        print('Cleaned empty values in PAGENCY2...')

        with arcpy.da.UpdateCursor(data, 'PAGENCY3') as cur:
            for row in cur:
                if row[0] == '' or row[0] == ' ' or row[0] == 'null':
                    row[0] = None
                    cur.updateRow(row)
        print('Cleaned empty values in PAGENCY3...')

        # Strangely, this left some empty values in PAGENCY4, which I corrected manually after
        # joining the agency names and checking the 'Not in domain' rows
        with arcpy.da.UpdateCursor(data, 'PAGENCY4') as cur:
            for row in cur:
                if row[0] == '' or row[0] == ' ' or row[0] == 'null':
                    row[0] = None
                    cur.updateRow(row)
        print('Cleaned empty values in PAGENCY4...')

    # There were issues with incompatibility in the CSVs created with prep_vt_attribute_codes.R due to leading zeroes
    # in some domain codes. These are lost when opening the raw tables in Arc Pro. The solution is to create a new table
    # that has the proper field types, append the CSV table, and recalculate the code field to have padding zeroes again.
    # The argument to this function is the PAGENCY CSV exported from R.
    def prepare_pagency_table(pagency_csv):
        arcpy.management.CreateTable('D:\\Lee\\POS\\Update_2023\\Data\\sources.gdb', 'vt_pagency')  # Create empty table
        arcpy.management.AddField('vt_pagency', 'PAGENCY_CODE', 'TEXT')  # Add two text fields with same names as CSV columns
        arcpy.management.AddField('vt_pagency', 'PAGENCY_DESC', 'TEXT')
        arcpy.management.Append(pagency_csv, 'vt_pagency', 'NO_TEST')  # Append the CSV to the GDB table
        arcpy.management.CalculateField('vt_pagency', 'PAGENCY_CODE', "!PAGENCY_CODE!.zfill(5)")  # Add leading zeroes back into PAGENCY_CODE
        print('Prepared new table vt_pagency...')

    # With the PAGENCY fields cleaned and the PAGENCY table prepared, we are ready to join our PAGENCY names to the spatial data
    # For each PAGENCY field, we join the PAGENCY_DESC field from the vt_pagency GDB table, alter the field to change its name,
    # and then check for rows where there is a PAGENCY value but no domain match and flag these as such.
    # THIS CODE DOES NOT WORK DUE TO BUGS THAT ARE FIXED IN MORE CURRENT VERSIONS OF ARCPRO. :(
    # For 2024 update, I joined and altered all the field manually, then just ran the UpdateCursor code below. In the future,
    # all this code below should work.
    def join_agency_names():
        #arcpy.management.JoinField(data, 'PAGENCY1', 'vt_pagency', 'PAGENCY_CODE', 'PAGENCY_DESC')
        #arcpy.management.AlterField(data, 'PAGENCY_DESC', 'PAGENCY1_NAME', 'PAGENCY1_NAME')
        with arcpy.da.UpdateCursor(data, ['PAGENCY1', 'PAGENCY1_NAME'],
                                   'PAGENCY1 IS NOT NULL AND PAGENCY1_NAME IS NULL') as cur:
            for row in cur:
                if row[0] is not None and row[1] is None:
                    row[1] = 'Not in domain'
                    cur.updateRow(row)

        #arcpy.management.JoinField(data, 'PAGENCY2', 'vt_pagency', 'PAGENCY_CODE', 'PAGENCY_DESC')
        #arcpy.management.AlterField(data, 'PAGENCY_DESC', 'PAGENCY2_NAME', 'PAGENCY2_NAME')
        with arcpy.da.UpdateCursor(data, ['PAGENCY2', 'PAGENCY2_NAME'],
                                   'PAGENCY2 IS NOT NULL AND PAGENCY2_NAME IS NULL') as cur:
            for row in cur:
                if row[0] is not None and row[1] is None:
                    row[1] = 'Not in domain'
                    cur.updateRow(row)

        #arcpy.management.JoinField(data, 'PAGENCY3', 'vt_pagency', 'PAGENCY_CODE', 'PAGENCY_DESC')
        #arcpy.management.AlterField(data, 'PAGENCY_DESC', 'PAGENCY3_NAME', 'PAGENCY3_NAME')
        with arcpy.da.UpdateCursor(data, ['PAGENCY3', 'PAGENCY3_NAME'],
                                   'PAGENCY3 IS NOT NULL AND PAGENCY3_NAME IS NULL') as cur:
            for row in cur:
                if row[0] is not None and row[1] is None:
                    row[1] = 'Not in domain'
                    cur.updateRow(row)

        #arcpy.management.JoinField(data, 'PAGENCY4', 'vt_pagency', 'PAGENCY_CODE', 'PAGENCY_DESC')
        #arcpy.management.AlterField(data, 'PAGENCY_DESC', 'PAGENCY4_NAME', 'PAGENCY4_NAME')
        with arcpy.da.UpdateCursor(data, ['PAGENCY4', 'PAGENCY4_NAME'],
                                   'PAGENCY4 IS NOT NULL AND PAGENCY4_NAME IS NULL') as cur:
            for row in cur:
                if row[0] is not None and row[1] is None:
                    row[1] = 'Not in domain'
                    cur.updateRow(row)
        print('Joined PAGENCY_NAME fields...')

    # Repeat the same process of field cleaning, table creation, and joining for the PTYPE fields
    # In addition to cleaning up empty values, there are several PTYPE values not in the domain
    # If possible, we categorize these into their parent category (e.g., 10.5 --> 10)
    # In the future, domain values should be checked to see if any of these get added to the domain
    # and don't need to be reclassified here
    def clean_ptype_fields():
        with arcpy.da.UpdateCursor(data, 'PTYPE1') as cur:
            for row in cur:
                if row[0] == '' or row[0] == ' ' or row[0] == 'null':
                    row[0] = None
                elif row[0] == '10.1' or row[0] == '10.3' or row[0] == '10.5' or row[0] == '10.9':
                    row[0] = '10'
                cur.updateRow(row)
        print('Cleaned values in PTYPE1...')

        with arcpy.da.UpdateCursor(data, 'PTYPE2') as cur:
            for row in cur:
                if row[0] == '' or row[0] == ' ' or row[0] == 'null':
                    row[0] = None
                elif row[0] == '10.1' or row[0] == '10.3' or row[0] == '10.5' or row[0] == '10.9':
                    row[0] = '10'
                cur.updateRow(row)
        print('Cleaned values in PTYPE2...')

        with arcpy.da.UpdateCursor(data, 'PTYPE3') as cur:
            for row in cur:
                if row[0] == '' or row[0] == ' ' or row[0] == 'null':
                    row[0] = None
                elif row[0] == '10.1' or row[0] == '10.3' or row[0] == '10.5' or row[0] == '10.9':
                    row[0] = '10'
                cur.updateRow(row)
        print('Cleaned values in PTYPE3...')

        # Strangely, this left some empty values in PAGENCY4, which I corrected manually after
        # joining the agency names and checking the 'Not in domain' rows
        with arcpy.da.UpdateCursor(data, 'PTYPE4') as cur:
            for row in cur:
                if row[0] == '' or row[0] == ' ' or row[0] == 'null':
                    row[0] = None
                elif row[0] == '10.1' or row[0] == '10.3' or row[0] == '10.5' or row[0] == '10.9':
                    row[0] = '10'
                cur.updateRow(row)
        print('Cleaned values in PTYPE4...')

    def prepare_ptype_table(ptype_csv):
        arcpy.management.CreateTable('D:\\Lee\\POS\\Update_2023\\Data\\sources.gdb', 'vt_ptype')  # Create empty table
        arcpy.management.AddField('vt_ptype', 'PTYPE_CODE', 'TEXT')  # Add two text fields with same names as CSV columns
        arcpy.management.AddField('vt_ptype', 'PTYPE_DESC', 'TEXT')
        arcpy.management.Append(ptype_csv, 'vt_ptype', 'NO_TEST')  # Append the CSV to the GDB table
        # Unlike PAGENCY where all codes are the same length, PTYPE codes have variable length,
        # and we just need to add zeroes to the single digit codes (01-09)
        with arcpy.da.UpdateCursor('vt_ptype', 'PTYPE_CODE') as cur:
            for row in cur:
                if row[0] == '1':
                    row[0] = '01'
                elif row[0] == '2':
                    row[0] = '02'
                elif row[0] == '2.1':
                    row[0] = '02.1'
                elif row[0] == '3':
                    row[0] = '03'
                elif row[0] == '4':
                    row[0] = '04'
                elif row[0] == '6':
                    row[0] = '06'
                elif row[0] == '9':
                    row[0] = '09'
                cur.updateRow(row)
        print('Prepared new table vt_ptype...')

    def join_ptype_names():
        #arcpy.management.JoinField(data, 'PTYPE1', 'vt_ptype', 'PTYPE_CODE', 'PTYPE_DESC')
        #arcpy.management.AlterField(data, 'PTYPE_DESC', 'PTYPE1_NAME', 'PTYPE1_NAME')
        with arcpy.da.UpdateCursor(data, ['PTYPE1', 'PTYPE1_NAME'],
                                   'PTYPE1 IS NOT NULL AND PTYPE1_NAME IS NULL') as cur:
            for row in cur:
                if row[0] is not None and row[1] is None:
                    row[1] = 'Not in domain'
                    cur.updateRow(row)

        #arcpy.management.JoinField(data, 'PTYPE2', 'vt_ptype', 'PTYPE_CODE', 'PTYPE_DESC')
        #arcpy.management.AlterField(data, 'PTYPE_DESC', 'PTYPE2_NAME', 'PTYPE2_NAME')
        with arcpy.da.UpdateCursor(data, ['PTYPE2', 'PTYPE2_NAME'],
                                   'PTYPE2 IS NOT NULL AND PTYPE2_NAME IS NULL') as cur:
            for row in cur:
                if row[0] is not None and row[1] is None:
                    row[1] = 'Not in domain'
                    cur.updateRow(row)

        #arcpy.management.JoinField(data, 'PTYPE3', 'vt_ptype', 'PTYPE_CODE', 'PTYPE_DESC')
        #arcpy.management.AlterField(data, 'PTYPE_DESC', 'PTYPE3_NAME', 'PTYPE3_NAME')
        with arcpy.da.UpdateCursor(data, ['PTYPE3', 'PTYPE3_NAME'],
                                   'PTYPE3 IS NOT NULL AND PTYPE3_NAME IS NULL') as cur:
            for row in cur:
                if row[0] is not None and row[1] is None:
                    row[1] = 'Not in domain'
                    cur.updateRow(row)

        #arcpy.management.JoinField(data, 'PTYPE4', 'vt_ptype', 'PTYPE_CODE', 'PTYPE_DESC')
        #arcpy.management.AlterField(data, 'PTYPE_DESC', 'PTYPE4_NAME', 'PTYPE4_NAME')
        with arcpy.da.UpdateCursor(data, ['PTYPE4', 'PTYPE4_NAME'],
                                   'PTYPE4 IS NOT NULL AND PTYPE4_NAME IS NULL') as cur:
            for row in cur:
                if row[0] is not None and row[1] is None:
                    row[1] = 'Not in domain'
                    cur.updateRow(row)
        print('Joined PTYPE_NAME fields...')

    # In 2024 data, Fee Ownership is only used in PTYPE1 and PTYPE2, so we only need these fields to populate FeeOwner
    # Similar to NH, we will use the CSV of preprocessed owner names to
    def assign_fee_owner():
        names = pd.read_csv("D:\\Lee\\POS\\Update_2023\\Data\\CadastralConserved_PROTECTEDLND\\vt_pld_pagency_20240910.csv")
        names = names.astype({"PAGENCY_CODE": "str"})  # Convert the codes from integer to string
        names['PAGENCY_CODE'] = names['PAGENCY_CODE'].str.zfill(5)  # Pad with zero on the left so all are length of 5

        # Function to assign type based on the first digit in the organization's code
        def label_type(r):
            if r['PAGENCY_DESC'] == 'Winooski Valley Park District':    # This one has code 5 but is a muni public non-profit
                return 'LOC'
            elif r['PAGENCY_CODE'][0] == '0' or r['PAGENCY_CODE'][0] == '1' or r['PAGENCY_CODE'][0] == '2':
                return 'LOC'
            elif r['PAGENCY_CODE'][0] == '3':
                return 'FED'
            elif r['PAGENCY_CODE'][0] == '4':
                return 'STP'
            # Most 5 codes are PNP orgs except for a few
            elif r['PAGENCY_CODE'][0] == '5':
                if r['PAGENCY_CODE'] == '50000' or r['PAGENCY_CODE'] == '51300' or r['PAGENCY_CODE'] == '51405' or r['PAGENCY_CODE'] == '59998':
                    return 'PVT'
                elif r['PAGENCY_CODE'] == '59999':
                    return 'UNK'
                else:
                    return 'PNP'

        names['PAGENCY_TYPE'] = names.apply(label_type, axis=1)  # Create TYPE column by applying function to all rows

        # Create a dictionary of names (key) and types (value)
        name_type = dict(zip(names.PAGENCY_DESC, names.PAGENCY_TYPE))

        fo_codes = ['01', '02', '02.1', '03', '04', '06', '09']   # domain codes that correspond to fee ownership
        with arcpy.da.UpdateCursor(data, ['PTYPE1', 'PAGENCY1_NAME', 'PTYPE2', 'PAGENCY2_NAME',
                                          'FeeOwner', 'FeeOwnType', 'FeeOwnCatComments']) as cur:
            for row in cur:
                try:
                    if row[0] is None:
                        row[4] = 'Unspecified'
                        row[5] = 'UNK'
                    elif row[0] in fo_codes and row[2] in fo_codes:
                        # Sometimes PTYPE2 is populated but PAGENCY2 is not -- check for this
                        if row[3] is not None:
                            row[4] = f'{row[1]} & {row[3]}'  # Type needs to be checked manually for these rows
                            owner1type = name_type[row[1]]
                            owner2type = name_type[row[3]]
                            if owner1type == owner2type:
                                row[5] = owner1type
                            else:
                                row[5] = 'OTH'
                                row[6] = 'Jointly owned by different owner types'
                        else:
                            row[6] = row[1]
                            row[7] = name_type[row[1]]
                    elif row[0] in fo_codes:
                        row[4] = row[1]
                        row[5] = name_type[row[1]]
                    elif row[0] not in fo_codes and row[2] in fo_codes:
                        row[4] = row[3]
                        row[5] = name_type[row[3]]
                    else:                        # In ME we said if fee ownership is not a protection type,
                        row[4] = 'Private'       # then we assume private, so implementing the same thing here
                        row[5] = 'PVT'
                    cur.updateRow(row)
                except Exception:
                    continue
        print('Populated FeeOwner...')

    def assign_prot_type_int_holders():
        # VT has many protection types and values which are not protection types but describe what activities
        # are allowed in the area that we want to retain. There are some we want to flag in ProtTypeComments:
        # - Ecological Protection Zone (16) -- this is sometimes associated with easements as PTYPE1 and sometimes not as a secondary PTYPE
        # - River Corridor (17) -- these are also easements
        # - Agricultural Easement (18)
        # - Agricultural Preservation Deed Restrictions (21)
        # - Set Aside area of Development Deed Restriction (23)
        # - State of Vermont 'Natural Area' Designation (32) -- comment only, not a protection type
        # - No Development Zone within Covenant (37.1)
        # - No Development Zone within Easement (39.1)
        # - Limited Development permitted (general) (70) -- comment only, not a protection type
        # - Homestead or building complex (71) -- comment only, not a protection type
        # - Reserved housesite or development zone (72) -- comment only, not a protection type
        # - Subdividable parcel (73) -- comment only, not a protection type
        # - Farmstead complex (75) -- comment only, not a protection type
        # - Designated Mineral extraction area (80) -- comment only, not a protection type

        # Create lists of PTYPE codes for easier access
        # Contents of these lsits should be checked prior to each update
        fee = ['01', '02', '02.1', '03', '04', '06', '09']
        easements = ['10', '11', '11.1', '12', '13', '14', '15', '17', '18', '19', '27', '37', '78']
        drs = ['20', '21', '22', '23', '24', '25', '26']
        other_ptype1 = ['30', '31', '33', '34']    # Values only recorded as 'other' if they are in PTYPE1
        # Values we want to note in ProtTypeComments - excludes ag ease/DR and set asides, which
        # are both protection types and things we want to classify as ease/DR. The values in comment_only
        # are descriptive terms that aren't necessarily a relevant protection type
        # (e.g., "limited development permitted", "state of VT natural area designation")
        comment_only = ['16', '32', '37.1', '39.1', '70', '71', '72', '73', '75', '76', '80']
        # These are values that we just ignore when they are not in PTYPE1 (e.g., reverter, executory interest)
        # Some of these are values that are classified when PTYPE1 and others are not used at all,
        # and are not values we want to note in ProtTypeComments
        # This list could change in the future if any of these values become more abundant in the data or become
        # present in PTYPE1
        ignore = ['30', '31', '33', '34', '38', '50', '51', '51.1', '52', '53', '54', '59', '60', '75.1', '77', '79', '81', '85', '88', '90', '99']

        with arcpy.da.UpdateCursor(data, ['PTYPE1', 'PTYPE2', 'PTYPE3', 'PTYPE4', 'ProtType', 'ProtTypeComments',
                                          'PAGENCY1_NAME', 'PAGENCY2_NAME', 'PAGENCY3_NAME', 'PAGENCY4_NAME',
                                          'IntHolder1', 'IntHolder2',
                                          'PTYPE1_NAME', 'PTYPE2_NAME', 'PTYPE3_NAME', 'PTYPE4_NAME']) as cur:
            for row in cur:
                # First handle rows where there is no PTYPE1
                if row[0] is None:
                    row[4] = 'Unknown'
                # Handle rows where only PTYPE1 is populated - this is more than half of rows
                elif row[0] is not None and row[1] is None and row[2] is None and row[3] is None:
                    if row[0] in fee:
                        row[4] = 'Fee'
                    # For PTYPE1, we include a few extra values that based on investigation of the data are asssociated
                    # with easements when they are PTYPE1, but not when they are other PTYPEs. So we don't have them
                    # in the 'easements' list but we want to call them Ease if they are PTYPE1
                    # This includes ecological protection zones, river corridors, and "no development zones" within easements
                    elif row[0] in easements or row[0] == '16' or row[0] == '37.1' or row[0] == '39.1':
                        row[4] = 'Ease'
                        row[10] = row[6]
                        if row[0] == '18':     # If easement is an agricultural easement, note in ProtTypeComments
                            row[5] = f'{row[12]}'
                        if row[7] is not None:   # In VT data (as of 2024), there is not always a 1:1 with PTYPE and PAGENCY
                            row[11] = row[7]     # There are many rows with multiple holders and 1 PTYPE listed
                    elif row[0] in drs:
                        row[4] = 'DR'
                        row[10] = row[6]
                        if row[0] == '21' or row[0] == '23':   # If the DR is a setaside or APR, flag in ProtTypeComments
                            row[5] = f'{row[12]}'
                        if row[7] is not None:  # So for easements and DRs, even if there is no PTYPE2,
                            row[11] = row[7]    # we check for a value in PAGENCY2 to populate IntHolder2
                    elif row[0] == '35':
                        row[4] = 'Lease'
                        row[10] = row[6]
                    elif row[0] in other_ptype1:
                        row[4] = 'Other'
                        row[10] = row[6]
                    elif row[0] == '74':   # Internal right-of-way access
                        row[4] = 'ROW'
                        row[10] = row[6]
                # Then rows where just PTYPE1 = Fee, PTYPE2 is populated, and PTYPE3 is null
                elif row[0] in fee and row[1] is not None and row[2] is None:
                    if row[1] in easements:
                        row[4] = 'Fee and Ease'
                        row[10] = row[7]   # IntHolder1 is PAGENCY2
                        if row[8] is not None:  # If PTYPE3 is null but PAGENCY3 is not, then IntHolder2 is PAGENCY3
                            row[11] = row[8]
                    elif row[1] in drs:
                        row[4] = 'DR'
                        row[10] = row[7]
                        if row[8] is not None:
                            row[11] = row[8]
                    else:
                        row[4] = 'Fee'
                    # If PTYPE2 is an ag DR/ease or set aside, note in ProtTypeComments
                    if row[1] == '18' or row[1] == '21' or row[1] == '23':
                        row[5] = f'{row[13]}'
                # Then rows where PTYPE1 != Fee, PTYPE2 is populated, and PTYPE3 is null
                elif row[0] not in fee and row[1] is not None and row[2] is None:
                    if row[0] in easements and row[1] in fee:   # This doesn't exist in 2024 data but leaving code anyway
                        row[4] = 'Fee and Ease'
                        row[10] = row[6]    # IntHolder1 is PAGENCY1
                    elif row[0] in easements and row[1] in easements:
                        row[4] = 'Ease'
                        row[10] = row[6]  # IntHolder1 is PAGENCY1
                        row[11] = row[7]  # IntHolder2 is PAGENCY2 (not always populated though)
                        if row[0] != row[1]:    # If the two PTYPE easements are not the same, note their types
                            row[5] = f'Subject to {row[12]} and {row[13]}'
                    elif row[0] in easements and row[1] in drs:
                        row[4] = 'Ease'
                        row[10] = row[6]
                        row[11] = row[7]
                        row[5] = f'Subject to {row[12]} and {row[13]}'
                    elif (row[0] in comment_only or row[0] in ignore) and row[1] in easements:
                        row[4] = 'Ease'
                        row[10] = row[6]
                    # There are a few rows where PTYPE1 and PTYPE2 = 16 so including that below
                    elif row[0] in easements or row[0] == '16':   # Rows where PTYPE1 is an easement and PTYPE2 is a value we either ignore (e.g., reverter)
                        row[4] = 'Ease'         # or that is not a protection type (e.g., barn complex) will execute here
                        row[10] = row[6]
                        row[11] = row[7]
                    elif row[0] in drs:
                        row[4] = 'DR'
                        row[10] = row[6]
                        row[11] = row[7]
                    # Check for ag DR/ease or set asides
                    # If both PTYPE1 and PTYPE2 are ag interests, we take unique values
                    if row[0] in ['18', '21', '23'] and row[1] in ['18', '21', '23']:
                        unique_int = set([row[12], row[13]])
                        if len(unique_int) == 2:
                            row[5] = f'Subject to {list(unique_int)[0]}, {list(unique_int)[1]}'
                        else:
                            row[5] = f'{list(unique_int)[0]}'
                    elif row[0] in ['18', '21', '23']:
                        row[5] = f'{row[12]}'
                    elif row[1] in ['18', '21', '23']:
                        row[5] = f'{row[13]}'
                # Then rows where PTPYE1 and PTYPE3 are populated but PTYPE2 is null
                # In 2024 data these all have CE as PTYPE1. Some have PTYPE4 but they are all ecological protection zones
                # So we will skip checking for empty/populated PTYPE4, since comment only ints are checked
                # separately and will be captured then
                elif row[0] is not None and row[1] is None and row[2] is not None:
                    if row[0] in easements and row[2] in drs:
                        row[4] = 'Ease'
                        row[10] = row[6]
                        row[11] = row[7]
                        if row[0] == '18':
                            row[5] = f'{row[12]}, also subject to {row[14]}'
                        else:
                            row[5] = f'Also subject to {row[14]}'
                    elif row[0] in easements and row[2] in easements:
                        row[4] = 'Ease'
                        row[10] = row[6]
                        row[11] = row[7]
                        unique_int = set([row[12], row[14]])
                        if len(unique_int) == 2:
                            row[5] = f'{list(unique_int)[0]}, {list(unique_int)[1]}'
                        elif row[0] == '18' or row[2] == '18':
                            row[5] = 'Agricultural Easement'
                    elif row[0] in easements and (row[2] in comment_only or row[2] in ignore):
                        row[4] = 'Ease'
                        if row[0] == '18':
                            row[5] = f'{row[12]}'
                # Then rows where PTYPE1, PTYPE2, and PTYPE3 are populated and PTYPE4 is null
                # As with the section above, it's helpful to look at what values are present in the data to inform
                # what actually needs to be handled in code
                elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is None:
                    # PTYPE1, PTYPE2, and PTYPE3 are all easements
                    if row[0] in easements and row[1] in easements and row[2] in easements:
                        row[4] = 'Ease'
                        row[10] = row[6]
                        row[11] = row[7]
                        # Get the number of unique interests and if more than 1 kind, populate ProtTypeComments
                        unique_int = set([row[12], row[13], row[14]])
                        if len(unique_int) == 2:
                            row[5] = f'Subject to {list(unique_int)[0]}, {list(unique_int)[1]}'
                        elif len(unique_int) == 3:
                            row[5] = f'Subject to {list(unique_int)[0]}, {list(unique_int)[1]}, {list(unique_int)[2]}'
                    # PTYPE1 is an easement, PTYPE2 is an easement, PTYPE3 is DR
                    elif row[0] in easements and row[1] in easements and row[2] in drs:
                        row[4] = 'Ease'
                        row[10] = row[6]
                        row[11] = row[7]
                        unique_int = set([row[12], row[13], row[14]])
                        if len(unique_int) == 2:
                            row[5] = f'Subject to {list(unique_int)[0]}, {list(unique_int)[1]}'
                        elif len(unique_int) == 3:
                            row[5] = f'Subject to {list(unique_int)[0]}, {list(unique_int)[1]}, {list(unique_int)[2]}'
                    # PTYPE1 is an easement, PTYPE2 is comment only or ignore, PTYPE3 is an easement
                    elif row[0] in easements and (row[1] in comment_only or row[1] in ignore) and row[2] in easements:
                        row[4] = 'Ease'
                        row[10] = row[6]
                        row[11] = row[7]
                        if row[0] != row[2]:
                            row[5] = f'Subject to {row[12]}, {row[14]}'
                        elif row[0] == '18' or row[2] == '18':
                            row[5] = 'Agricultural Easement'
                    # PTYPE1 is an easement, PTYPE2 is an easement, PTYPE3 is comment only or ignore
                    elif row[0] in easements and row[1] in easements and (row[2] in comment_only or row[2] in ignore):
                        row[4] = 'Ease'
                        row[10] = row[6]
                        row[11] = row[7]
                        if row[0] != row[1]:
                            row[5] = f'Subject to {row[12]}, {row[13]}'
                        elif row[0] == '18' or row[1] == '18':
                            row[5] = 'Agricultural Easement'
                    # PTYPE1 is an easement, PTYPE2 is DR, PTYPE3 is comment only or ignore
                    elif row[0] in easements and row[1] in drs and (row[2] in comment_only or row[2] in ignore):
                        row[4] = 'Ease'
                        row[10] = row[6]
                        row[11] = row[7]
                        if row[0] == '18':
                            row[5] = f'{row[12]}, also subject to {row[13]}'
                        else:
                            row[5] = f'Also subject to {row[13]}'   # Note DR (PTYPE2) in ProtTypeComments
                    # PTYPE1 is an easement, PTYPE2 is comment only or ignore, PTYPE3 is comment only or ignore
                    elif row[0] in easements and (row[1] in comment_only or row[1] in ignore) and (row[2] in comment_only or row[2] in ignore):
                        row[4] = 'Ease'
                        row[10] = row[6]
                        row[11] = row[7]
                        if row[0] == '18':
                            row[5] = f'{row[12]}'
                    # PTYPE1 is an easement, PTYPE2 is comment_only or ignore, PTYPE3 is DR
                    elif row[0] in easements and (row[1] in comment_only or row[1] in ignore) and row[2] in drs:
                        row[4] = 'Ease'
                        row[10] = row[6]
                        row[11] = row[7]
                        if row[0] == '18':
                            row[5] = f'{row[12]}, also subject to {row[14]}'
                        else:
                            row[5] = f'Also subject to {row[14]}'
                    # PTYPE1 is fee, PTYPE2 is comment only or ignore, PTYPE3 is comment only or ignore
                    elif row[0] in fee and (row[1] in comment_only or row[1] in ignore) and (row[2] in comment_only or row[2] in ignore):
                        row[4] = 'Fee'
                    # PTYPE1 is fee, PTYPE2 is easement, PTYPE3 is comment only or ignore
                    elif row[0] in fee and row[1] in easements and (row[2] in comment_only or row[2] in ignore):
                        row[4] = 'Fee and Ease'
                        row[10] = row[7]   # IntHolder1 is PAGENCY2
                        row[11] = row[8]
                        if row[1] == '18':
                            row[5] = f'{row[13]}'
                    # PTYPE1 is fee, PTYPE2 is easement, PTYPE3 is easement
                    elif row[0] in fee and row[1] in easements and row[2] in easements:
                        row[4] = 'Fee and Ease'
                        row[10] = row[7]
                        row[11] = row[8]
                        if row[1] != row[2]:
                            row[5] = f'Subject to {row[13]}, {row[14]}'
                        elif row[1] == '18' or row[2] == '18':
                            row[5] = 'Agricultural Easement'
                    # PTYPE1 is fee, PTYPE2 is fee, PTYPE3 is DR
                    elif row[0] in fee and row[1] in fee and row[2] in drs:
                        row[4] = 'DR'
                        row[10] = row[8] # IntHolder1 is PAGENCY3
                        if row[2] == '21' or row[2] == '23':
                            row[5] = f'{row[14]}'
                    # PTYPE1 is fee, PTYPE2 is easement, PTYPE3 is DR
                    elif row[0] in fee and row[1] in easements and row[2] in drs:
                        row[4] = 'Fee and Ease'
                        row[10] = row[7]
                        if row[1] == '18':
                            row[5] = f'{row[13]}, also subject to {row[14]}'
                        else:
                            row[5] = f'Also subject to {row[14]}'
                    # PTYPE1 is fee, PTYPE2 is DR, PTYPE3 is comment only or ignore
                    elif row[0] in fee and row[1] in drs and (row[2] in comment_only or row[2] in ignore):
                        row[4] = 'DR'
                        row[10] = row[7]
                        if row[1] == '21' or row[1] == '23':
                            row[5] = f'{row[13]}'
                    # PTYPE1 is fee, PTYPE2 is fee, PTYPE3 is comment only or ignore
                    elif row[0] in fee and row[1] in fee and (row[2] in comment_only or row[2] in ignore):
                        row[4] = 'Fee'
                    # PTYPE1 is fee, PTYPE2 is fee, PTYPE3 is easement
                    elif row[0] in fee and row[1] in fee and row[2] in easements:
                        row[4] = 'Fee and Ease'
                        row[10] = row[8]  # IntHolder1 is PAGENCY3
                        if row[2] == '18':
                            row[5] = f'{row[14]}'
                    # PTYPE1 is comment only or ignore, PTYPE2 is easement, PTYPE3 is comment only or ignore
                    # THere's only one row that meets this crtieria and it only has PAGENCY1 listed
                    elif (row[0] in comment_only or row[0] in ignore) and row[1] in easements and (row[2] in comment_only or row[2] in ignore):
                        row[4] = 'Ease'
                        row[10] = row[6]
                        if row[1] == '18':
                            row[5] = f'{row[13]}'
                # THen rows where all four protection types are used
                elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None:
                    # PTYPE1 is easement, PTYPE2 is easement, PTYPE3 is easement, PTYPE4 is comment only or ignore
                    if row[0] in easements and row[1] in easements and row[2] in easements and (row[3] in comment_only or row[3] in ignore):
                        row[4] = 'Ease'
                        row[10] = row[6]
                        row[11] = row[7]
                        unique_int = set([row[12], row[13], row[14]])
                        if len(unique_int) == 2:
                            row[5] = f'Subject to {list(unique_int)[0]}, {list(unique_int)[1]}'
                        elif len(unique_int) == 3:
                            row[5] = f'Subject to {list(unique_int)[0]}, {list(unique_int)[1]}, {list(unique_int)[2]}'
                        elif row[0] == '18' or row[1] == '18' or row[2] == '18':
                            row[5] = 'Agricultural Easement'
                    # PTYPE1 and PTYPE 3 are easements, PTYPE2 and PTYPE4 are comment only or ignore
                    elif row[0] in easements and (row[1] in comment_only or row[1] in ignore) and row[2] in easements and (row[3] in comment_only or row[3] in ignore):
                        row[4] = 'Ease'
                        row[10] = row[6]
                        row[11] = row[7]
                        if row[0] != row[2]:
                            row[5] = f'Subject to {row[12]}, {row[14]}'
                        elif row[0] == '18' or row[2] == '18':
                            row[5] = 'Agricultural Easement'
                    # PTYPE1 and PTYPE4 are easements, PTYPE2 and PTYPE3 are comment only or ignore
                    elif row[0] in easements and (row[1] in comment_only or row[1] in ignore) and \
                            (row[2] in comment_only or row[2] in ignore) and row[3] in easements:
                        row[4] = 'Ease'
                        row[10] = row[6]
                        row[11] = row[7]
                        if row[0] != row[3]:
                            row[5] = f'Subject to {row[12]}, {row[15]}'
                        elif row[0] == '18' or row[3] == '18':
                            row[5] = 'Agricultural Easement'
                    # PTYPE1 is easement, all others are comment only or ignore
                    elif row[0] in easements and (row[1] in comment_only or row[1] in ignore) and \
                            (row[2] in comment_only or row[2] in ignore) and (row[3] in comment_only or row[3] in ignore):
                        row[4] = 'Ease'
                        row[10] = row[6]
                        row[11] = row[7]
                        if row[0] == '18':
                            row[5] = 'Agricultural Easement'
                    # PTYPE1 is fee, all others are easements
                    elif row[0] in fee and row[1] in easements and row[2] in easements and row[3] in easements:
                        row[4] = 'Fee and Ease'
                        row[10] = row[7]
                        row[11] = row[8]
                        unique_int = set([row[13], row[14], row[15]])
                        if len(unique_int) == 2:
                            row[5] = f'Subject to {list(unique_int)[0]}, {list(unique_int)[1]}'
                        elif len(unique_int) == 3:
                            row[5] = f'Subject to {list(unique_int)[0]}, {list(unique_int)[1]}, {list(unique_int)[2]}'
                        elif row[1] == '18' or row[2] == '18' or row[3] == '18':
                            row[5] = 'Agricultural Easement'
                    # PTYPE1 is fee, PTYPE2 is easement, PTYPE3 is easement, PTYPE4 is comment only or ignore
                    elif row[0] in fee and row[1] in easements and row[2] in easements and (row[3] in comment_only or row[3] in ignore):
                        row[4] = 'Fee and Ease'
                        row[10] = row[7]
                        row[11] = row[8]
                        if row[1] != row[2]:
                            row[5] = f'Subject to {row[13]}, {row[14]}'
                        elif row[1] == '18' or row[2] == '18':
                            row[5] = 'Agricultural Easement'
                    # PTYPE1 is fee, PTYPE2 is easement, PTYPE3 is DR, PTYPE4 is comment only or ignore
                    elif row[0] in fee and row[1] in easements and row[2] in drs and (row[3] in comment_only or row[3] in ignore):
                        row[4] = 'Fee and Ease'
                        row[10] = row[7]
                        row[11] = row[8]
                        if row[1] == '18':
                            row[5] = f'{row[13]}, also subject to {row[14]}'
                        else:
                            row[5] = f'Also subject to {row[14]}'  # Note DR in ProtTypeComments
                    # PTYPE1 is fee, PTYPE2 and PTYPE4 are easements, PTYPE3 is comment only or ignore
                    elif row[0] in fee and row[1] in easements and (row[2] in comment_only or row[2] in ignore) and row[3] in easements:
                        row[4] = 'Fee and Ease'
                        row[10] = row[7]
                        row[11] = row[8]
                        if row[1] != row[3]:
                            row[5] = f'Subject to {row[13]}, {row[15]}'
                        elif row[1] == '18' or row[3] == '18':
                            row[5] = 'Agricultural Easement'
                    # PTYPE1 is fee, PTYPE2 and PTYPE3 are easements, and PTYPE4 is DR
                    elif row[0] in fee and row[1] in easements and row[2] in easements and row[3] in drs:
                        row[4] = 'Fee and Ease'
                        row[10] = row[7]
                        row[11] = row[8]
                        if row[1] != row[2]:
                            row[5] = f'Subject to {row[13]}, {row[14]}, {row[15]}'
                        elif row[1] == '18' or row[2] == '18':
                            row[5] = f'Agricultural Easement, also subject to {row[15]}'
                        else:
                            row[5] = f'Also subject to {row[15]}'
                cur.updateRow(row)   # IMPORTANT! Update the rows so that ProtTypeComments updates

                # Then add additional comments if any prot type is in comment only
                # or 23 (Set aside DR -- added 5/2025)
                # Create empty list to store comment only interests
                comment_int = []
                # And add descriptive interest names to the list if in comment_only
                if row[0] in comment_only or row[0] == '23':
                    comment_int.append(row[12])
                if row[1] in comment_only or row[1] == '23':
                    comment_int.append(row[13])
                if row[2] in comment_only or row[2] == '23':
                    comment_int.append(row[14])
                if row[3] in comment_only or row[3] == '23':
                    comment_int.append(row[15])
                if len(comment_int) == 0:   # If there are no comment only interest, go to next row
                    continue
                else:
                    unique_comment_int = set(comment_int)   # Get unique comment only interests
                    unique_comment_int_list = list(unique_comment_int)
                    if row[5] is None:
                        row[5] = ', '.join(unique_comment_int_list)
                    else:
                        row[5] = '; '.join([row[5], ', '.join(unique_comment_int_list)])
                    cur.updateRow(row)
        print('Assigned ProtType and IntHolders...')

    def assign_int_holder_type():
        names = pd.read_csv("D:\\Lee\\POS\\Update_2023\\Data\\CadastralConserved_PROTECTEDLND\\vt_pld_pagency_20240910.csv")
        names = names.astype({"PAGENCY_CODE": "str"})  # Convert the codes from integer to string
        names['PAGENCY_CODE'] = names['PAGENCY_CODE'].str.zfill(5)  # Pad with zero on the left so all are length of 5

        # Function to assign type based on the first digit in the organization's code
        def label_type(r):
            if r['PAGENCY_CODE'][0] == '0' or r['PAGENCY_CODE'][0] == '1' or r['PAGENCY_CODE'][0] == '2':
                return 'LOC'
            elif r['PAGENCY_CODE'][0] == '3':
                return 'FED'
            elif r['PAGENCY_CODE'][0] == '4':
                return 'STP'
            # Most 5 codes are PNP orgs except for a few
            elif r['PAGENCY_CODE'][0] == '5':
                if r['PAGENCY_CODE'] == '50000' or r['PAGENCY_CODE'] == '51300' or r['PAGENCY_CODE'] == '51405' or r['PAGENCY_CODE'] == '59998':
                    return 'PVT'
                elif r['PAGENCY_CODE'] == '59999':
                    return 'UNK'
                else:
                    return 'PNP'

        names['PAGENCY_TYPE'] = names.apply(label_type, axis=1)  # Create TYPE column by applying function to all rows

        # Create a dictionary of names (key) and types (value)
        name_type = dict(zip(names.PAGENCY_DESC, names.PAGENCY_TYPE))

        # Since IntHolder1 and IntHolder2 are populated, we can use those fields to populate the corresponding type fields
        # Rather than using PTYPE codes like we did for assigning fee owner type
        with arcpy.da.UpdateCursor(data, ['IntHolder1', 'IntHolder1Type', 'IntHolder2', 'IntHolder2Type']) as cur:
            for row in cur:
                try:
                    # If IntHolder1 is not null, populate IntHolder1Type
                    if row[0] is not None:
                        row[1] = name_type[row[0]]
                        cur.updateRow(row)
                    # Same for IntHolder2
                    if row[2] is not None:
                        row[3] = name_type[row[2]]
                        cur.updateRow(row)
                except Exception:
                    continue
        print('Populated IntHolder types...')

    def assign_fee_own_cat():
        with arcpy.da.UpdateCursor(data, ['FeeOwnType', 'FeeOwnCat']) as cur:
            for row in cur:
                if row[0] in ['LOC', 'STP', 'FED']:
                    row[1] = 'Public'
                elif row[0] in ['PNP', 'PFP', 'PVT']:
                    row[1] = 'Private'
                elif row[0] == 'TRB':
                    row[1] = 'Tribal'
                else:
                    row[1] = 'Unknown'
                cur.updateRow(row)
        print('Assigned FeeOwnCat...')

    # Final clean up  - in addition to deleting fields, we also need to rename GapStatus1 and PubAccess1
    # after deleting the
    def delete_fields():
        deletes = ['POLYID', 'PTYPE1', 'PTYPE2', 'PTYPE3', 'PTYPE4', 'PTYPE5', 'PAGENCY1', 'PAGENCY2', 'PAGENCY3',
                   'PAGENCY4', 'PAGENCY5', 'GISACRES', 'SRCORG', 'SOURCE', 'SRCNOTES', 'STEWARD', 'DATEAQRD', 'NOTES',
                   'GAPSTATUS', 'DESIGNAT', 'PUBACCESS', 'PANOTES', 'UPDACTION', 'UPDDATE', 'UPDNOTES', 'SUBMIT',
                   'PAGENCY1_NAME', 'PAGENCY2_NAME', 'PAGENCY3_NAME', 'PAGENCY4_NAME', 'PTYPE1_NAME', 'PTYPE2_NAME',
                   'PTYPE3_NAME', 'PTYPE4_NAME']
        arcpy.management.DeleteField(data, deletes)
        print('Deleted fields...')

        arcpy.management.AlterField(data, 'GapStatus1', 'GapStatus', 'GapStatus')
        arcpy.management.AlterField(data, 'PubAccess1', 'PubAccess', 'PubAccess')
        print('Renamed GapStatus1 and PubAccess1...')

    try:
        #export_domains()
        #clean_agency_fields()
        #prepare_pagency_table('D:\\Lee\\POS\\Update_2023\\Data\\CadastralConserved_PROTECTEDLND\\vt_pld_pagency_20240910.csv')
        #join_agency_names()
        #clean_ptype_fields()
        #prepare_ptype_table('D:\\Lee\\POS\\Update_2023\\Data\\CadastralConserved_PROTECTEDLND\\vt_pld_ptype_20240910.csv')
        #join_ptype_names()
        add_alter_fields()
        assign_year()
        assign_access()
        assign_gap()
        assign_duration()
        assign_fee_owner()
        assign_prot_type_int_holders()
        assign_int_holder_type()
        assign_fee_own_cat()
    except Exception:
        print(traceback.format_exc())
    else:
        delete_fields()
    finally:
        print_elapsed_time()


# The conserved lands inventory is a one-off effort that has some new VT data
# but it is missing a lot of attributes unfortunately
# Prior to running this code, I used the Remove Domain from Field tool to remove
# the domains from the INT_TYPE, INT_ORGTYP, and FEE_ORGTYP fields so they can be
# recoded in the existing field. I also unchecked "Read Only" for any fields
# that were set to be read-only (which prevents them from being deleted)
def prep_vt_cli(data):
    def add_alter_fields():
        arcpy.management.AddField(data, "State", "TEXT", field_length=2)
        arcpy.management.CalculateField(data, "State", "'VT'")
        arcpy.management.AlterField(data, "AREA_NAME", "AreaName", "AreaName")
        arcpy.management.AlterField(data, "FEE_OWNER", "FeeOwner", "FeeOwner")
        arcpy.management.AlterField(data, "FEE_ORGTYP", "FeeOwnType", "FeeOwnType")
        arcpy.management.AddField(data, "FeeOwnCat", "TEXT", field_length=7)
        arcpy.management.AlterField(data, "INT_HOLDER", "IntHolder1", "IntHolder1")
        arcpy.management.AlterField(data, "INT_ORGTYP", "IntHolder1Type", "IntHolder1Type")
        arcpy.management.AlterField(data, "INT_TYPE", "ProtType", "ProtType")
        arcpy.management.AddField(data, "IntHolder2", "TEXT", field_length=150)
        arcpy.management.AddField(data, "IntHolder2Type", "TEXT", field_length=3)
        arcpy.management.AddField(data, "YearProt", "SHORT")
        arcpy.management.AddField(data, "FeeYear", "SHORT")
        arcpy.management.AddField(data, "EaseYear", "SHORT")
        arcpy.management.AddField(data, "GapStatus", "SHORT")
        arcpy.management.AddField(data, "PubAccess", "TEXT", field_length=7)
        arcpy.management.AddField(data, "ProtDuration", 'TEXT', field_length=5)
        arcpy.management.AddField(data, "UID", "TEXT")
        arcpy.management.CalculateField(data, "UID", "str(!OBJECTID!)")
        print("Added and altered fields...")

    # Attributes are very limited- need to use Comments field to draw some out
    def assign_pub_access():
        with arcpy.da.UpdateCursor(data, ['Comments', 'PubAccess']) as cur:
            for row in cur:
                if row[0] is not None:
                    if 'public access easement' in row[0].lower():
                        row[1] = 'Yes'
                        cur.updateRow(row)
                        continue
                    elif row[0].lower() == 'public access from river' or row[0].lower() == 'public access with certain conditions':
                        row[1] = 'Yes'
                        cur.updateRow(row)
                        continue
                # Any rows not meeting the above criteria will be set to unknown
                row[1] = 'Unknown'
                cur.updateRow(row)
        print('Updated PubAccess...')
    
    def assign_gap():
        with arcpy.da.UpdateCursor(data, ['Act59Cat', 'Comments', 'GapStatus']) as cur:
            for row in cur:
                # Because checking for values 'in' and not direct/complete comparison,
                # need to set criteria first that Comments is not empty to avoid an error
                if row[1] is not None:
                    if ('resource type: farm' in row[1].lower() or 'farmstead complex' in row[1].lower() 
                        or 'farm labor housing area' in row[1].lower()):
                        row[2] = 39
                        cur.updateRow(row)
                        continue
                if row[0] == '3':
                    row[2] = 3
                elif row[0] == '2':
                    row[2] = 2
                elif row[0] == '1':
                    row[2] = 1
                elif row[0] == '5':
                    row[2] = 4
                else:
                    row[0] = 0
                cur.updateRow(row)
        print('Updated GapStatus...')
    
    def assign_prot_duration():
        with arcpy.da.UpdateCursor(data, ['GapStatus', 'ProtDuration']) as cur:
            for row in cur:
                if row[0] in [39, 1, 2, 3]:
                    row[1] = 'PERM'
                else:
                    row[1] = 'UNK'
                cur.updateRow(row)
        print('Assigned ProtDuration based on GapStatus')
    
    def recode_org_type(field):
        with arcpy.da.UpdateCursor(data, field) as cur:
            for row in cur:
                if row[0] == 'TRIB':
                    row[0] = 'TRB'
                elif row[0] == 'STAT':
                    row[0] = 'STP'
                elif row[0] == 'DIST':
                    row[0] = 'LOC'
                elif row[0] == 'NGO':
                    row[0] = 'PNP'
                elif row[0] == 'JNT':
                    row[0] = 'OTH'
                cur.updateRow(row)
        print(f'Recoded org type in {field}')
    
    def recode_prot_type():
        with arcpy.da.UpdateCursor(data, 'ProtType') as cur:
            for row in cur:
                if row[0] is None:
                    row[0] = 'Unknown'
                elif 'Fee Ownership' in row[0]:
                    row[0] = 'Fee'
                elif 'Conservation Easement' in row[0]:
                    row[0] = 'Ease'
                elif 'Deed Restrictions' in row[0]:
                    row[0] = 'DR'
                elif 'Right of Way' in row[0]:
                    row[0] = 'ROW'
                else:
                    row[0] = 'Other'
                cur.updateRow(row)
        print('Recoded ProtType...')
    
    # Years are not provided with this data so all are unknown
    def assign_year_prot():
        arcpy.management.CalculateField(data, "YearProt", "0")
        print('Populated YearProt...')
    
    def assign_fee_own_cat():
        with arcpy.da.UpdateCursor(data, ['FeeOwnType', 'FeeOwnCat']) as cur:
            for row in cur:
                if row[0] in ['LOC', 'STP', 'FED']:
                    row[1] = 'Public'
                elif row[0] in ['PVT', 'PNP']:
                    row[1] = 'Private'
                elif row[0] == 'TRB':
                    row[1] = 'Tribal'
                elif row[0] == 'OTH':
                    row[1] = 'Other'
                elif row[0] == 'UNK':
                    row[1] = 'Unknown'
                cur.updateRow(row)
        print('Assigned FeeOwnCat...')
    
    def delete_fields():
        # All original / unaltered fields except SOURCE because we want to hold onto that
        # and GlobalID because that can't be deleted but will go away on its own when single part version is made
        deletes = ['SHARE', 'COMMENTS', 'StewardGIS', 'Act59Cat', 'Act59Notes']
        arcpy.management.DeleteField(data, deletes)
        print('Deleted fields...')
    
    try:
        add_alter_fields()
        assign_pub_access()
        assign_gap()
        assign_prot_duration()
        recode_org_type('FeeOwnType')
        recode_org_type('IntHolder1Type')
        recode_prot_type()
        assign_year_prot()
        assign_fee_own_cat()
    except Exception:
        print(traceback.format_exc())
        sys.exit()
    else:
        print('Success! Deleting fields...')
        delete_fields()
    finally:
        print_elapsed_time()
       

# Function to preprocess Eastern CT / MA data from Brian Hall
# Requires a state shapefile that has a field STUSPS with state abbreviations
def prep_bh_ma_ct(data, states):
    def add_alter_fields():
        arcpy.management.AddField(data, "UID", 'TEXT', field_length = 25)
        arcpy.management.CalculateField(data, "UID", "str(!OBJECTID!)")
        arcpy.management.AddField(data, "State", "TEXT", field_length = 2)
        arcpy.management.AlterField(data, "Local_AreaName", "AreaName", "AreaName")
        arcpy.management.AlterField(data, "Local_Fee_Owner_Name", "FeeOwner", "FeeOwner")
        arcpy.management.AddField(data, "FeeOwnType", "TEXT", field_length = 3)
        arcpy.management.AddField(data, "FeeOwnCat", "TEXT", field_length = 7)
        arcpy.management.AlterField(data, "Local_FeeOrEasement", "ProtType", "ProtType")
        arcpy.management.AddField(data, "ProtTypeComments", "TEXT", field_length = 200)
        arcpy.management.AlterField(data, "Local_CRIntHolder", "IntHolder1", "IntHolder1")
        arcpy.management.AddField(data, "IntHolder1Type", "TEXT", field_length = 3)
        arcpy.management.AddField(data, "IntHolder2", "TEXT", field_length = 100)
        arcpy.management.AddField(data, "IntHolder2Type", "TEXT", field_length = 3)
        arcpy.management.AlterField(data, "Local_NameOfWhoAddedToGIS", "Source", "Source")
        arcpy.management.AddField(data, "YearProt", "SHORT")
        arcpy.management.AddField(data, "FeeYear", "SHORT")
        arcpy.management.AddField(data, "EaseYear", "SHORT")
        arcpy.management.AddField(data, "GapStatus", "SHORT")
        arcpy.management.AddField(data, "PubAccess", "TEXT", field_length = 7)
        arcpy.management.AddField(data, "ProtDuration", "TEXT", field_length = 5)
        print("Added and altered fields...")
    
    # This code did not work... everything gets set to the last value calculated (MA)
    # I set the CT rows manually in ArcPro
    def calculate_state():
        # CT
        ct = arcpy.management.SelectLayerByAttribute(states, "NEW_SELECTION", "STUSPS = 'CT'")
        arcpy.management.SelectLayerByLocation(data, "HAVE_THEIR_CENTER_IN", ct, selection_type = "NEW_SELECTION")
        arcpy.management.CalculateField(data, "State", "'CT'")

        # MA
        ma = arcpy.management.SelectLayerByAttribute(states, "NEW_SELECTION", "STUSPS = 'MA'")
        arcpy.management.SelectLayerByLocation(data, "HAVE_THEIR_CENTER_IN", ma, selection_type = "NEW_SELECTION")
        arcpy.management.CalculateField(data, "State", "'MA'")

        print("Assigned state...")
    
    def assign_fee_owner_type():
        with arcpy.da.UpdateCursor(data, ["Local_Owner_Type", "FeeOwnType", "FeeOwner"]) as cur:
            for row in cur:
                if row[0] == "CONSERVATION_NGO":
                    row[1] = "PNP"
                elif row[0] == "EDUCATIONAL":
                    if (row[2] == "CONNECTICUT COLLEGE" or row[2] == "BECKER COLLEGE" or row[2] == "YALE UNIVERSITY"
                        or row[2] == "MITCHELL COLLEGE"):
                        row[1] = "PNP"
                    elif row[2] == "UCONN":
                        row[1] = "STP"
                    else:
                        row[1] = "UNK"
                elif row[0] == "FEDERAL":
                    row[1] = "FED"
                elif row[0] == "HOMEOWNERS ASSN." or row[0] == "PRIVATE":
                    row[1] = "PVT"
                elif row[0] == "MUNICIPAL":
                    row[1] = "LOC"
                elif row[0] == "OTHER":
                    row[1] = "OTH"
                elif row[0] == "STATE":
                    row[1] = "STP"
                elif row[0] == "UNKNOWN":
                    row[1] = "UNK"
                # Most utilities are from other sources so we'll avoid classifying these and just call them
                # private -- if it's wrong, it can be corrected in final QAQC
                elif row[0] == "UTILITY":
                    row[1] = "PVT"
                cur.updateRow(row)
        print("Assigned FeeOwnType...")
    
    def assign_fee_own_cat():
        with arcpy.da.UpdateCursor(data, ["FeeOwnType", "FeeOwnCat"]) as cur:
            for row in cur:
                if row[0] in ["PNP", "PVT"]:
                    row[1] = "Private"
                elif row[0] in ["FED", "STP", "LOC"]:
                    row[1] = "Public"
                elif row[0] == "OTH":
                    row[1] = "Other"
                elif row[0] == "UNK":
                    row[1] = "Unknown"
                cur.updateRow(row)
        print("Assigned FeeOwnCat...")
    
    def clean_int_holder():
        with arcpy.da.UpdateCursor(data, "IntHolder1") as cur:
            for row in cur:
                if row[0] == "NO EASEMENT":
                    row[0] = None
                    cur.updateRow(row)
        print("Set NO EASEMENT IntHolder to Null...")
    
    def assign_int_holder_type():
        with arcpy.da.UpdateCursor(data, ["Local_Interest_Holder_Type", "IntHolder1Type"]) as cur:
            for row in cur:
                if row[0] == "CONSERVATION_NGO":
                    row[1] = "PNP"
                elif row[0] == "CONSERVATION_NGO WITH OTHER":
                    row[1] = "OTH"
                elif row[0] == "FEDERAL":
                    row[1] = "FED"
                elif row[0] == "MUNICIPAL":
                    row[1] = "LOC"
                elif row[0] == "PRIVATE ERROR?":
                    row[1] = "PVT"
                elif row[0] == "STATE":
                    row[1] = "STP"
                elif row[0] == "STATE OR FEDERAL" or row[0] == "UNKNOWN":
                    row[1] = "UNK"
                cur.updateRow(row)
    
    def assign_prot_type():
        with arcpy.da.UpdateCursor(data, ["ProtType", "ProtTypeComments", "IntHolder1", "FeeOwnType"]) as cur:
            for row in cur:
                if row[0] == "FEE" or row[0] == "PROB FEE":
                    row[0] = "Fee"
                elif row[0] == "AG_EASEMENT":
                    row[0] = "Ease"
                    row[1] = "Agricultural Easement"
                elif row[0] == "EASE":
                    row[0] = "Ease"
                elif row[0] == "FEE AND EASE":
                    row[0] = "Fee and Ease"
                elif row[0] is None:
                    if row[2] is None:
                        row[0] = "Fee"
                    else:
                        if row[3] in ["FED", "STP", "LOC", "PNP"]:
                            row[0] = "Fee and Ease"
                        else:
                            row[0] = "Ease"
                cur.updateRow(row)
        print("Assigned ProtType...")
    
    # All the fields that exist in NEPOS but don't exist in source
    def populate_unknown_fields():
        arcpy.management.CalculateField(data, "YearProt", "0")
        arcpy.management.CalculateField(data, "GapStatus", "0")
        arcpy.management.CalculateField(data, "PubAccess", "'Unknown'")
        arcpy.management.CalculateField(data, "ProtDuration", "'UNK'")
        print("Calculated unknown fields...")
    
    def delete_fields():
        deletes = ["Local_Area_Name2", "Local_LandTrust_ID", "Notes", "Note2", "Local_Owner_Type",
                   "Acres_GIS", "Local_Address", "Local_Interest_Holder_Type"]
        arcpy.management.DeleteField(data, deletes)
        print("Deleted fields...")

    try:
        add_alter_fields()
        assign_fee_owner_type()
        assign_fee_own_cat()
        clean_int_holder()
        assign_int_holder_type()
        assign_prot_type()
        populate_unknown_fields()
    except Exception:
        print(traceback.format_exc())
    finally:
        delete_fields()
        print_elapsed_time()


def prep_ct_deep(data):
    def add_alter_fields():
        arcpy.management.AddField(data, "State", "TEXT", field_length = 2)
        arcpy.management.AlterField(data, "PROPERTY", "AreaName", "AreaName")
        arcpy.management.AddField(data, "FeeOwner", "TEXT", field_length = 150)
        arcpy.management.AddField(data, "FeeOwnType", "TEXT", field_length = 3)
        arcpy.management.AddField(data, "FeeOwnCat", "TEXT", field_length = 7)
        arcpy.management.AddField(data, "IntHolder1", "TEXT", field_length = 150)
        arcpy.management.AddField(data, "IntHolder1Type", "TEXT", field_length = 3)
        arcpy.management.AddField(data, "IntHolder2", "TEXT", field_length = 150)
        arcpy.management.AddField(data, "IntHolder2Type", "TEXT", field_length = 3)
        arcpy.management.AddField(data, "ProtType", "TEXT", field_length = 15)
        arcpy.management.AddField(data, "YearProt", "SHORT")
        arcpy.management.AddField(data, "FeeYear", "SHORT")
        arcpy.management.AddField(data, "EaseYear", "SHORT")
        arcpy.management.AddField(data, "GapStatus", "SHORT")
        arcpy.management.AddField(data, "PubAccess", "TEXT", field_length = 7)
        arcpy.management.AddField(data, "ProtDuration", "TEXT", field_length = 5)
        arcpy.management.AddField(data, "UID", "TEXT", field_length = 25)
        print("Added and altered fields...")
    
    def calculate_fields():
        arcpy.management.CalculateField(data, "State", "'CT'")
        arcpy.management.CalculateField(data, "FeeOwner", "'CT Department of Energy and Environmental Protection'")
        arcpy.management.CalculateField(data, "FeeOwnType", "'STP'")
        arcpy.management.CalculateField(data, "FeeOwnCat", "'Public'")
        arcpy.management.CalculateField(data, "ProtType", "'Fee'")
        arcpy.management.CalculateField(data, "YearProt", "0")
        arcpy.management.CalculateField(data, "GapStatus", "0")
        arcpy.management.CalculateField(data, "PubAccess", "'Unknown'")
        arcpy.management.CalculateField(data, "ProtDuration", "'UNK'")
        arcpy.management.CalculateField(data, "UID", "!DEP_ID!")  # May 2024 data this is a unique ID! Should check each download though
        print("Calculated fields...")
    
    def delete_fields():
        deletes = ["AV_LEGEND", "IMS_LEGEND", "DEP_ID", "AGNCYFN_CD", "ACRE_GIS"]
        arcpy.management.DeleteField(data, deletes)
        print("Deleted fields...")
    
    try:
        add_alter_fields()
        calculate_fields()
    except Exception:
        print(traceback.format_exc())
    finally:
        delete_fields()
        print_elapsed_time


def prep_wildlands(data):
    def add_alter_fields():
        arcpy.management.AlterField(data, "Property", "AreaName", "AreaName")
        arcpy.management.AddField(data, "FeeOwnType", "TEXT", field_length = 3)
        arcpy.management.AddField(data, "FeeOwnCat", "TEXT", field_length = 7)
        arcpy.management.AddField(data, "ProtType", "TEXT", field_length = 15)
        arcpy.management.AddField(data, "IntHolder1", "TEXT", field_length = 150)
        arcpy.management.AddField(data, "IntHolder2", "TEXT", field_length = 150)
        arcpy.management.AddField(data, "YearProt", "SHORT")
        arcpy.management.AddField(data, "FeeYear", "SHORT")
        arcpy.management.AddField(data, "EaseYear", "SHORT")
        arcpy.management.AddField(data, "WildYear", "SHORT")
        arcpy.management.AddField(data, "GapStatus", "SHORT")
        arcpy.management.AddField(data, "PubAccess", "TEXT", field_length = 7)
        arcpy.management.AddField(data, "ProtDuration", "TEXT", field_length = 5)
        arcpy.management.AddField(data, "UID", "TEXT", field_length = 25)
        arcpy.management.CalculateField(data, "UID", "!PropID!")
        print("Added and altered fields...")
    
    def assign_owner_type_cat():
        with arcpy.da.UpdateCursor(data, ["FeeOwner", "OwnerSubType", "FeeOwnType", "FeeOwnCat"]) as cur:
            for row in cur:
                if row[1] == "BUSINESS" or row[1] == "REALTY" or row[1] == "TIMBER":
                    row[2] = "PFP"
                    row[3] = "Private"
                # Check institutions if this data is updated -- currently all are private except
                # UMass Amherst and UVM
                elif row[1] == "COLLEGE/UNIVERSITY":
                    if row[0] == "University of Massachusetts - Amherst" or row[0] == "University of Vermont":
                        row[2] = "STP"
                        row[3] = "Public"
                    else:
                        row[2] = "PNP"
                        row[3] = "Private"
                # Another category to check for future updates -- currently only 1 record which
                # is a boarding school
                elif row[1] == "ELEMENTARY/SECONDARY":
                    row[2] = "PNP"
                    row[3] = "Private"
                elif row[1] == "FAMILY-OWNED":
                    row[2] = "PVT"
                    row[3] = "Private"
                elif row[1] == "FEDERAL":
                    row[2] = "FED"
                    row[3] = "Public"
                elif row[1] == "FOUNDATION":
                    row[2] = "PNP"
                    row[3] = "Private"
                elif row[1] == "LAND CONS/EDUCATION" or row[1] == "LAND CONS/HEALTH" or row[1] == "LAND CONSERVATION":
                    row[2] = "PNP"
                    row[3] = "Private"
                elif row[1] == "MUNICIPAL":
                    row[2] = "LOC"
                    row[3] = "Public"
                elif row[1] == "MUNICIPAL (QUASI)":
                    row[2] = "QP"
                    row[3] = "Public"
                elif row[1] == "NGO (RECREATIONAL CLUB)":
                    row[2] = "PNP"
                    row[3] = "Private"
                # Another category to check -- currently only 1 record and it's Baxter SP Auth
                elif row[1] == "PUBLIC TRUST":
                    row[2] = "STP"
                    row[3] = "Public"
                elif row[1] == "STATE":
                    row[2] = "STP"
                    row[3] = "Public"
                cur.updateRow(row)
        print("Assigned FeeOwnType and FeeOwnCat...")

    def assign_wild_year():
        with arcpy.da.UpdateCursor(data, ["YearOrig", "WildYear"]) as cur:
            for row in cur:
                if row[0] == "-99":
                    row[1] = 0
                else:
                    row[1] = int(row[0])
                cur.updateRow(row)
        print("Assigned WildYear...")
    
    # GapStatus does not apply for this dataset because many of the wildlands
    # are not legally protected as such -- GapStatus is about both management intensity
    # and protection duration and the latter doesn't work for this dataset
    def assign_gap():
        arcpy.management.CalculateField(data, "GapStatus", "0")
        print("Assigned GapStatus of 0...")

    def calc_unknown_fields():
        arcpy.management.CalculateField(data, "ProtType", "'Unknown'")
        arcpy.management.CalculateField(data, "PubAccess", "'Unknown'")
        arcpy.management.CalculateField(data, "ProtDuration", "'UNK'")
        print("Calculated unknown fields...")
    
    def delete_fields():
        deletes = ["STATE_ID", "PropID", "OwnerType", "OwnerSubType", "YearOrig",
                   "AcresPerDoc", "WldPerDeed", "WldPerStatueFed", "WldPerStatueState",
                   "WldPerMgmtPlan", "WldPerOther", "ProtDocs", "HasThirdParty",
                   "ThirdPartyID", "VersionAddd", "Town", "WildIntent", "Catalyst",
                   "MgmtPlanType", "CommProhibit", "ExceptByOwner", "ExceptHumanHealth",
                   "ExceptTrails", "ExceptEcosy", "ExceptRare", "ExceptInvasive", "AcresGIS"]
        arcpy.management.DeleteField(data, deletes)
        print("Deleted fields...")
    
    try:
        add_alter_fields()
        assign_owner_type_cat()
        assign_wild_year()
        assign_gap()
        calc_unknown_fields()
    except Exception:
        traceback.print_exc()
    else:
        delete_fields()
    finally:
        print_elapsed_time()


# This function takes the preprocessed data and saves it to a new GDB, which
# should be the same GDB that NEPOS is located in for development, spatial matching,
# and other updates.
# Arguments:
#  - data (text): path to preprocessed data (output returned by project_data() after
#                 running through main "prep" script)
#  - path (text): complete path to the GDB where final prepared data should be sent
#                 technically, you could send it to the same GDB as the raw data,
#                 but personally I like to have one GDB where I have the raw and preprocessed
#                 multipart source layers, and then send the final singlepart layer to a clean
#                 space where NEPOS lives so I have my final analysis files in a separate space
def make_single_part(data, path):
    data_fn = os.path.basename(data)
    print('Beginning {}...'.format(data_fn))
    out_feat = f'{path}/{data_fn}_sp'  # Note saves to different gdb

    # Add field PART_COUNT to refer to when assigning UID2
    try:
        arcpy.management.AddField(data, 'PART_COUNT', 'SHORT')
    except Exception:
        pass        # The only error would be the field already existing, in which case just keep going
    finally:
        arcpy.management.CalculateGeometryAttributes(data, [['PART_COUNT', 'PART_COUNT']])
        print('Created and calculated PART_COUNT...')

    arcpy.management.MultipartToSinglepart(data, out_feat)
    print('Created single part features...')

    arcpy.management.AddField(out_feat, 'UID2', 'TEXT', field_length=40)
    print('Created field to store UID2...')

    # Populate UID2 with either UID or UID-OBJECTID if PART_COUNT > 1
    c = 0    # Counter to track number of multipart features
    with arcpy.da.UpdateCursor(out_feat, ['PART_COUNT', 'UID', 'UID2', 'OBJECTID']) as cur:
        for row in cur:
            if row[0] == 1:
                row[2] = str(row[1])
            elif row[0] > 1:
                row[2] = f'{str(row[1])}-{str(row[3])}'
                c = c + 1
            cur.updateRow(row)
    del row, cur
    print('Populated UID2...')
    print(f"There are {c} multipart features in {data_fn}...")

    # Recalculate acres
    arcpy.management.CalculateGeometryAttributes(out_feat, [["acres", "AREA"]], area_unit="ACRES_US")
    print("Recalculated acres...")


####### FUNCTION CALLS ###############
# NOTE: You need to comment out any lines you don't need! 
# It can also be useful to not make the singlepart features until after
# you've had a chance to inspect the results of the main prep function.
# (For example, there may be new values that don't get captured in the code
# and the function needs to be edited/refined.)
# It just cuts down on time as you're iterating on that to not make
# singlepart features until you're sure the data are ready for that.
#
# NOTE: Sometimes it is necessary to repair geometry before making
# singlepart files. It might be a good idea to just incorporate that
# into part of the preprocessing at some point... but as of now, it's
# only done on an as-needed basis


##### Call MassGIS function #####
ma = ""         # Raw data (last updated 1/2025) copied into workspace GDB from MassGIS GDB
#ma_proj = project_data(ma)   # Project to NEPOS CRS
#prep_massgis(ma_proj)       # Preprocess projected layer
#make_single_part(ma_proj)   # Make preprocessed layer singlepart


###### Call TNC function ######
tnc = ""  # Clipped or subset to NE
#tnc_proj = project_data(tnc)
#prep_tnc(tnc_proj)
#make_single_part(ma_proj)


###### Call NCED function #####
nced = ""   # NCED polygons clipped to NE state boundaries - 2024 version
#nced_proj = project_data(nced)
#prep_nced(nced_proj)
#make_single_part(nced_proj)


###### Call PADUS function #####
padus = ""  # Clipped or subset to NE, fee and easement layers combined
#padus_proj = project_data(padus)
#prep_padus(padus_proj)
#make_single_part(padus_proj)


###### Call Maine function #####
maine = ""
#maine_proj = project_data(maine)
#prep_maine(maine_proj)
#make_single_part(maine_proj)


##### Call RI Local function #####
ri_local = ""
#ri_local_proj = project_data(ri_local)
#prep_ri_local(ri_local_proj)
#arcpy.management.RepairGeometry(ri_local_proj)   # Need to get rid of polygons with empty geom
#make_single_part(ri_local_proj)


##### Call RI state function #####
ri_state = ""
#ri_state_proj = project_data(ri_state)
#prep_ri_state(ri_state_proj)
#make_single_part(ri_state_proj)


##### Call NH function #####
nh = ""
#nh_proj = project_data(nh)
#prep_nh(nh_proj)
#make_single_part(nh_proj)


#### Call VT function #####
# VT Protected Lands Database
vt_pld = ""
#vt_pld_proj = project_data(vt_pld)
#prep_vt(vt_pld_proj)
#make_single_part(vt_pld_proj)

# VT Conserved Land Inventory
vt_cli = ""
#vt_cli_proj = project_data(vt_cli)
#prep_vt_cli(vt_cli_proj)
#make_single_part(vt_cli_proj)


#### Call CT DEEP function #####
deep = ""
#deep_proj = project_data(deep)
#prep_ct_deep(deep_proj)
#make_single_part(deep_proj)


#### Call Wildlands function ####
wildlands = ""
#wildlands_proj = project_data(wildlands)
#prep_wildlands(wildlands_proj)
#make_single_part(wildlands_proj)
