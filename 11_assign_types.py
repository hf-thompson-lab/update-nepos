#######################################################################
# This script calculates the 'type' field of POS for the entire
# dataset, and applies corrections based on FinalID to PAs that we
# know are misclassified by the type creation.
#
# Created 11/2019 to replace create_pos_types.R
# The ArcGIS R package is not ideal for writing data because it
# overwrites all field types and lengths. Using R, all text fields are
# enlarged to 255 characters, and short integers are changed to long.
# Because POS is so large we cannot have our fields that big, and
# so we should use this Python script to edit POS types, and not R.
#
# One potentially desirable modification could be to select only
# rows that do not have 'type' assigned before the UpdateCursor.
# This script does not take long to run, but that edit would speed
# it up and avoid correcting the same rows over and over.
#
# Created by Lucy Lee based on create_pos_types.R, 11/2019
# 4/14/21 Updated to include a where_clause in the UpdateCursor
#    which returns only rows where type is null - avoids need
#    to use correction code (left correction code commented out for ref)
#
#######################################################################

import arcpy

# Workspace should be wherever development POS lives
arcpy.env.workspace = "D:/Lee/POS/Update_2023/Data/new_data2.gdb/"

fc = "POS_v2_29_sp"

# Function to update PA type
# This function does general classifying then does a second round based on keywords and other criteria
# Arguments: state (text): state abbreviation, used to update type for only one state
#                          default is None which updates type for all states
#            sql (text): SQL query to subset rows to update type for (based on criteria other than state)
# You can also leave state set to None and include state in the SQL (the messages printed by the function
# will be wrong but it will work fine). That is probably a better idea if the SQL query is complex, so that
# the entire query can be in one argument and you can be more confident it will apply correctly.
#          
# By default, this code does not update any rows where type is LPT or CF since these areas
# are usually determined through additional research. If you decide to use a custom SQL query and do not
# exclude LPT and CF types from being updated, you will need to set include_lpt_cf to True in the correct_type()
# function below.
def update_type(state = None, sql = None):

    # Use just the fields we need for calculating type
    fields = ["type", "YearProt", "Area_Ha", "GapStatus", "FeeOwnCat",
            "AreaName", "FeeOwner", "WildYear", "ProtTypeComments", "IntHolder1"]

    # Create lists of GAP statuses used for reserve / multiple use
    gap_res = [1, 2]
    gap_mu = [3, 4, 39]

    # Update types! If sql is None, then the default query will be used which is to
    # update all types EXCEPT LPT and CF -- these are checked manually and usually we don't want to change these
    # If sql is not None, then we use the sql query instead - so be careful about using the sql argument.
    # In most cases we only want to use the sql argument when we are making changes to a specific type
    # of subset of rows that inherently excludes LPTs or CFs
    if state is not None and sql is None:
        print(f"State selected: {state}...")
        query = "((type <> 'LPT' AND type <> 'CF') OR type IS NULL)) AND State = '" + state + "'" 
    elif state is None and sql is None:
        print("Updating type for all states...")
        query = "(type <> 'LPT' AND type <> 'CF') OR type IS NULL"
    elif state is not None and sql is not None:
        print(f"State selected: {state}...")
        query = sql + " AND State = '" + state + "'"
    elif state is None and sql is not None:
        print("Updating type for all states...")
        query = sql
    print(f"NEPOS subset to process is: {query} in {state} state")

    with arcpy.da.UpdateCursor(fc, fields, query) as cursor:
        for row in cursor:
            if row[2] > 1400 and row[3] in gap_mu and row[4] == "Private":
                row[0] = "LPT"
            elif row[2] < 1400 and row[3] in gap_mu and row[4] == "Private":
                row[0] = "PrMu"
            elif row[3] in gap_mu and row[4] == "Public":
                row[0] = "PuMu"
            elif row[3] in gap_res and row[4] == "Public":
                row[0] = "PuRes"
            elif row[3] in gap_res and row[4] == "Private":
                row[0] = "PrRes"
            elif row[3] is None or row[3] == 0:
                row[0] = "No-Gap-Status"
            elif row[4] == 'Other' or row[4] == 'Unknown':
                row[0] = "No-Owner-Type"
            elif row[4] == 'Tribal':
                row[0] = 'Tribal'
            else:
                row[0] = "NA"
            cursor.updateRow(row)
    print("Done with initial type creation...on to specific categories")

    # Check for wildlands, community forests, and ag
    # These are more specific subcategories of the above general types
    # Note on structure: The conditionals are all set to "if" so that all
    # are evaluated if needed. In each conditional, if the correct condition
    # is met, "continue" is used to push to the next row after the row is updated
    # This way, don't need to worry about things being missed or overwritten.
    # e.g., if ProtTypeComments is not null but isn't about APR
    # The order of these statements matters, because as soon as one is successful it goes to the next row
    # These criteria may need to be updated over time if additional keywords, interest holders, etc.
    # become prominent enough in the data that we notice them
    if state is not None:
        query = "type <> 'LPT' AND type <> 'CF' AND State = '" + state + "'"
    else:
        query = "type <> 'LPT' AND type <> 'CF'"
    
    # Variables for counting the number of each type created
    cf = 0
    f = 0
    w = 0
    cem = 0
    p = 0
    r = 0
    gc = 0
    cc = 0
    with arcpy.da.UpdateCursor(fc, fields, query) as cur:
        for row in cur:
            ### Farmland
            # Based on GAP status
            if row[3] == 39:
                row[0] = "Farm"
                f = f +1
                cur.updateRow(row)
                continue
            # Based on AreaName or ProtTypeComments and private ownership and GAP
            if ((' APR' in row[5] or 'farmland' in row[5].lower() or 'frpp' in row[5].lower() or 'farm services agency' in row[5].lower()
                 or 'ACEP-ALE' in row[5].lower()) and row[4] == 'Private' and row[3] not in gap_res):
                row[0] = "Farm"
                f = f + 1
                cur.updateRow(row)
                continue
            if row[8] is not None:
                if ('agricultural' in row[8].lower() or ' APR' in row[8] or 'farmstead' in row[8].lower() 
                    or 'Agricultural Land Preservation' in row[8] or 'farm labor' in row[8].lower()):
                    row[0] = "Farm"
                    f = f + 1
                    cur.updateRow(row)
                    continue
            # Based on IntHolder - mainly for CT where there's a lot of data that NCED
            # got from the CT Dept of Ag -- assuming these are all farm easements
            if row[9] is not None:
                if row[9] == 'CT Department of Agriculture':
                    row[0] = 'Farm'
                    f = f + 1
                    cur.updateRow(row)
                    continue
            
            ### Wildlands - based on presence of any value (even 0) for WildYear
            if row[7] is not None:
                row[0] = "Wildland"
                w = w + 1
                cur.updateRow(row)
                continue
            
            ### Community forests
            if 'community forest' in row[5].lower():
                row[0] = "CF"
                cf = cf + 1
                cur.updateRow(row)
                continue
            
            ### Cemeteries
            if 'cemetery' in row[5].lower() or 'burial ground' in row[5].lower():
                row[0] = 'Cemetery'
                cem = cem + 1
                cur.updateRow(row)
                continue

            ### Playgrounds
            if 'playground' in row[5].lower() or 'play ground' in row[5].lower() or 'tot lot' in row[5].lower():
                row[0] = 'Playground'
                p = p + 1
                cur.updateRow(row)
                continue

            ### Playing fields
            if 'recreation field' in row[5].lower() or 'ball field' in row[5].lower() or 'little league field' in row[5].lower() or 'soccer field' in row[5].lower():
                row[0] = 'Rec Field'
                r = r + 1
                cur.updateRow(row)
                continue

            ### Golf course
            if 'golf club' in row[5].lower() or 'golf course' in row[5].lower():
                row[0] = 'Golf course'
                gc = gc + 1
                cur.updateRow(row)
                continue

            ### Country club
            if 'country club' in row[5].lower() or row[5] == 'Ten Mile River (Agawam Hunt)' or row[5] == 'Ten Mile River (Agawam Hunt 2)':
                row[0] = 'Country club'
                cc = cc + 1
                cur.updateRow(row)
                continue

            ### Military (fee)??

                
        print(f"Updated type for {w} wildlands, {f} farmlands, {cf} community forests, {cem} cemeteries, {p} playgrounds, {r} playing fields, {gc} golf courses, and {cc} country clubs")

# Corrections by unique ID
# There is a list for each "type" where you can put FinalID2 of polygons that should be changed to that type
# Most commonly, this is PrMu, LPT, and CF.
# Areas that need to be changed to PrMu are typically large areas that get categorized as LPT based on size, 
# but they are not actually LPT, usually determined through additional research. 
# Areas that need to be changed to LPT are usually smaller polygons that are not large enough on their own
# to be categorized as LPT, but they are part of an LPT based on shared attributes.
# Areas that need to be changed to CF are areas we know to be community forests but do not have "community forest" in the area name.
# Each list of types to correct to is sorted by PA - if any FinalID2s are on the same line, they are the same (multipart) PA.
# Some PAs go across multiple lines - usually the FinalID2s will be more or less consecutive for a PA so if consecutive
# numbers are split across multiple lines that also indicates the FinalID2s belong to the same multipart PA.
# The order of FinalID2 in the list matches the comments above the list saying which PAs the corrections include.
# NOTE: It is important to use FinalID2 so that the singlepart version of the data has the correct type. We want
# the singlepart version to dissolve nicely into the multipart version, so type has to be consistent across polygons
# that should be dissolved in the multipart version. This is usually an iterative process with LPTs: you try dissolving
# the singlepart NEPOS, and investigate the type field for any rows where type contains the text "LPT /"
# which indicates LPT and another type. Then you can investigate the polygons and if appropriate, add the polygon to the
# list below so that it gets recategorized to LPT (or another type). It is also important that these corrections
# be based on FinalID2 because FinalID can change - it is recalculated each time multipart polygons are created. FinalID2
# is the permanent unique ID on a polygon level - the multipart ID (FinalID) does not have the same permanence.
# Argument: include_lpt_cf (boolean) - should the type correction by FinalID2 also do LPT and CF types?
#           the general type categorization function above by default does not overwrite LPT and CF PAs since these
#           are usually determined based on additional research. The only time these would get overwritten is
#           if the user specifies a custom query that does not exclude LPT and CF rows from type assignment.
#           Therefore, running type correction code here for those categories is only necessary if they get overwritten above.
def correct_type(include_lpt_cf = False):
    # Use just the fields we need for type corrections
    corr_fields = ["FinalID2", "type"]

    # FinalID2s of PAs that should be marked as PrMu
    # These areas include Acadia NP, EPI Quimby Lands, Machias River, Great Mountain Forest,
    # The Balsams, Dartmouth's Second College Grant, Dartmouth's Mt. Moosilauke, USN Ursa Major,
    # Crooked River Headwaters Easement, Kennebago Headwaters
    prmu_corr_ids = ["FinalID2 - 068054", 
                     "FinalID2 - 066848", 
                     "FinalID2 - 080322", "FinalID2 - 080323", "FinalID2 - 080324", "FinalID2 - 080325", "FinalID2 - 080326", "FinalID2 - 080327", 
                     "FinalID2 - 080328", "FinalID2 - 080329", "FinalID2 - 080330", "FinalID2 - 080331", "FinalID2 - 080332", "FinalID2 - 080333",
                     "FinalID2 - 080260",
                     "FinalID2 - 079739", "FinalID2 - 079740", "FinalID2 - 079741", "FinalID2 - 079742", "FinalID2 - 079743",
                     "FinalID2 - 066136",
                     "FinalID2 - 026795",
                     "FinalID2 - 124838",
                     "FinalID2 - 124530",
                     "FinalID2 - 124637"]

    # FinalID2s of PAs that should be marked as PrRes
    prres_corr_ids = []

    # FinalID2s of PAs that should be marked as PuMu
    pumu_corr_ids = []

    # FinalID2s of PAs that should be marked as PuRes
    pures_corr_ids = []

    # FinalID2s of PAs that should be marked as LPT, polygons that alone are too small to be flagged as LPT
    # but are part of the LPT based on attributes, easement docs, etc.
    # These include Bunnell Working Forest, Pillsbury-Sunapee Highlights, Ossipee Mountains Tract,
    # Coburn Gore, Fish River Chain of Lakes, Pingree Easement, Upper St John River Easement South,
    # Katahdin Forest, Reed Forest (Apple CE), Sunrise Easement, Connecticut Lakes Headwaters
    # The previous function by default does not overwrite LPT types, so this code is only needed if those get overwritten above.
    lpt_corr_ids = ["FinalID2 - 065918", "FinalID2 - 065919",
                    "FinalID2 - 029311", "FinalID2 - 029312", "FinalID2 - 029313", "FinalID2 - 029314", "FinalID2 - 029315", "FinalID2 - 029316", "FinalID2 - 029317",
                    "FinalID2 - 080263", "FinalID2 - 080264",
                    "FinalID2 - 124265", "FinalID2 - 124267",
                    "FinalID2 - 123916", "FinalID2 - 123917", "FinalID2 - 123918", "FinalID2 - 123919", "FinalID2 - 123920", "FinalID2 - 123921", 
                    "FinalID2 - 123922", "FinalID2 - 123924", "FinalID2 - 123925", "FinalID2 - 124807", "FinalID2 - 124808",
                    "FinalID2 - 079904",
                    "FinalID2 - 065430",
                    "FinalID2 - 070673", "FinalID2 - 070674", "FinalID2 - 070675", "FinalID2 - 070676", "FinalID2 - 070677", "FinalID2 - 070678", 
                    "FinalID2 - 070679", "FinalID2 - 070680", "FinalID2 - 070681", "FinalID2 - 070682", "FinalID2 - 070685", "FinalID2 - 070688", 
                    "FinalID2 - 070689", "FinalID2 - 070690", "FinalID2 - 070691", "FinalID2 - 070693", "FinalID2 - 070694", "FinalID2 - 070698",
                    "FinalID2 - 078919", "FinalID2 - 078920", "FinalID2 - 078921", "FinalID2 - 078922", "FinalID2 - 078924", "FinalID2 - 078925",
                    "FinalID2 - 123837", "FinalID2 - 069204", "FinalID2 - 069205", "FinalID2 - 069206", "FinalID2 - 069207", 
                    "FinalID2 - 069208", "FinalID2 - 069210", "FinalID2 - 069214", "FinalID2 - 069215", "FinalID2 - 069216",
                    "FinalID2 - 069217", "FinalID2 - 069219", "FinalID2 - 069220", "FinalID2 - 069222",
                    "FinalID2 - 078217", "FinalID2 - 078218", "FinalID2 - 078219", "FinalID2 - 078220", "FinalID2 - 078221", "FinalID2 - 078222", "FinalID2 - 078223",
                    "FinalID2 - 078224", "FinalID2 - 078225", "FinalID2 - 078229", "FinalID2 - 078230", "FinalID2 - 078233", "FinalID2 - 078235", "FinalID2 - 078238",
                    "FinalID2 - 078239", "FinalID2 - 078240", "FinalID2 - 078241", "FinalID2 - 078243", "FinalID2 - 078246", "FinalID2 - 078247", "FinalID2 - 078250", "FinalID2 - 078251",
                    "FinalID2 - 078252", "FinalID2 - 078253", "FinalID2 - 078254", "FinalID2 - 078255", "FinalID2 - 078256", "FinalID2 - 078257", "FinalID2 - 078258", "FinalID2 - 078259",
                    "FinalID2 - 078260", "FinalID2 - 078261", "FinalID2 - 078262", "FinalID2 - 078263", "FinalID2 - 078264", "FinalID2 - 078265", "FinalID2 - 078266", "FinalID2 - 078268"]

    # FinalID2s of PAs that should be marked as CF
    # As of 1/2026 these are all Downeast Lakes CF tracts determined to be CF based on personal communication
    # In the update type function above, the default is to not change any rows that are CF, so this
    # code should not be needed unless CF is overwritten, in which case we will need to correct these rows
    # which are CF but don't have "community forest" in the AreaName (and so wouldn't be marked CF in code above)
    cf_corr_ids = ["FinalID2 - 066922",
                   "FinalID2 - 066692", "FinalID2 - 066693", "FinalID2 - 066694", "FinalID2 - 066695",
                   "FinalID2 - 069629", "FinalID2 - 069630", "FinalID2 - 069631", "FinalID2 - 069632", "FinalID2 - 069633", "FinalID2 - 069634",
                   "FinalID2 - 069635", "FinalID2 - 069636", "FinalID2 - 069637", "FinalID2 - 069638", "FinalID2 - 069639", "FinalID2 - 069640",
                   "FinalID2 - 069641", "FinalID2 - 069642", "FinalID2 - 069643", "FinalID2 - 069644", "FinalID2 - 069645", "FinalID2 - 069646",
                   "FinalID2 - 069647", "FinalID2 - 069648", "FinalID2 - 069649", "FinalID2 - 069650"]

    # Go through data, checking FinalID2 to find rows for correction
    with arcpy.da.UpdateCursor(fc, corr_fields) as cur:
        for row in cur:
            if row[0] in prmu_corr_ids:
                row[1] = "PrMu"
                cur.updateRow(row)
                continue
            if include_lpt_cf == True:
                if row[0] in lpt_corr_ids:
                    row[1] = "LPT"
                    cur.updateRow(row)
                    continue
                if row[0] in cf_corr_ids:
                    row[1] = "CF"
                    cur.updateRow(row)
                    continue

    print("Done")

update_type(state=None, sql=None)
correct_type()