# Wildlands  are handled specially because they can have complex relationships
# with underlying NEPOS (e.g., carveouts). This script only populates
# WildYear is there is a match code 1, the best possible match. All other
# match codes are looked at in ArcPro and handled manually.
#
# You can run this function first and then all the fields will be joined
# to continue exploration of other match codes in ArcPro.
#
# Presence of any value (even 0) in WildYear is what determines whether an NEPOS
# polygon is a wildland based on WWF&C data. A wildland may or may not have
# PolySource from WWF&C Wildlands (that depends on whether the WWF&C polygon
# is unique or if it matches source data), but all wildlands will have
# WildYear populated even if it is with 0 (unknown).
#
# Prior to running this script, it is expected that spatial matching was done
# between NEPOS and wildlands using 02_spatial_matching.py and a spatial match
# table was created using 04_find_matching_polygons.
#
# This script reads in the match table, joins the relevant fields, and also
# joins WildYear from the wildlands data. If match code is 1, it updates
# WildYear. You can choose to only update rows where WildYear is NULL
# or update all WildYear.
#
# NOTE: You will need to comment/uncomment lines as you need them - all
# the function calls for all states are in this script but you work
# state by state so any ones you aren't using need to be commented out!
#
# Lucy Lee, 12/2025

import arcpy

# Workspace GDB where both NEPOS and wildlands live
arcpy.env.workspace = "D:/Lee/POS/Update_2023/Data/new_data2.gdb"

# Function to populate WildYear based on match with wildlands
# In this function we only update WildYear for rows where
# match code equals 1 - you could change that
# in the code body but I really recommend looking at wildlands directly
# because sometimes the geometries are quite unique.
# Arguments:
#  - nepos (text): name of NEPOS layer
#  - wildlands (text): name of wildlands layer
#  - wildlands_desc (text): text to populate Source_WildYear
#                           e.g. "WWF&C Wildlands 4/2022"
#  - match_table (text): full path to wildlands match table for the state
#  - state (text): two-letter state abbreviation (CT, MA, RI, VT, NH, ME)
#  - null_only (boolean): should the function only update rows where
#                         WildYear IS NULL? If True, any existing WildYear
#                         will NOT be overwritten.
def update_wild_year(nepos, wildlands, wildlands_desc, match_table, state, null_only=True):
    # First join match_type_code from match table based in FinalID2
    arcpy.management.JoinField(nepos, "FinalID2", match_table, "FinalID2",
                               ["match_type_code", "UID2"])
    print("Joined match_type_code and UID2 based on FinalID2...")

    # Then join WildYear and UID from the wildlands data to NEPOS
    # Using UID2 as the join field. We want UID because UID2 is not
    # in a consistent pattern that easily allows us to extract UID from UID2
    arcpy.management.JoinField(nepos, "UID2", wildlands, "UID2",
                               ["WildYear", "UID"])
    print("Joined WildYear and UID from wildlands data based on UID2...")

    # Now we can update WildYear
    fields = ["match_type_code", "WildYear_1", "UID",
              "WildYear", "Source_WildYear", "Source_WildYear_FeatID"]
    
    # Subset NEPOS by state and (optionally) existing WildYear
    if null_only == False:
        query = "State = '" + state + "'"
    elif null_only == True:
        query = "State = '" + state + "' AND WildYear IS NULL"

    c = 0
    with arcpy.da.UpdateCursor(nepos, fields, query) as cur:
        for row in cur:
            if row[0] == 1:
                row[3] = row[1]
                row[4] = wildlands_desc
                row[5] = row[2]
                cur.updateRow(row)
                c = c + 1
    print(f"Populated WildYear for {c} rows")


###### Shared variables #####
# Update for next update! These are from 2025
desc = "WWF&C Wildlands 4/2022"     # To populate Source_WildYear
wildlands = "wildlands_albers_sp"   # File name

##### CT FUNCTION CALL #####
nepos = "POS_v2_20_sp"
ct_wild_match_table = "D:/Lee/POS/Update_2023/Data/matching/ct_wildlands_match_table_2025-02-13_full.csv"
#update_wild_year(nepos, wildlands, desc, ct_wild_match_table, "CT", True)

###### MA FUNCTION CALL ######
nepos = "POS_v2_22_sp"
ma_wild_match_table = "D:/Lee/POS/Update_2023/Data/matching/nepos_wildlands_match_table_2025-03-17_full.csv"
#update_wild_year(nepos, wildlands, desc, ma_wild_match_table, "MA", True)

##### ME FUNCTION CALL ######
nepos = "POS_v2_23_sp"
me_wild_match_table = "D:/Lee/POS/Update_2023/Data/matching/nepos_wildlands_match_table_2025-04-03_full.csv"
#update_wild_year(nepos, wildlands, desc, me_wild_match_table, "ME", True)

##### NH FUNCTION CALL #####
nepos = "POS_v2_25_sp"
nh_wild_match_table = "D:/Lee/POS/Update_2023/Data/matching/nepos_wildlands_match_table_2025-04-18_full.csv"
#update_wild_year(nepos, wildlands, desc, nh_wild_match_table, "NH", True)

##### RI FUNCTION CALL ######
nepos = "POS_v2_26_sp"
ri_wild_match_table = "D:/Lee/POS/Update_2023/Data/matching/nepos_wildlands_match_table_2025-05-07_full.csv"
#update_wild_year(nepos, wildlands, desc, ri_wild_match_table, "RI", True)

###### VT FUNCTION CALL ######
nepos = "POS_v2_28_sp"
vt_wild_match_table = "D:/Lee/POS/Update_2023/Data/matching/nepos_wildlands_match_table_2025-05-15_full.csv"
#update_wild_year(nepos, wildlands, desc, vt_wild_match_table, "VT", True)