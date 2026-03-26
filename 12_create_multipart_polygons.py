# Creates multipart polygons
#
# Notes:
#  - Need to exclude 'Unknown' AreaName rows, 'Unknown' FeeOwner, 'Unknown' IntHolder
#  - Should unknown dates also be excluded from being multipart or no???
#
# Lucy Lee, 7/2025

import time
start_time = time.time()
import arcpy
import sys
import traceback

# GDB where NEPOS lives
arcpy.env.workspace = "D:\\Thompson_Lab_POS\\Data\\Old_GDBs_Data\\Update_2025_v2\\ct_2003_correction\\ct_2003_correction.gdb"
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


# Function to export 2 subsets of NEPOS - one which we have good information for and will
# get dissolved based on standard attributes, and 1 which is missing some information or has
# vague information for a key attribute (for example, fee owner or area name). 
#
# The function queries data based on the criteria that make some less certain and so we
# add the additional check of using PolySource and PolySource_FeatID as dissolve fields
# (in other words, if the source said they were the same place, then we allow it, otherwise
# we keep them separate because we can't be sure due to vague/missing information).
#
# The criteria include rows where 1) FeeOwner = 'Private' and there is some generic AreaName,
# 2) there is a generic AreaName, 3) FeeOwner = 'Unknown', 4) IntHolder1Type = 'UNK',
# 5) YearProt = 0
#
# In essence these are years where there is either some missing crucial attribute (interest holder, fee owner, year, area name)
# OR the attributes are too general to definitively say an area is one area based solely on those attributes
# (e.g., rows where FeeOwner is general 'Private' and AreaName is something like 'Vermont Land Trust Easement')
#
# The function uses one query that defines these rows that need an extra check, and uses the inverse function
# in Select by Attribute to export the opposite of the query
#
# The function returns a list of the 2 subsets which can be used in further functions.
#
# NOTE: It is important to check the data with each update for any new things that should be added.
# This can also be done after making multipart polygons the first time and scanning/querying the data.
def export_subsets(nepos):
    # Query that defines the rows that DO need to be checked with PolySource because of incomplete or overly general attributes
    query = ("(FeeOwner = 'Private' AND (AreaName = 'Unknown' OR AreaName LIKE 'Vermont Land Trust%' OR AreaName LIKE 'Upper Valley Land Trust%' OR AreaName = 'The Nature Conservancy Easement'"
    " OR AreaName = 'Vermont Fish & Wildlife Easement' OR AreaName = 'Farmland PDR' OR AreaName = 'Wetland Reserve Program Easement' OR AreaName = 'Vermont River Conservancy Easement'"
    " OR AreaName LIKE 'Vermont Land Trust Covenant%' OR AreaName = 'NHDOT Mitigation' OR AreaName = 'New England Forestry Foundation Easement' OR AreaName = 'Bellamy Reservoir Easements'"
    " OR AreaName = 'Middlebury Area Land Trust Easement' OR AreaName = 'Stowe Land Trust Easement' OR AreaName LIKE 'Farm and Ranch Lands Protection Program (FRPP)%'"
    " OR AreaName = 'Farm Service Agency Easement' OR AreaName = 'Northern Rivers Land Trust Easement' OR AreaName = 'Sharon Land Trust Easement #'"
    " OR AreaName = 'The Nature Conservancy Restriction' OR AreaName = 'Kent Land Trust Easement #' OR AreaName = 'Goshen Land Trust Easement'"
    " OR AreaName = 'Grasslands Reserve Program (GRP)' OR AreaName = 'Private Donation' OR AreaName LIKE 'Agricultural Conservation Easement Program%'"
    " OR AreaName = 'Housatonic Valley Association Easement' OR AreaName = 'ACOE Easement' OR AreaName = 'Wetlands Reserve Program' OR AreaName = 'Salisbury Association Land Trust Easement'"
    " OR AreaName = 'Richmond Land Trust Easement' OR AreaName = 'VT Dept. of Environmental Conservation Easement'"
    " OR AreaName = 'FSA Easement' OR AreaName = 'VHCB Easement' OR AreaName = 'American Farmland Trust Easement'"
    " OR AreaName = 'Vermont Land Trust and VTDFW Easement' OR AreaName = 'Town of Litchfield Land'))"
    " OR IntHolder1Type = 'UNK' OR FeeOwner = 'Unknown' OR FeeOwner = 'Maine Minor Civil Division' OR FeeOwner = 'Municipal'"
    " OR AreaName = 'Vermont Land Trust' OR AreaName = 'Brewster Uplands Conservation Trust' OR AreaName = 'Town of Hatfield CR' OR AreaName = 'Cape Cod Land Bank Acquisition'"
    " OR AreaName LIKE 'The Nature Conservancy%' OR AreaName = 'Town of Brookline Land' OR AreaName = 'Greensboro Land Trust' OR AreaName = 'Conservation Area'"
    " OR AreaName = 'Upper Valley Land Trust' OR AreaName LIKE 'Unknown%' OR AreaName = 'Pennichuck Water Works' OR AreaName = 'Barnstable Land Trust CR'"
    " OR AreaName = 'Blue Hills Foundation' OR AreaName = 'Nissitissit River Land Trust' OR AreaName = 'Southside Community Land Trust'"
    " OR AreaName = 'Grafton Pond Land Trust' OR AreaName = 'Middlebury Area Land Trust' OR AreaName = 'Stowe Land Trust' OR AreaName = 'Goshen Land Trust'"
    " OR AreaName = 'Land Trust' OR AreaName = 'Litchfield Land Trust' OR AreaName = 'Sharon Land Trust' OR AreaName = 'Wyndham Land Trust'"
    " OR YearProt = 0")

    # Export the rows that fit the query
    dc = arcpy.management.SelectLayerByAttribute(nepos, "NEW SELECTION", query)
    arcpy.conversion.ExportFeatures(dc, "POS_sp_for_mp_check_polysource")

    # Export the inverse of the query
    inv = arcpy.management.SelectLayerByAttribute(nepos, "NEW_SELECTION", query, "INVERT")
    arcpy.conversion.ExportFeatures(inv, "POS_sp_for_mp_reg")
    
    # Return both files as a list
    return(["POS_sp_for_mp_check_polysource", "POS_sp_for_mp_reg"])

# Function to do the dissolving that creates multipart features
def create_multipart(fc):
    # Output file name for multipart features
    output = "POS_mp"

    # We dissolve on all fields except type, because there may be pieces of an LPT
    # that are not called LPT yet (ideally, this would be the case but need to check for this issue)
    dissolve_fields = ["State", "AreaName", "FeeOwner", "FeeOwnType", "FeeOwnCat", "ProtType",
                       "IntHolder1", "IntHolder1Type", "IntHolder2", "IntHolder2Type", 
                       "YearProt", "FeeYear", "EaseYear", "WildYear", 
                       "GapStatus", "PubAccess", "ProtDuration", "Area_Owner_Name", "LPT_Num"]

    # For stats fields, we want to sum the area fields,
    # take the most common type value (these should all match but there could be cases where
    # PrMu polygons are dissolved into LPT), and for FinalID we want to take the mode.
    # This will result in the most common existing FinalID being preserved for rows that have
    # FinalID populated. Rows that do not have FinalID populated (i.e., new data) need to have
    # that attribute populated after this script.
    # FinalID2 is useless in a multipart dataset as the IDs are designed to be for singlepart polygons.
    # NOTE: Would it be useful to then join FinalID to rows in the SP dataset that don't have FinalID??? Hmmm...
    stats_fields = [["Area_Ac", "SUM"], ["Area_Ha", "SUM"], ["type", "CONCATENATE"],
                    ["FinalID", "CONCATENATE"], ["ProtTypeComments", "CONCATENATE"],
                    ["YearProtComments", "CONCATENATE"], ["FeeOwnCatComments", "CONCATENATE"],
                    ["Comments", "CONCATENATE"]]

    arcpy.analysis.PairwiseDissolve(fc, output, dissolve_fields, stats_fields, 
                                    multi_part="MULTI_PART", concatenation_separator=" / ")
    
    print("Created multipart features...")
    return(output)

def create_multipart_check_polysource(fc):
    output = "POS_mp_check_polysource"
    dissolve_fields = ["State", "AreaName", "FeeOwner", "FeeOwnType", "FeeOwnCat", "ProtType",
                       "IntHolder1", "IntHolder1Type", "IntHolder2", "IntHolder2Type", 
                       "YearProt", "FeeYear", "EaseYear", "WildYear", "LPT_Num",
                       "GapStatus", "PubAccess", "ProtDuration", "Area_Owner_Name", "PolySource", "PolySource_FeatID"]
    stats_fields = [["Area_Ac", "SUM"], ["Area_Ha", "SUM"], ["type", "CONCATENATE"],
                    ["FinalID", "CONCATENATE"], ["ProtTypeComments", "CONCATENATE"],
                    ["YearProtComments", "CONCATENATE"], ["FeeOwnCatComments", "CONCATENATE"],
                    ["Comments", "CONCATENATE"]]
    arcpy.analysis.PairwiseDissolve(fc, output, dissolve_fields, stats_fields, 
                                    multi_part="MULTI_PART", concatenation_separator=" / ")
    print("Created multipart features...")
    return(output)

# Function that takes a concatenated field -- which can have many repeat values -- 
# and populates a new field with just unique values separated by a forward slash
# Function takes 3 args: a feature class (object), name of the concatenated field (text),
# and name of the simplified field (text)
def parse_concat_field(fc, concat_field, simplified_field):
    with arcpy.da.UpdateCursor(fc, [concat_field, simplified_field]) as cur:
        for row in cur:
            if row[0] is not None:                       # Check for Null otherwise split() throws an error
                items = row[0].split(' / ')              # Split concatenated field into a list of values
                unique_items = list(set(items))          # Reduce to unique values only
                new_value = (' / ').join(unique_items)   # Recreate string structure using reduced values
                row[1] = new_value                       # Poplate and update simplified field
                cur.updateRow(row)

# Setting merged statistics fields (from PairwisePissolve) back to original names and cleaning up fields
def refine_multipart_attributes(fc):
    # CONCATENATE fields (text fields)
    # Create new fields with original field names and populate
    # using unique values
    arcpy.management.AddField(fc, "type", "TEXT", field_length=30)
    parse_concat_field(fc, "CONCATENATE_type", "type")

    arcpy.management.AddField(fc, "ProtTypeComments", "TEXT", field_length = 200)
    parse_concat_field(fc, "CONCATENATE_ProtTypeComments", "ProtTypeComments")

    arcpy.management.AddField(fc, "YearProtComments", "TEXT", field_length=300)
    parse_concat_field(fc, "CONCATENATE_YearProtComments", "YearProtComments")

    arcpy.management.AddField(fc, "FeeOwnCatComments", "TEXT", field_length=200)
    parse_concat_field(fc, "CONCATENATE_FeeOwnCatComments", "FeeOwnCatComments")

    arcpy.management.AddField(fc, "Comments", "TEXT", field_length=255)
    parse_concat_field(fc, "CONCATENATE_Comments", "Comments")

    # SUM fields (Area_Ha and Area_Ac)
    arcpy.management.AddField(fc, "Area_Ha", "DOUBLE")
    arcpy.management.CalculateField(fc, "Area_Ha", "!SUM_Area_Ha!")

    arcpy.management.AddField(fc, "Area_Ac", "DOUBLE")
    arcpy.management.CalculateField(fc, "Area_Ac", "!SUM_Area_Ac!")

    print(f"Cleaned up stats fields in {fc}...")

    # Delete old fields
    deletes = ["CONCATENATE_type", "CONCATENATE_ProtTypeComments", "CONCATENATE_YearProtComments",
               "CONCATENATE_FeeOwnCatComments", "CONCATENATE_FinalID", "CONCATENATE_Comments", "SUM_Area_Ha", "SUM_Area_Ac"]
    arcpy.management.DeleteField(fc, deletes)
    print("Deleted old stats fields...")
    

# NOTE: Before this merge will work, need to rename the stats fields
# in the multipart FC to their original names (e.g., SUM_Area_Ac --> Area_Ac)
# Should the fields be altered or new fields created with the original field
# lengths? Depends I suppose...
def recreate_full_nepos(sp, mp):
    output = "POS_final"
    arcpy.management.Merge([sp, mp], output)
    print("Merged singlepart and multipart NEPOS...")

    # Delete extra fields used in double check mp fc
    deletes = ["PolySource", "PolySource_FeatID"]
    arcpy.management.DeleteField(output, deletes)
    print("Deleted extra fields...")

    return(output)

# Assign FinalID to the final merged multipart NEPOS based on OBJECTID
# IMPORTANT: This means that FinalID is NOT a persistent field across versions of NEPOS
# It is recreated every time the multipart data is and can change over time for a given PA
# The more persistent identifer is FinalID2 in the singlepart data, but even this can change
# (e.g., if a polygon is subdivided or wildlands are carved out).
def assign_finalid(mp_fc):
    arcpy.management.AddField(mp_fc, "FinalID", "TEXT", field_length=25)
    arcpy.management.CalculateField(mp_fc, "FinalID", "'FinalID - ' + str(!OBJECTID!).zfill(6)")

def spatial_join_id(sp_fc, mp_fc):
    # Create points of singlepart FC
    # Very important to make sure the points are within the polygons - this is crucial for the join to work properly
    sp_pt = f"{sp_fc}_pt"
    arcpy.management.FeatureToPoint(sp_fc, sp_pt, "INSIDE")

    # Join the merged multipart FC to the singlepart points
    # This will give us the multipart FinalID and the singlepart FinalID2 in one table
    arcpy.analysis.SpatialJoin(sp_pt, mp_fc, "NEPOS_join_mp_to_sp_pt", join_operation="JOIN_ONE_TO_ONE",
                               match_option="WITHIN")
    
    # Return join output for use in next function (JoinField)
    return("NEPOS_join_mp_to_sp_pt")

# Join the multipart FinalID field from the spatial join output to the singlepart polygon NEPOS
# Using FinalID2 as the join field in both FCs
# Note that since FinalID already exists in singlepart POS, the correct field to join for the multipart FC
# is FinalID_1
def join_mp_finalid(join_fc, sp_poly):
    # Join the multipart FinalID field to the singlepart NEPOS
    arcpy.management.JoinField(sp_poly, "FinalID2", join_fc, "FinalID2", "FinalID_1")

    # Copy the multipart FinalID into the FinalID field in the singlepart data
    arcpy.management.CalculateField(sp_poly, "FinalID", "!FinalID_1!")

    # Delete the previously joined field
    arcpy.management.DeleteField(sp_poly, "FinalID_1")
    
    print("Joined and copied multipart FinalID into singlpart FinalID field...")

try:
    # POS single part internal
    pos = "nepos_v2_0_sp_internal"

    # Separate the parts of NEPOS that will be used
    # for multipart and those that won't
    subsets = export_subsets(pos)
    sp_for_mp_check_polysource = subsets[0]
    sp_for_mp_reg = subsets[1]

    # Create multipart features
    # When running for the first time after changes to these functions or the data, 
    # it is important to check these output and make sure the sum of the total areas in the two
    # multipart files equals the area in the singlepart NEPOS used above
    # Matching areas confirms that no rows are lost or double counted in the two multipart files
    mp_check_polysource = create_multipart_check_polysource(sp_for_mp_check_polysource)
    mp = create_multipart(sp_for_mp_reg)

    # Remove duplicate values in concatenated fields
    # It's useful to check data for presence of / in certain fields where this would not be
    # expected, such as type
    refine_multipart_attributes(mp)
    refine_multipart_attributes(mp_check_polysource)

    # Merge multipart and singlepart files together
    merged_nepos = recreate_full_nepos(mp, mp_check_polysource)

    # Assign FinalID to the merged multipart NEPOS
    assign_finalid(merged_nepos)

    # Join the multipart FinalID to singlepart NEPOS and copy into FinalID field
    # This is to have a reference between the singlepart ID (FinaliD2)
    # and the multipart ID (FinalID)
    pos_pt_with_mp_id = spatial_join_id(pos, merged_nepos)

    join_mp_finalid(pos_pt_with_mp_id, pos)

    print("Done")
except Exception:
    print(traceback.format_exc())
    sys.exit()
finally:
    print_elapsed_time()