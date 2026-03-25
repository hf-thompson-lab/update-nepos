# This script contains a series of functions that can be used to update NEPOS attributes.

# Generally, there is one function per attribute (although there are some cases where there are
# multiple functions for one attribute). There are also some "helper" functions that are called
# within the attribute updating functions to reduce redundant lines of code.
#
# The attribute update functions all work in a similar way:
#   * Subset source data using a query that specifies 1) state and 2) if unknown values should be excluded
#     - Relevant function arguments: 'state' and 'take_only_known'
#     - This is important because there are some instances in which we do want to exclude unknown values
#       and others in which we don't.
#     - Excluding unknown rows (take_only_known=True) is most useful when we want to try to replace
#       unknown values with a known value from another source (e.g., RI has no GAP info so we check
#       other sources to try to complete this information, but we don't want to update with another unknown value!)
#     - Including unknown rows (take_only_known=False) is most useful when updating all of the data.
#       If you are trying to update all data, we want to include unknown values in that. Sometimes the
#       best we can do is "unknown" and we don't want those rows getting left behind and never updated.
#     - Best practice if updating all data is to run the function twice, changing the take_only_known argument
#       so that everything gets updated to the latest data and then we go again and fill in missing information.
#   * Use a SearchCursor to create a dictionary of {UID: attribute} pairs for all source datasets
#   * Subset NEPOS based on whether we want to only update unknown values and/or new data
#     - Relevant arguments: unknown_only, null_only, new_data_only
#     - This is like the NEPOS equivalent of what we did when subsetting the source data
#     - There are some cases in which we might only want to look at unknown values and see if we can get better info,
#       either in all data or in newly added rows
#     - In other cases like a complete data update, we would set unknown_only and new_data_only to False
#   * Use an UpdateCursor to iterate through each row in NEPOS:
#     - Use the match table to find the source polygons that match the NEPOS polygon
#     - Use the matching source polygon's UID to get the relevant attribute (e.g., AreaName) from the source
#     - Compare the match code, percent overlap, and (optionally) attribute value to determine
#       if the source polygon is an adequate match, and if so, update the NEPOS attribute
#
# The attribute update functions are written in a specific order of sources - state source is checked first,
# then TNC, then NCED, and then PADUS (generally). The order matters because whatever source has a solid
# match with a valid attribute value will be used first, so the sources should be in order
# from most reliable to least. The order can be changed (or sources removed, as with PADUS in
# the update_area_name function) based on changes in data quality.
#
# By default, attribute update functions are set to update the narrowest scope of rows (new_data_only, unknown_only,
# and take_only_known all set to True). You can set these arguments to different combinations of True/False,
# making the functions suitable for iterating on information completion in existing or new data, or doing
# a complete update where we want to update every attribute for every row to the latest data. These are some
# important combinations of these arguments that are useful:
#   * new_data_only = True, unknown_only = True, take_only_known = True --> getting better info for newly added data
#   * new_data_only = False, unknown_only = True, take_only_known = True --> getting better info for all data
#   * new_data_only = False, unknown_only = False, take_only_known = False --> update all data to latest sources
#   * new_data_only = False, unknown_only = False, take_only_known = True --> update all data to latest source IF source has known value
#
# Note that take_only_known and unknown_only are like twin parameters -- if one is true, it usually makes
# sense for the other to be true as well and vice versa. unknown_only is about unknown values in NEPOS
# while take_only_known is about unknown values in the source data.
#
# The update attribute functions work by updating the relevant NEPOS fields directly. It is strongly
# recommended to make copies of NEPOS frequently so that if you make any mistakes you don't need
# to start a whole state over again.
#
# Also, something you could do with these functions is make a copy of this entire file, and edit the functions
# in the copy so that they add a dummy field (e.g., ProtType2) and populate that dummy field in the UpdateCursor
# instead of directly modifying the real field. This way you can explore rows where the original field is not
# the same as its corresponding dummy and get a sense for how much the data has changed before making
# any lasting changes to NEPOS.
#
# In the 2024 update, separate scripts were created for each state since there were sometimes things specific
# to a state that didn't make sense to do for other states. You can checked those out in FOLDER where
# code is archived to see how it was done. However, I did try to make these functions flexible 
# and useful for all states. I recommend keeping one "clean" copy with the multipurpose functions
# and making any edits that should apply to all states in the main copy. Then you can make copies of that
# file for modifying functions and doing any state-specific work.
#
# This version of the code is updated and was not actually run in the 2024 update process - there may be
# a few bugs to work out. This version was updated to incorporate enhancements not present in the
# version used for the 2024 update.
#
# Lucy Lee, 12/2025


#### LIBRARIES AND SETTINGS ####
import time
start_time = time.time()
import traceback
import arcpy
import pandas as pd
import sys

arcpy.env.workspace = 'D:/Lee/POS/Update_2023/Data/new_data2.gdb/'

#### SET GLOBAL VARIABLES ####
# These are variables that are called in multiple functions, including data layers and
# other variables that are used repeatedly across functions

### DATA LAYERS ###
## NEPOS ##
# All attribute and geometry updates should be done on the latest SINGLEPART version of NEPOS
# Located in a geodatabase
# You might want to consider making copies of NEPOS frequently in case one of these functions
# does not work as you intended or you make a mistake
# At minimum, you should make a copy after each state is completed, but it can be helpful
# to make copies for frequently than this
pos = ""

## SOURCES ##
# Source spatial data (outputs of recode_source_data.py)
# Located in same GDB as NEPOS
tnc = 'TNC_SA2022_albers_sp'
nced = 'NCED_albers_sp'
padus = 'PADUS4_0Fee_Easement_NE_sp'
bh = "POS_from_Brian_Hall_albers_sp"
deep = "CT_DEEP_albers_sp"
wild = "wildlands_albers_sp"
massgis = "MassGIS_OpenSpace_albers_sp"

### OTHER VARIABLES ###
## SOURCE DESCRIPTIONS ##
# The text that populates Source_AreaName, etc.
# These few lines of code set the source descriptions for multi-state data sources
# The name should stay the same unless you are going to update all rows to match
# (we want source descriptions to be consistent and streamlined across years).
# We should be able to query the data for Source_IntHolder1 LIKE 'NCED%' and get all rows where
# IntHolder1 is from NCED, even if they are from different release years (e.g., NCED 7/2023, NCED 7/2024).
# NOTE: DATES SHOULD BE UPDATED to reflect the date data was last updated at time of use.
# NOTE: See get_state_info() for state dataset descriptions
# NOTE: See get_local_info() for RI Local description
nced_src = "NCED 7/2024"
tnc_src = "TNC SA2022"
padus_src = "USGS PAD-US v4.0"
wildlands_src = "WWF&C Wildlands 4/2022"

## MATCH CODES AND PERCENT OVERLAP ##
# These variables are used to define what defines a good enough polygon match to make
# the polygon usable for updating attributes.
# In all of the attribute update functions, there are lines like this for each source:
#
# if ((min_match_code <= state_match_code <= max_match_code or (state_match_code == 10 and state_pct_overlap >= min_pct_overlap)) 
#
# Those lines use these variables to determine which source polygons will be used.
# NOTE: Match code 10 refers to the relationship when there are multiple NEPOS polygons corresponding to 1 source polygons -
# this suggests that NEPOS has more detail than the source dataset (e.g., in WMNF in NH) and if there is high % overlap,
# we can be confident that it is acceptable to take attributes from the larger source polygon.
# NOTE: While these variables are defined up here, you can also redefine these variables later in this script just
# before calling an attribute update function, if you want to use different options. These are just here to be the default values.
# To explore what other values you might choose, I recommend exploring the match codes and % overlap in the data to see
# how the match codes and % overlap play out in the data. (To do this, you just have to join the match table CSV to NEPOS
# by FinalID2).
min_match_code = 1       # min_match_code should never be less than 1
max_match_code = 4       # max_match_code should never be more than 6 in my experience
min_pct_overlap = 90.0   # recommend keeping this at 90 or higher, but 85 would possibly be okay too

#### HELPER FUNCTIONS ####
### Function to print elapsed time running script ###
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

### Function to retrieve the column names from a state match table and to populate ###
# the Source_[Field] fields based on state
# This function is called from within other functions below when a state match table is used
# NOTE: The state_src variables in this function NEED TO BE UPDATED with each update to reflect the new source date
# The date for each state_src should be the date the source data was last updated at time of download/use
# Argument: state (text): two-letter state abbreviation in capital letters (CT, MA, RI, VT, NH, ME)
# Returns: a list of 4 strings - 3 column names from the match table and the source description
def get_state_info(state):
    if state == 'ME':
        state_id_col = 'megis_id'
        state_code_col = 'megis_match_code'
        state_pct_overlap_col = 'megis_pct_overlap'
        state_src = 'MEGIS 3/2025'     # Update this for next update!
    elif state == 'MA':
        state_id_col = 'massgis_id'
        state_code_col = 'massgis_match_code'
        state_pct_overlap_col = 'massgis_pct_overlap'
        state_src = 'MassGIS 1/2025'  # Update this for next update!
    elif state == 'NH':
        state_id_col = 'nh_id'
        state_code_col = 'nh_match_code'
        state_pct_overlap_col = 'nh_pct_overlap'
        state_src = 'NH Conservation Public Lands 3/2025'   # Update this for next update!
    elif state == 'RI':
        state_id_col = 'ri_state_id'
        state_code_col = 'ri_state_match_code'
        state_pct_overlap_col = 'ri_state_pct_overlap'
        state_src = 'RI State Conservation Areas 2/2025'  # Update this for next update!
    elif state == 'VT':
        state_id_col = 'vt_id'
        state_code_col = 'vt_match_code'
        state_pct_overlap_col = 'vt_pct_overlap'
        state_src = 'VT Protected Lands Database 6/2021'   # Update this for next update!
    return([state_id_col, state_code_col, state_pct_overlap_col, state_src])

### Function for RI Local dataset - same functionality as above function ###
# NOTE: This function contains the source description for RI Local and must be updated
# Arguments: None
# Returns: a list of 4 strings - 3 column names from the match table and the source description
def get_local_info():
    local_id_col = 'ri_local_id'
    local_code_col = 'ri_local_match_code'
    local_pct_overlap_col = 'ri_local_pct_overlap'
    local_src = 'RI Local Conservation Areas 4/2025'  # Update this for next update!
    return([local_id_col, local_code_col, local_pct_overlap_col, local_src])

### Function to retrieve the state UID based on state abbreviation ###
# This function assumes that all states except MA have a UID stucture like 000000 (no dashes), and MA
# has a structure like 00-000 (two items separated by a dash).
# This function may need to be updated if UIDs change for data sources in the future.
# This function takes an ID (UID2 from the match table, which is the single part ID and may have a dash for multipart polygons 
# that were made into singlepart polygons), and reconstructs the original (potentially multipart) ID from the source polygon.
# Example: a multiple polygon with UID 3A in source data is turned into 3 polygons with ID 3A-1, 3A-2, 3A-3 for purposes of updating NEPOS.
# This function reconstructs the original UID for populating source fields so that it can be more easily linked to the source data.
# Arguments: 
#   src (text): two-letter state abbreviation in capital letters (CT, MA, RI, VT, NH, ME)
#   id (text, integer): the UID2 pulled from the match table
# Returns:
#   The original UID of the source polygon as a string
def get_src_orig_id(src, id):
    if src != 'MA':
        state_orig_id = str(id).split('-')[0]
    elif src == 'MA':
        state_orig_id_parts = str(id).split('-')
        state_orig_id = '-'.join(state_orig_id_parts[0:2])   # MA has OS_ID like 00-00
    return(state_orig_id)

### Function to get the attribute from the source dictionary of {UID: attribute} pairs by UID ###
# First it attempts to get it with UID as a string, if that doesn't work it tries with UID as an integer.
# If that still doesn't work we print an exception and the match code will be set to -1 so that
# that source does not get used for that row.
# Arguments: 
#   src_dict (dictionary): the dictionary of {UID: attribute} pairs for the source dataset
#   src_id (text, integer): the source UID used to access the attribute stored in the dictionary
#                           (UID of source polygon that matches an NEPOS polygon)
# Returns:
#   If successful, returns the attribute value (e.g., AreaName, FeeOwner, etc.)
#   If there is an error, it rases an exception to trigger the exception handling in
#     the attribute update functions, which set the match code to -1 for that source
def get_source_attribute(src_dict, src_id):
    # Try to get the attribute with state_orig_id
    try:
        attribute = src_dict[src_id]
        return(attribute)
    # If that doesn't work, try wrapping the ID in int() -- there have been some inconsistencies in this
    # and some datasets are int and others are string
    except Exception:
        try:
            attribute = src_dict[int(src_id)]
            return(attribute)
        except Exception:
            raise Exception(f"Cannot retrieve attribute for feature {src_id}")


#### ATTRIBUTE UPDATE FUNCTIONS ####
#### AREA NAME ####
### Function to update AreaName ###
# If using to update Unknown area names, best to run consolidate_unknown_area_names() first
# This function updates AreaName if the match is adequate AND the source has a value for 
# AreaName that is not 'Unknown' or empty - it can be useful to summarize the AreaName field
# of the different sources to check that there are no values that should be added to the checking
# for unknown values below.
# Arguments:
#  - state (text): two-letter state abbreviation in capital letters (CT, MA, RI, VT, NH, ME)
#  - state_fc (text): name of the preprocessed, singlepart state data source
#                     it should be in same GDB as NEPOS so should only need name, not full path
#  - match_table (pandas df): dataframe resulting from reading in match table CSV with pd.read_csv()
#  - unknown_only (boolean): should area name be updated only for rows where AreaName is unknown or empty?
#  - new_data_only (boolean): should area name be updated only for new rows?
#                             new rows are identified by lack of FinalID
#  - take_only_known (boolean): should we only update NEPOS if the source has a known value (not unknown)?
#  - local_fc (text): RI only - name of preprocessed, singlepart RI Local data source
def update_area_name(state, state_fc, match_table, unknown_only=True, new_data_only=True, take_only_known=True, local_fc=None):
    # Subset source by state and optionally, values (update second SQL query to add any other 'unknown' variations that may exist)
    if take_only_known == False:
        src_query = "State = '" + state + "'"
    elif take_only_known == True:
        src_query = "State = '" + state + "' AND AreaName IS NOT NULL AND AreaName <> '' AND AreaName <> ' ' AND LOWER(AreaName) NOT LIKE '%unknown%'"
    
    # Create dictionaries of {UID: AreaName} pairs for each source
    tnc_area_names = {key: value for (key, value) in arcpy.da.SearchCursor(tnc, ['UID', 'AreaName'], where_clause=src_query)}
    nced_area_names = {key: value for (key, value) in arcpy.da.SearchCursor(nced, ['UID', 'AreaName'], where_clause=src_query)}
    # NOTE: PADUS has a weird thing with unknown area names where they assign the area name to be the fee owner / interest holder
    # plus a number (e.g., Town of Petersham 1, Town of Petersham 2). We don't want this so not going to use PADUS to update AreaNames
    # padus_area_names = {key: value for (key, value) in arcpy.da.SearchCursor(padus, ['UID', 'AreaName'], where_clause=src_query)}
    state_area_names = {key: value for (key, value) in arcpy.da.SearchCursor(state_fc, ['UID', 'AreaName'], where_clause=src_query)}
    if local_fc is not None:
        local_area_names = {key: value for (key, value) in arcpy.da.SearchCursor(local_fc, ['UID', 'AreaName'], where_clause=src_query)}
        local_items = get_local_info()
        local_id_col = local_items[0]
        local_code_col = local_items[1]
        local_pct_overlap_col = local_items[2]
        local_src = local_items[3]

    # Subset NEPOS for UpdateCursor based on function parameters
    if unknown_only == True and new_data_only == True:
        query = "State = '" + state + "' AND (AreaName IS NULL or LOWER(AreaName) LIKE '%unknown%' OR AreaName = '' OR AreaName = ' ') AND FinalID IS NULL"
    elif unknown_only == True and new_data_only == False:
        query = "State = '" + state + "' AND (AreaName IS NULL or LOWER(AreaName) LIKE '%unknown%' OR AreaName = '' OR AreaName = ' ')"
    elif unknown_only == False and new_data_only == True:
        query = "State = '" + state + "' AND FinalID IS NULL"
    elif unknown_only == False and new_data_only == False:
        query = "State = '" + state + "'"

    # Get the column names for the state match table and the state source name using get_state_info()
    # This is a list - need to separate each item in the list 
    state_items = get_state_info(state)
    state_id_col = state_items[0]
    state_code_col = state_items[1]
    state_pct_overlap_col = state_items[2]
    state_src = state_items[3]

    # Update AreaName field based on either PolySource_FeatID or match code for polygons that weren't updated
    c = 0   # To count rows updated
    fields = ['FinalID2', 'AreaName', 'Source_AreaName', 'Source_AreaName_FeatID']
    with arcpy.da.UpdateCursor(pos, fields, query) as cur:
        for row in cur:
            # If a manual AreaName has been set, we skip this row and do not update AreaName
            # There are not many of these and they can be checked manually if desired
            if 'Harvard Forest' in row[2]:
                continue
                
            ##### Retrieve the AreaName from the matching source polygons based on FinalID2 ######
            # For RI only - RI Local
            if local_fc is not None:
                local_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [local_id_col, local_code_col, local_pct_overlap_col]].drop_duplicates()
                local_matched_src_id = local_match_ss.iloc[0, 0]
                local_match_code = local_match_ss.iloc[0, 1]
                local_pct_overlap = local_match_ss.iloc[0, 2]
                local_orig_id = get_src_orig_id(state, local_matched_src_id)
                try:
                    local_area_name = get_source_attribute(local_area_names, local_orig_id)
                except Exception:
                    print(f'Setting match code to -1 for {state} Local feature {local_orig_id}')
                    local_match_code = -1
            # State match codes
            state_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [state_id_col, state_code_col, state_pct_overlap_col]].drop_duplicates()
            state_matched_src_id = state_match_ss.iloc[0, 0]
            state_match_code = state_match_ss.iloc[0, 1]
            state_pct_overlap = state_match_ss.iloc[0, 2]
            state_orig_id = get_src_orig_id(state, state_matched_src_id)
            try:
                state_area_name = get_source_attribute(state_area_names, state_orig_id)
            except Exception:
                print(f'Setting match code to -1 for {state} feature {state_orig_id}')
                state_match_code = -1     # Setting this to -1 ensures the source won't be used for this row
            
            # TNC match codes
            tnc_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["tnc_id", "tnc_match_code", "tnc_pct_overlap"]].drop_duplicates()
            tnc_matched_src_id = tnc_match_ss.iloc[0, 0]
            tnc_match_code = tnc_match_ss.iloc[0, 1]
            tnc_pct_overlap = tnc_match_ss.iloc[0, 2]
            tnc_orig_id = get_src_orig_id('TNC', tnc_matched_src_id)
            try:
                tnc_area_name = get_source_attribute(tnc_area_names, tnc_orig_id)
            except Exception:
                print(f'Setting match code to -1 for TNC feature {tnc_orig_id}')
                tnc_match_code = -1
    
            # # NCED match codes
            nced_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["nced_id", "nced_match_code", "nced_pct_overlap"]].drop_duplicates()
            nced_matched_src_id = nced_match_ss.iloc[0, 0]
            nced_match_code = nced_match_ss.iloc[0, 1]
            nced_pct_overlap = nced_match_ss.iloc[0, 2]
            nced_orig_id = get_src_orig_id('NCED', nced_matched_src_id)
            try:
                nced_area_name = get_source_attribute(nced_area_names, nced_orig_id)
            except Exception:
                print(f'Setting match code to -1 for NCED feature {nced_orig_id}')
                nced_match_code = -1
            
            # PADUS match codes
            # padus_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["padus_id", "padus_match_code", "padus_pct_overlap"]].drop_duplicates()
            # padus_matched_src_id = padus_match_ss.iloc[0, 0]
            # padus_match_code = padus_match_ss.iloc[0, 1]
            # padus_pct_overlap = padus_match_ss.iloc[0, 2]
            # padus_orig_id = get_src_orig_id('PADUS', padus_matched_src_id)
            # try:
            #     padus_area_name = get_source_attribute(padus_area_names, padus_orig_id)
            # except Exception:
            #     print(f'Setting match code to -1 for PADUS feature {padus_orig_id}')
            #     padus_match_code = -1
            
            # Compare each sources match code (including pct overlap for code 10) and attribute value
            # If the match code is appropriate and AreaName is not unknown, we take AreaName
            # For RI, we want to check the RI Local dataset before State dataset
            if local_fc is not None:
                if (min_match_code <= local_match_code <= max_match_code or (local_match_code == 10 and local_pct_overlap >= min_pct_overlap)):
                    row[1] = local_area_name
                    row[2] = local_src
                    row[3] = local_orig_id 
                    c = c + 1
                    cur.updateRow(row)  # Update row
                    continue            # And continue to next row so line (and entire conditional sequence) below is not run
            if (min_match_code <= state_match_code <= max_match_code or (state_match_code == 10 and state_pct_overlap >= min_pct_overlap)):
                row[1] = state_area_name
                row[2] = state_src
                row[3] = state_orig_id 
                c = c + 1
            elif (min_match_code <= tnc_match_code <= max_match_code or (tnc_match_code == 10 and tnc_pct_overlap >= min_pct_overlap)):
                row[1] = tnc_area_name
                row[2] = tnc_src
                row[3] = tnc_orig_id
                c = c + 1
            elif (min_match_code <= nced_match_code <= max_match_code or (nced_match_code == 10 and nced_pct_overlap >= min_pct_overlap)):
                row[1] = nced_area_name
                row[2] = nced_src
                row[3] = nced_orig_id
                c = c + 1
            # elif (min_match_code <= padus_match_code <= max_match_code or (padus_match_code == 10 and padus_pct_overlap >= min_pct_overlap)):
            #     row[1] = padus_area_name
            #     row[2] = padus_src
            #     row[3] = padus_orig_id
            #     c = c + 1
            cur.updateRow(row)
    print(f'Updated AreaName for {c} rows!')

### Function to specifically update some vague VT area names ###
# VT has a lot of rows where the AreaName is 'TNC Easement' or something like that
# We'll see if we can get a better value from TNC data
# This function could be copied and modified to deal with similar issues in other states
# Arguments: match_table (pandas df): dataframe resulting from reading in match table with pd.read_csv()
def update_vt_tnc_area_name(match_table):
    tnc_area_names = {key: value for (key, value) in arcpy.da.SearchCursor(tnc, ['UID', 'AreaName'], "State = 'VT'")}
    query = "State = 'VT' AND AreaName LIKE 'The Nature Conservancy%'"
    c = 0
    with arcpy.da.UpdateCursor(pos, ['FinalID2', 'AreaName', 'Source_AreaName', 'Source_AreaName_FeatID'], query) as cur:
        for row in cur:
            # TNC match codes
            tnc_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["tnc_id", "tnc_match_code", "tnc_pct_overlap"]].drop_duplicates()
            tnc_matched_src_id = tnc_match_ss.iloc[0, 0]
            tnc_match_code = tnc_match_ss.iloc[0, 1]
            tnc_pct_overlap = tnc_match_ss.iloc[0, 2]
            tnc_orig_id = get_src_orig_id('TNC', tnc_matched_src_id)
            try:
                tnc_area_name = get_source_attribute(tnc_area_names, tnc_orig_id)
            except Exception:
                print(f'Setting match code to -1 for TNC feature {tnc_orig_id}')
                tnc_match_code = -1
            
            if ((min_match_code <= tnc_match_code <= max_match_code or (tnc_match_code == 10 and tnc_pct_overlap >= min_pct_overlap)) 
                and 'The Nature Conservancy' not in tnc_area_name and 'TNC' not in tnc_area_name):
                row[1] = tnc_area_name
                row[2] = tnc_src
                row[3] = tnc_orig_id
                cur.updateRow(row)
                c = c + 1
            else:
                continue
    print(f"Updated AreaName containing 'The Nature Conservancy' for {c} rows...")

### Function to conslidate unknown area names ###
# This can be a useful function to run before update_area_name() especially if you are only
# trying to update rows where AreaName is not known.
# It can be useful to check the most current data for any variations of unknown that might exist
# (like UNK, or unspecified) in case there are any new ones that are not captured in the current code.
# Arguments:
#   - state (text): two letter state abbreviation in capital letters (CT, MA, RI, VT, NH, ME)
#   - new_data_only (boolean): should unknown names be consolidated only for new rows?
def consolidate_unknown_area_names(state, new_data_only = True):
    if new_data_only == False:
        query = "State = '" + state + "'"
    elif new_data_only == True:
        query = "State = '" + state + "' AND FinalID IS NULL"

    with arcpy.da.UpdateCursor(pos, ['AreaName'], query) as cur:
        for row in cur:
            if row[0] is None:
                row[0] = 'Unknown'
            elif row[0].upper() == 'UNK' or row[0] == 'Unknwon' or row[0] == ' ' or row[0]== '' or row[0].lower == 'unspecified' or row[0] == 'unknown':
                row[0] = 'Unknown'
            cur.updateRow(row)
    print('Consolidated some unknown area names')

### Function to apply title case to words ###
# This code changes letters follow apostrophe to capital which is not ideals
# NOTE: There is a more advanced function in one of the update_names scripts that
# could be copied here
def correct_capitalization():
    lowercase = ['Of', 'And', 'For']  # Words to be lowercase as found in the AreaName field
    with arcpy.da.UpdateCursor(pos, 'AreaName', "State = 'CT' AND FinalID IS NULL") as cur:
        for row in cur:
            # If 'And' or 'Or' is in the AreaName
            if lowercase[0] in row[0] or lowercase[1] in row[0] or lowercase[2] in row[0]:
                area_name = row[0].split()   # Split the area name by spaces
                new_area_name = [x.lower() if x in lowercase else x for x in area_name]  # Create new name changing to lowercase if in lowercase list
                final_name = ' '.join(new_area_name)   # Join corrected strings back together
                row[0] = final_name
                cur.updateRow(row)
    print('Corrected AreaName capitalization')


#### OWNER NAME ####
### Function to update FeeOwner and FeeOwnType ###
# Manual fee owners are skipped - these could be checked manually if desired
# This function updates owner name/type only if there is an adequate match AND the matching polygon
# has a fee owner that is not 'Unknown'. It is useful to check the source data to see if there
# are other values that should be included in this check (e.g., the check for TNC also include
# 'Unspecified' and 'UNK').
# It is useful to run consolidate_unk_names() first to try to consolidate any unknown owner names
# into streamlined values, especially if running this code to update only unknown owner names.
# Arguments:
#  - state (text): two-letter state abbreviation in capital letters (CT, MA, RI, VT, NH, ME)
#  - state_fc (text): name of the preprocessed, singlepart state data source
#                     it should be in same GDB as NEPOS so should only need name, not full path
#  - match_table (pandas df): dataframe resulting from reading in match table CSV with pd.read_csv()
#  - unknown_only (boolean): should owner name be updated only for rows where FeeOwner is unknown or empty?
#  - new_data_only (boolean): should owner name be updated only for new rows?
#                             new rows are identified by lack of FinalID
#  - take_only_known (boolean): should we only update NEPOS if the source has a known value (not unknown)?
#  - local_fc (text): RI only - name of preprocessed, singlepart RI Local data source
def update_owner_name_type(state, state_fc, match_table, unknown_only=True, new_data_only=True, take_only_known=True, local_fc=None):
    # Subset source datasets by state and (optionally) fee owner values
    # Good to check source data for any other variations of 'unknown' that should be added to SQL where take_only_known == True
    if take_only_known == False:
        src_query = "State = '" + state + "'"
    elif take_only_known == True:
        src_query = "State = '" + state + "' AND FeeOwner <> 'Unknown' AND FeeOwner <> 'UNK' AND FeeOwner <> 'Unspecified' AND FeeOwner <> 'N/A' AND FeeOwner <> ''"
    
    # Create dictionary of {UID: FeeOwner} pairs
    tnc_owner_names = {key: value for (key, value) in arcpy.da.SearchCursor(tnc, ['UID', 'FeeOwner'], where_clause=src_query)}
    tnc_owner_types = {key: value for (key, value) in arcpy.da.SearchCursor(tnc, ['UID', 'FeeOwnType'], where_clause=src_query)}
    padus_owner_names = {key: value for (key, value) in arcpy.da.SearchCursor(padus, ['UID', 'FeeOwner'], where_clause=src_query)}
    padus_owner_types = {key: value for (key, value) in arcpy.da.SearchCursor(padus, ['UID', 'FeeOwnType'], where_clause=src_query)}
    nced_owner_names = {key: value for (key, value) in arcpy.da.SearchCursor(nced, ['UID', 'FeeOwner'], where_clause=src_query)}
    nced_owner_types = {key: value for (key, value) in arcpy.da.SearchCursor(nced, ['UID', 'FeeOwnType'], where_clause=src_query)}
    state_owner_names = {key: value for (key, value) in arcpy.da.SearchCursor(state_fc, ['UID', 'FeeOwner'], where_clause=src_query)}
    state_owner_types = {key: value for (key, value) in arcpy.da.SearchCursor(state_fc, ['UID', 'FeeOwnType'], where_clause=src_query)}
    if local_fc is not None:     # For RI Local dataset only
        local_owner_names = {key: value for (key, value) in arcpy.da.SearchCursor(local_fc, ['UID', 'FeeOwner'], where_clause=src_query)}
        local_owner_types = {key: value for (key, value) in arcpy.da.SearchCursor(local_fc, ['UID', 'FeeOwnType'], where_clause=src_query)}
        local_items = get_local_info()
        local_id_col = local_items[0]
        local_code_col = local_items[1]
        local_pct_overlap_col = local_items[2]
        local_src = local_items[3]
    
    # Get the column names for the state match table and the state source name using get_state_info()
    # This is a list - need to separate each item in the list 
    state_items = get_state_info(state)
    state_id_col = state_items[0]
    state_code_col = state_items[1]
    state_pct_overlap_col = state_items[2]
    state_src = state_items[3]
    
    # Subset NEPOS rows for UpdateCursor
    if unknown_only == True and new_data_only == True:
        query = "State = '" + state + "' AND (FeeOwner IS NULL or FeeOwner = 'Unknown' OR FeeOwner = '' OR FeeOwner = ' ') AND FinalID IS NULL"
    elif unknown_only == True and new_data_only == False:
        query = "State = '" + state + "' AND (FeeOwner IS NULL or FeeOwner = 'Unknown' OR FeeOwner = '' OR FeeOwner = ' ')"
    elif unknown_only == False and new_data_only == True:
        query = "State = '" + state + "' AND FinalID IS NULL"
    elif unknown_only == False and new_data_only == False:
        query = "State = '" + state + "'"

    c = 0
    fields = ['FinalID2', 'FeeOwner', 'Source_FeeOwner', 'Source_FeeOwner_FeatID', 'FeeOwnType']
    with arcpy.da.UpdateCursor(pos, fields, query) as cur:
        for row in cur:
            try:
                # Skip row if it has a manual or Sewall fee owner name
                if 'Harvard Forest' in row[2] or 'Sewall' in row[2]:
                    continue

                # Retrieve the FeeOwner and FeeOwnType attributes for the matching polygon from each source
                # based on FinalID2
                # For RI only - local match codes
                if local_fc is not None:
                    local_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [local_id_col, local_code_col, local_pct_overlap_col]].drop_duplicates()
                    local_matched_src_id = local_match_ss.iloc[0, 0]
                    local_match_code = local_match_ss.iloc[0, 1]
                    local_pct_overlap = local_match_ss.iloc[0, 2]
                    local_orig_id = get_src_orig_id(state, local_matched_src_id)
                    try:
                        local_owner_name = get_source_attribute(local_owner_names, local_orig_id)
                        local_owner_type = get_source_attribute(local_owner_types, local_orig_id)
                    except Exception:
                        print(f'Setting match code to -1 for {state} Local feature {local_orig_id}')
                        local_match_code = -1
                # State match codes
                state_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [state_id_col, state_code_col, state_pct_overlap_col]].drop_duplicates()
                state_matched_src_id = state_match_ss.iloc[0, 0]
                state_match_code = state_match_ss.iloc[0, 1]
                state_pct_overlap = state_match_ss.iloc[0, 2]
                state_orig_id = get_src_orig_id(state, state_matched_src_id)
                try:
                    state_owner_name = get_source_attribute(state_owner_names, state_orig_id)
                    state_owner_type = get_source_attribute(state_owner_types, state_orig_id)
                except Exception:
                    print(f'Setting match code to -1 for {state} feature {state_orig_id}')
                    state_match_code = -1     # Setting this to -1 ensures the source won't be used for this row
                # TNC match codes
                tnc_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["tnc_id", "tnc_match_code", "tnc_pct_overlap"]].drop_duplicates()
                tnc_matched_src_id = tnc_match_ss.iloc[0, 0]
                tnc_match_code = tnc_match_ss.iloc[0, 1]
                tnc_pct_overlap = tnc_match_ss.iloc[0, 2]
                tnc_orig_id = get_src_orig_id('TNC', tnc_matched_src_id)
                try:
                    tnc_owner_name = get_source_attribute(tnc_owner_names, tnc_orig_id)
                    tnc_owner_type = get_source_attribute(tnc_owner_types, tnc_orig_id)
                except Exception:
                    print(f'Setting match code to -1 for TNC feature {tnc_orig_id}')
                    tnc_match_code = -1
                # NCED match codes
                nced_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["nced_id", "nced_match_code", "nced_pct_overlap"]].drop_duplicates()
                nced_matched_src_id = nced_match_ss.iloc[0, 0]
                nced_match_code = nced_match_ss.iloc[0, 1]
                nced_pct_overlap = nced_match_ss.iloc[0, 2]
                nced_orig_id = get_src_orig_id('NCED', nced_matched_src_id)
                try:
                    nced_owner_name = get_source_attribute(nced_owner_names, nced_orig_id)
                    nced_owner_type = get_source_attribute(nced_owner_types, nced_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to NCED feature {nced_orig_id}')
                    nced_match_code = -1
                # PADUS match codes
                padus_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["padus_id", "padus_match_code", "padus_pct_overlap"]].drop_duplicates()
                padus_matched_src_id = padus_match_ss.iloc[0, 0]
                padus_match_code = padus_match_ss.iloc[0, 1]
                padus_pct_overlap = padus_match_ss.iloc[0, 2]
                padus_orig_id = get_src_orig_id('padus', padus_matched_src_id)
                try:
                    padus_owner_name = get_source_attribute(padus_owner_names, padus_orig_id)
                    padus_owner_type = get_source_attribute(padus_owner_types, padus_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to PADUS feature {padus_orig_id}')
                    padus_match_code = -1
                
                # RI Local we want to use first
                if local_fc is not None:
                    if (min_match_code <= local_match_code <= max_match_code or (local_match_code == 10 and local_pct_overlap >= min_pct_overlap)):
                        row[1] = local_owner_name
                        row[2] = local_src
                        row[3] = local_orig_id
                        row[4] = local_owner_type
                        c = c + 1
                        cur.updateRow(row)   # Update row
                        continue             # Continue to next row so conditionals below are not executed
                if (min_match_code <= state_match_code <= max_match_code or (state_match_code == 10 and state_pct_overlap >= min_pct_overlap)):
                    row[1] = state_owner_name
                    row[2] = state_src
                    row[3] = state_orig_id
                    row[4] = state_owner_type
                    c = c + 1
                elif (min_match_code <= tnc_match_code <= max_match_code or (tnc_match_code == 10 and tnc_pct_overlap >= min_pct_overlap)):
                    row[1] = tnc_owner_name
                    row[2] = tnc_src
                    row[3] = tnc_orig_id
                    row[4] = tnc_owner_type
                    c = c + 1
                elif (min_match_code <= nced_match_code <= max_match_code or (nced_match_code == 10 and nced_pct_overlap >= min_pct_overlap)):
                    row[1] = nced_owner_name
                    row[2] = nced_src
                    row[3] = nced_orig_id
                    row[4] = nced_owner_type
                    c = c + 1
                elif (min_match_code <= padus_match_code <= max_match_code or (padus_match_code == 10 and padus_pct_overlap >= min_pct_overlap)):
                    row[1] = padus_owner_name
                    row[2] = padus_src
                    row[3] = padus_orig_id
                    row[4] = padus_owner_type
                    c = c + 1
                cur.updateRow(row)
            except Exception:
                print(traceback.format_exc())
                continue
    print(f'Updated FeeOwner for {c} rows!')


#### FUNCTIONS APPLICABLE TO BOTH OWNER AND INT HOLDER NAMES ####
### Functions to standardize spelling of different owner/interest holder names ###
# There may be names that are spelled differently within source dataset for the same entity.
# Ideally, these would be handled in preprocessing but currently that's not part of the workflow.
# The scripts below can be used to streamline names so there is 1 spelling for a given organization.
# Scripts are separated by type (e.g. local, state) and there are conditionals within each function to
# organize names for each state (since sometimes town/cities share names across states).
# Arguments:
#  - state (text): two-letter state abbreviation in capital letters (CT, MA, RI, VT, NH, ME)
#  - field (text): FeeOwner, IntHolder1, or IntHolder2
#  - new_data_only (boolean): should names be corrected only for new data?
#                             new rows are identified based on lack of FinalID
def correct_LOC_names(state, field, new_data_only = True):
    # Set query
    if new_data_only == False:
        query = "State = '" + state + "'"
    elif new_data_only == True:
        query = "State = '" + state + "' AND FinalID IS NULL"

    with arcpy.da.UpdateCursor(pos, field, query) as cur:
        for row in cur:
            # If field is an IntHolder field and is empty, go to the next row
            if (field == 'IntHolder1' or field == 'IntHolder2') and row[0] is None:
                continue
            if state == 'CT':
                if row[0] == 'Amston Lake Hebron Tax District and Amston Lake L*':
                    row[0] = 'Amston Lake Hebron Tax District & Amston Lake Lebanon Tax District'
                elif row[0] == 'ASHFORD, TOWN OF':
                    row[0] = 'Town of Ashford'
                elif row[0] == 'Avon town':
                    row[0] = 'Town of Avon'
                elif row[0] == 'Berlin town':
                    row[0] = 'Town of Berlin'
                elif row[0] == 'Town of Bethel, CT':
                    row[0] = 'Town of Bethel'
                elif row[0] == 'Bethlehem Town of':
                    row[0] = 'Town of Bethlehem'
                elif row[0] == 'Bridgwater Town of':
                    row[0] = 'Town of Bridgewater'
                elif row[0] == 'Burlington Town of':
                    row[0] = 'Town of Burlington'
                elif row[0] == 'CANTERBURY, TOWN OF':
                    row[0] = 'Town of Canterbury'
                elif row[0] == 'City of New Haven East Rock Pa' or row[0] == 'City of New Haven Park' or row[0] == 'City of New Havenn':
                    row[0] = 'City of New Haven'
                elif row[0] == 'CITY OF WATERBURY':
                    row[0] = 'City of Waterbury'
                elif row[0] == 'Cornwall Town of' or row[0] == 'Cornwall Tonw of':
                    row[0] = 'Town of Cornwall'
                elif row[0] == 'COVENTRY, TOWN OF' or row[0] == 'TOWN OF COVENTRY':
                    row[0] = 'Town of Coventry'
                elif row[0] == 'CROMWELL TOWN OF':
                    row[0] = 'Town of Cromwell'
                elif row[0] == 'Darien town of':
                    row[0] = 'Town of Darien'
                elif row[0] == 'Derby city of':
                    row[0] = 'City of Derby'
                elif row[0] == 'East Lyme, CT' or row[0] == 'EAST LYME, TOWN OF':
                    row[0] = 'Town of East Lyme'
                elif row[0] == 'EASTFORD, TOWN OF':
                    row[0] = 'Town of Eastford'
                elif row[0] == 'Farmington town':
                    row[0] = 'Town of Farmington'
                elif row[0] == 'FRANKLIN, TOWN OF':
                    row[0] = 'Town of Franklin'
                elif row[0] == 'GLASTONBURY TOWN OF':
                    row[0] = 'Town of Glastonbury'
                elif row[0] == 'Goshen Town of' or row[0] == 'Goshen, CT':
                    row[0] = 'Town of Goshen'
                elif row[0] == 'Granby town':
                    row[0] = 'Town of Granby'
                elif row[0] == 'GROTON, CITY OF':
                    row[0] = 'City of Groton'
                elif row[0] == 'GROTON, TOWN OF':  # Note that there is both a City and Town of Groton (city is dependent of town)
                    row[0] = 'Town of Groton'
                elif row[0] == 'Guilford, CT':
                    row[0] = 'Town of Guilford'
                elif row[0] == 'HAMPTON, TOWN OF':
                    row[0] = 'Town of Hampton'
                elif row[0] == 'Harwinton Town of':
                    row[0] = 'Town of Harwinton'
                elif row[0] == 'Kent Town of':
                    row[0] = 'Town of Kent'
                elif row[0] == 'KILLINGLY, TOWN OF':
                    row[0] = 'Town of Killingly'
                elif row[0] == 'LEBANON, TOWN OF':
                    row[0] = 'Town of Lebanon'
                elif row[0] == 'LEDYARD, TOWN OF':
                    row[0] = 'Town of Ledyard'
                elif row[0] == 'Litchfield Town of':
                    row[0] = 'Town of Litchfield'
                elif row[0] == 'LYME, TOWN OF' or row[0] == 'TOWN OF LYME':
                    row[0] = 'Town of Lyme'
                elif row[0] == 'Manchester town':
                    row[0] = 'Town of Manchester'
                elif row[0] == 'Mansfield town' or row[0] == 'MANSFIELD, TOWN OF' or row[0] == 'TOWN OF MANSFIELD':
                    row[0] = 'Town of Mansfield'
                elif row[0] == 'Marlborough town':
                    row[0] = 'Town of Marlborough'
                elif row[0] == 'MONTVILLE, TOWN OF':
                    row[0] = 'Town of Montville'
                elif row[0] == 'Morris Town of':
                    row[0] = 'Town of Morris'
                elif row[0] == 'CITY' or row[0] == 'MUNICIPAL' or row[0] == 'Municipality' or row[0] == 'Town':
                    row[0] = 'Municipal'
                elif row[0] == 'New Hartford Town of' or row[0] == 'New Hartford, CT':
                    row[0] = 'Town of New Hartford'
                elif row[0] == 'New Hartford Village':   # Village of NH is distinct from Town
                    row[0] = 'Village of New Hartford'
                elif row[0] == 'NEW LONDON, CITY OF':
                    row[0] = 'City of New London'
                elif row[0] == 'New Miford Town of' or row[0] == 'New Milford Town of':
                    row[0] = 'Town of New Milford'
                elif row[0] == 'Norfolk Town of':
                    row[0] = 'Town of Norfolk'
                elif row[0] == 'North Canaan Town of':
                    row[0] = 'Town of North Canaan'
                elif row[0] == 'NORWALK CITY':
                    row[0] = 'City of Norwalk'
                # The city and town of Norwich were consolidated into one municipality (a city) in 1952
                # so we are just going to call everything City of Norwich
                elif (row[0] == 'NORWICH, CITY OF' or row[0] == 'NORWICH, TOWN OF' or row[0] == 'Town of Norwich' or 
                row[0] == 'NORWICH, TOWN OF AND CITY OF'):
                    row[0] = 'City of Norwich'
                elif row[0] == 'NORWICH, CITY OF - BOARD OF WATER COMMISSIONERS':
                    row[0] = 'City of Norwich Board of Water Commissioners'
                elif row[0] == 'PLAINFIELD, TOWN OF':
                    row[0] = 'Town of Plainfield'
                elif row[0] == 'Plymouth Town of':
                    row[0] = 'Town of Plymouth'
                elif row[0] == 'POMFRET, TOWN OF' or row[0] == 'TOWN OF POMFRET':
                    row[0] = 'Town of Pomfret'
                elif row[0] == 'PRESTON, TOWN OF':
                    row[0] = 'Town of Preston'
                elif row[0] == 'Putnam town of' or row[0] == 'PUTNAM, TOWN OF':
                    row[0] = 'Town of Putnam'
                elif row[0] == 'Rocky Hill town':
                    row[0] = 'Town of Rocky Hill'
                elif row[0] == 'ROUTE 11 GREENWAY AUTHORITY':
                    row[0] = row[0].title()
                elif row[0] == 'Roxbury Town of':
                    row[0] = 'Town of Roxbury'
                elif row[0] == 'SALEM, TOWN OF':
                    row[0] = 'Town of Salem'
                elif row[0] == 'Salisbury' or row[0] == 'Salisbury, CT':
                    row[0] = 'Town of Salisbury'
                elif row[0] == 'Shelton City of' or row[0] == 'Town of Shelton':
                    row[0] = 'City of Shelton'
                elif row[0] == 'Simsbury town':
                    row[0] = 'Town of Simsbury'
                elif row[0] == 'Southeast Water Authority':
                    row[0] = 'Southeastern Connecticut Water Authority'
                elif row[0] == 'SOUTH WINDHAM FIRE DEPARTMENT, INC.':
                    row[0] = 'South Windham Fire Department'
                elif row[0] == 'South Windsor town':
                    row[0] = 'Town of South Windsor'
                elif row[0] == 'SPRAGUE, TOWN OF':
                    row[0] = 'Town of Sprague'
                elif row[0] == 'Stamford City of' or row[0] == 'Town of Stamford' or row[0] == 'City of Stamford Park':
                    row[0] = 'City of Stamford'
                elif row[0] == 'STONINGTON, BOROUGH OF':
                    row[0] = 'Borough of Stonington'
                elif row[0] == 'STONINGTON, TOWN OF' or row[0] == 'TOWN OF STONINGTON':
                    row[0] = 'Town of Stonington'
                elif row[0] == 'Stratford Town of' or row[0] == 'Town of Stratford/boothe Memorial Park':
                    row[0] = 'Town of Stratford'
                elif row[0] == 'Thomaston Town of':
                    row[0] = 'Town of Thomaston'
                elif row[0] == 'THOMPSON, TOWN OF' or row[0] == 'TOWN OF THOMPSON':
                    row[0] = 'Town of Thompson'
                elif row[0] == 'Tolland Conservation Commission, CT':
                    row[0] = 'Town of Tolland Conservation Commission'
                elif row[0] == 'Town of Bridgeport':
                    row[0] = 'City of Bridgeport'
                elif row[0] == 'Town of Danbury':
                    row[0] = 'City of Danbury'
                elif row[0] == 'Town of Derby':
                    row[0] = 'City of Derby'
                elif row[0] == 'Town of Greenwich Bruce Meseum' or row[0] == 'Town of Greenwich C/O Finance Dept':
                    row[0] = 'Town of Greenwich'
                elif row[0] == 'Town of Hartford':
                    row[0] = 'City of Hartford'
                elif row[0] == 'Town of Meriden':
                    row[0] = 'City of Meriden'
                elif row[0] == 'Town of Middletown':
                    row[0] = 'City of Middletown'
                elif row[0] == 'Town of Mystic':
                    row[0] = 'Village of Mystic'
                elif row[0] == 'Town of New Britain':
                    row[0] = 'City of New Britain'
                elif row[0] == 'Town of Old Lym':
                    row[0] = 'Town of Old Lyme'
                elif row[0] == 'Town of Torrington' or row[0] == 'Torrington, CT':
                    row[0] = 'City of Torrington'
                elif row[0] == 'Town of Town of Old Saybrook':
                    row[0] = 'Town of Old Saybrook'
                elif row[0] == 'Town of Waterbury':
                    row[0] = 'City of Waterbury'
                elif row[0] == 'City of Wethersfield':
                    row[0] = 'Town of Wethersfield'
                elif row[0] == 'Unknown Conservation Commission, CT':
                    row[0] = 'Unknown Conservation Commission'
                elif row[0] == 'UNKL':
                    row[0] = 'Unknown Local Government'
                elif row[0] == 'Vernon town':
                    row[0] = 'Town of Vernon'
                elif row[0] == 'VOLUNTOWN, TOWN OF':
                    row[0] = 'Town of Voluntown'
                elif row[0] == 'Warren Town of':
                    row[0] = 'Town of Warren'
                elif row[0] == 'Washington Town of':
                    row[0] = 'Town of Washington'
                elif row[0] == 'WATERFORD, TOWN OF':
                    row[0] = 'Town of Waterford'
                elif row[0] == 'West Hartford, CT':
                    row[0] = 'Town of West Hartford'
                elif row[0] == 'WILLINGTON, TOWN OF':
                    row[0] = 'Town of Willington'
                elif row[0] == 'Winchester Town of':
                    row[0] = 'Town of Winchester'
                elif row[0] == 'WINDHAM, TOWN OF':
                    row[0] = 'Town of Windham'
                elif row[0] == 'Woodbury Town of' or row[0] == 'Woodbury, CT':
                    row[0] = 'Town of Woodbury'
                elif row[0] == 'Windham County Soil and Water Conservation Distri*':
                    row[0] = 'Windham County Soil and Water Conservation District'
            elif state == 'MA':
                if row[0] == 'AMHERST PELHAM REGIONAL SCHOOL DISTRICT':
                    row[0] = 'Amherst Pelham Regional School District'
                elif row[0] == 'BLACKSTONE VALLEY REGIONAL VOCATIONAL TECHNICAL HIGH SCH':
                    row[0] = 'Blackstone Valley Regional Vocational Technical High School'
                elif row[0] == 'BLUE HILLS REGIONAL VOCATIONAL TECHNICAL HIGH SCHOOL':
                    row[0] = 'Blue Hills Regional Vocational Technical High School'
                elif row[0] == 'BRISTOL COUNTY':
                    row[0] = 'Bristol County'
                elif row[0] == 'Byfield Water Department' or row[0] == 'BYFIELD WATER DISTRICT':
                    row[0] = 'Byfield Water District'
                elif row[0] == 'CAPE COD REGIONAL TECHNICAL HIGH SCHOOL':
                    row[0] = 'Cape Cod Regional Technical High School'
                elif row[0] == 'City Of Northampton':
                    row[0] = 'City of Northampton'
                elif row[0] == 'Dedham-Westwood Water Supply District':
                    row[0] = 'Dedham-Westwood Water District'
                elif row[0] == 'DUKES COUNTY':
                    row[0] = 'Dukes County'
                elif row[0] == 'FRANKLIN COUNTY TECHNICAL SCHOOL':
                    row[0] = 'Franklin County Technical School'
                elif row[0] == 'GILL MONTAGUE SCHOOL':
                    row[0] = 'Gill Montague School'
                elif row[0] == 'GREATER LOWELL REGIONAL VOCATIONAL TECHNICAL HIGH SCHOOL':
                    row[0] = 'Greater Lowell Regional Vocational Technical High School'
                elif row[0] == 'HOLYOKE HOUSING AUTHORITY':
                    row[0] = 'City of Holyoke Housing Authority'
                elif row[0] == 'NASHOBA VALLEY TECHNICAL HIGH SCHOOL':
                    row[0] = 'Nashoba Valley Technical High School'
                elif row[0] == 'NIPMUC REGINAL HIGH SCHOOL':
                    row[0] = 'Nipmuc Regional High School'
                elif row[0] == 'NORFOLK COUNTY':
                    row[0] = 'Norfolk County'
                elif row[0] == 'NORTHEAST METROPOLITAN REGIONAL VOCATIONAL HIGH SCHOOL':
                    row[0] = 'Northeast Metropolitan Regional Vocational High School'
                elif row[0] == 'Norther Middlesex Regional School District':
                    row[0] = 'Northern Middlesex Regional School District'
                elif row[0] == 'SILVER LAKE REGIONAL SCHOOL DISTRICT':
                    row[0] = 'Silver Lake Regional School District'
                elif row[0] == 'SOUTH MIDDLESEX REG VOC TECH SCHOOL':
                    row[0] = 'South Middlesex Regional Vocational Technical School'
                # Two entries that are the same but order is reversed
                # Consolidating to one format
                elif row[0] == "Town of Rockland & Town of Abington Water Department":
                    row[0] = "Town of Abington Water Department & Town of Rockland"
                elif row[0] == 'Town of FALL RIVER':
                    row[0] = 'Town of Fall River'
                elif row[0] == 'TOWN OF MILLIS':
                    row[0] = 'Town of Millis'
                elif row[0] == 'Town Of Norton':
                    row[0] = 'Town of Norton'
                elif row[0] == 'TOWN OF NORWELL':
                    row[0] = 'Town of Norwell'
                elif row[0] == 'TRITON REGIONAL SCHOOL DISTRICT':
                    row[0] = 'Triton Regional School District'
                elif row[0] == 'WHITMAN HANSON REGIONAL HIGH SCHOOL':
                    row[0] = 'Whitman Hanson Regional High School'
            elif state == 'ME':
                if row[0] == 'CIty of Portland':
                    row[0] = 'City of Portland'
                elif row[0] == 'Deer Isle':
                    row[0] = 'Town of Deer Isle'
            elif state == 'NH':
                if row[0] == 'EXETER TOWN OF':
                    row[0] = 'Town of Exeter'
            elif state == 'RI':
                if row[0] == 'Glocester Conservation Commission':
                    row[0] = 'Town of Glocester Conservation Commission'
                elif row[0] == 'Woonsocket Conservation Commission':
                    row[0] = 'City of Woonsocket Conservation Commission'
                elif row[0] == 'Hopkinton Land Trust':
                    row[0] = 'Town of Hopkinton Land Trust'
            elif state == 'VT':
                if row[0] == 'Barre City':
                    row[0] = 'City of Barre'
                elif row[0] == 'Barre Town':
                    row[0] = 'Town of Barre'
                elif row[0] == 'Burlington Department of Parks & Rec':
                    row[0] = 'City of Burlington Department of Parks & Rec'
            cur.updateRow(row)
    print(f'Updated LOC names in {field}')

def correct_private_names(state, field, new_data_only = True):
    # Set query
    if new_data_only == False:
        query = "State = '" + state + "'"
    elif new_data_only == True:
        query = "State = '" + state + "' AND FinalID IS NULL"
    
    with arcpy.da.UpdateCursor(pos, field, query)  as cur:
        for row in cur:
            # If the field is an IntHolder field and is empty, go to the next row
            if (field == 'IntHolder1' or field == 'IntHolder2') and row[0] is None:
                continue
            if row[0] == 'AGODASH ACHIM':
                row[0] = 'Agodash Achim'
            elif row[0] == 'ALFORD BROOK CLUB':
                row[0] = 'Alford Brook Club'
            elif row[0] == 'ALICE RICH NORTHROP MEMORIAL INC':
                row[0] = 'Alice Rich Northrop Memorial Inc'
            elif row[0] == 'AMERICAN CHESTNUT NOMINEE TRUST':
                row[0] = 'American Chestnut Nominee Trust'
            elif row[0] == 'AMHERST COLLEGE':
                row[0] = 'Amherst College'
            elif row[0] == 'ANNAWON COUNCIL INC BSA':
                row[0] = 'Boy Scouts of America-Annawon Council'
            elif row[0] == 'ANNISQUAM ASSOCIATION':
                row[0] = 'Annisquam Association'
            elif row[0] == 'Applachain Mountain Club':
                row[0] = 'Appalachian Mountain Club'
            elif row[0] == 'Applachain Mountain Club & Forest Society of Maine':
                row[0] = 'Appalachian Mountain Club & Forest Society of Maine'
            elif row[0] == 'Aquidneck Land Trust':
                row[0] = 'Aquidneck Island Land Trust'
            elif row[0] == 'Ashfield Burial Ground Associtation':
                row[0] = 'Ashfield Burial Ground Association'
            elif row[0] == 'Aspetuck Land Trust, Inc.':
                row[0] = 'Aspetuck Land Trust'
            elif (row[0] == 'Aquraion Water Co' or row[0] == 'Aquarion Water Company of Connecticut, CT' or row[0] == 'Aquarian Water Company'
                  or row[0] == 'Quarion Water Company of Connecticut'):
                row[0] = 'Aquarion Water Company'
            elif row[0] == 'AVALONIA LAND CONSERVANCY' or row[0] == 'Avalonia Land Conservancy, Inc.':
                row[0] = 'Avalonia Land Conservancy'
            elif row[0] == 'Barnhamsted Land Trust':
                row[0] = 'Barkhamsted Land Trust'
            elif row[0] == 'Barrington Land Conservancy Trust':
                row[0] = 'Barrington Land Conservation Trust'
            elif row[0] == 'Bethleham Land Trust':
                row[0] = 'Bethlehem Land Trust'
            elif row[0] == 'BIG POND PRESERVATION ASSOCIATION, INC.':
                row[0] = row[0].title()
            elif row[0] == 'Blackston Valley Boys and Girls Club Inc.':
                row[0] = 'Blackstone Valley Boys and Girls Club'
            elif row[0] == 'Cheshire Land Trust Inc':
                row[0] = 'Cheshire Land Trust, Inc.'
            elif row[0] == "CHOCALOG REALTY TR" or row[0] == "CHOCALOG REALTY TRUST":
                row[0] = "Chocalog Realty Trust"
            elif row[0] == 'Coastal Mountain Land Trust':
                row[0] = 'Coastal Mountains Land Trust'
            elif row[0] == 'Coastal River Conservation Trust':
                row[0] = 'Coastal Rivers Conservation Trust'
            elif row[0] == 'COLCHESTER LAND TRUST':
                row[0] = 'Colchester Land Trust'
            elif row[0] == 'Colebrook Land Conservancy, Inc.':
                row[0] = 'Colebrook Land Conservancy'
            elif (row[0] == 'Audubon Society of CT' or row[0] == 'CONNECTICUT AUDUBON SOCIETY' or row[0] == 'National Audubon Society of Connecticut'
                  or row[0] == 'CT AUDUBON SOCIETY'):
                row[0] = 'Connecticut Audubon Society'
            elif row[0] == 'CONNECTICUT COLLEGE':
                row[0] = 'Connecticut College'
            elif row[0] == 'CONNECTICUT FARMLAND TRUST':
                row[0] = 'Connecticut Farmland Trust'
            elif (row[0] == 'CONNECTICUT FOREST & PARKS ASSOCIATION, INC.' or row[0] == 'CONNECTICUT FOREST & PARK ASSOCIATION'
                  or row[0] == 'CONNECTICUT FOREST & PARK ASSOCIATION, INC.'):
                row[0] = 'Connecticut Forest and Parks Association'
            elif row[0] == 'CT River Watershed Council' or row[0] == 'Connecticut River Watershed Council':   # According to Guidestar these are the same
                row[0] = 'Connecticut River Conservancy'   # It looks like CRWC has rebranded as CRC
            elif row[0] == 'DENISON PEQUOTSEPOS NATURE CENTER INC' or row[0] == 'DENISON PEQUOTSEPOS NATURE CENTER':
                row[0] = 'Denison Pequotsepos Nature Center Inc'
            elif row[0] == 'Dudley Conservation Land Trust ':
                row[0] = 'Dudley Conservation Land Trust'
            elif row[0] == 'Duxbury Rural and Historical Society Inc':
                row[0] = 'Duxbury Rural and Historical Society'
            elif row[0] == 'EAST LYME LAND TRUST':
                row[0] = 'East Lyme Land Trust'
            elif (row[0] == 'EASTERN CT FOREST LANDOWNERS ASSOC./WOLF DEN LAND TRUST' or 
            row[0] == 'Eastern CT Forest Landowners Assoc./Wolf Den Land*' or row[0] == 'ECFLA - WOLF DEN'):
                row[0] = 'Eastern CT Forest Landowners Association / Wolf Den Land Trust'
            elif row[0] == 'Farm Bureau Agricultural Preservation Corp':
                row[0] == 'Farm Bureau Agricultural Preservation Corporation'
            elif row[0] == 'Flanders Nature Center' or row[0] == 'Flanders Nature Center and Land Trust':
                row[0] = 'Flanders Nature Center and Land Trust, Inc.'
            elif row[0] == 'Forest Society Of Maine':
                row[0] = 'Forest Society of Maine'
            elif row[0] == 'Frenchman Bay Conservacy':
                row[0] = 'Frenchman Bay Conservancy'
            elif row[0] == 'FRIENDS OF THE SHETUCKET RIVER VALLEY':
                row[0] = 'Friends of the Shetucket River Valley'
            elif row[0] == 'GREENWICH LAND TRUST':
                row[0] = 'Greenwich Land Trust'
            elif row[0] == 'GROTON OPEN SPACE ASSOCIATION':
                row[0] = 'Groton Open Space Association'
            elif row[0] == 'Guilford Land Consrvation Trust':
                row[0] = 'Guilford Land Conservation Trust'
            elif row[0] == 'Harpswell Heritage Trust':
                row[0] = 'Harpswell Heritage Land Trust'
            elif row[0] == 'High StreeT Cemetery Association':
                row[0] = 'High Street Cemetery Association'
            elif row[0] == 'The Hopkinton Land Trust':
                row[0] = 'Hopkinton Land Trust'
            elif row[0] == 'Housatonic Valley Association, Inc.':
                row[0] = 'Housatonic Valley Association'
            elif row[0] == 'HULL FOREST PRODUCTS':
                row[0] = 'Hull Forest Products'
            elif row[0] == 'HULL FORESTLANDS LP':
                row[0] = 'Hull Forestlands LP'
            elif row[0] == 'John Dorr Nature Laboratory of the Horace Mann Sc*':
                row[0] = 'John Dorr Nature Laboratory of the Horace Mann School'
            elif row[0] == "JOSHUA'S TRUST":
                row[0] = "Joshua's Tract Conservation & Historic Trust, Inc."
            elif row[0] == 'Kent Land Trust, Inc.':
                row[0] = 'Kent Land Trust'
            elif row[0] == 'LYME LAND CONSERVATION TRUST':
                row[0] = 'Lyme Land Conservation Trust'
            elif row[0] == 'Madison Land Coservation Trust':
                row[0] = 'Madison Land Conservation Trust'
            elif row[0] == 'Maxwell Conservation Land Trust':
                row[0] = 'Maxwell Conservation Trust'
            elif row[0] == 'Meadow CIty Conservation Coalition':
                row[0] = 'Meadow City Conservation Coalition'
            elif row[0] == 'Middlebury Land Trust  Inc.' or row[0] == 'Middlebury Land Trust Inc':
                row[0] = 'Middlebury Land Trust'
            elif row[0] == 'Monadnock Conservancy, The':
                row[0] = 'The Monadnock Conservancy'
            elif row[0] == 'The Narrow River Land Trust':
                row[0] = 'Narrow River Land Trust'
            elif (row[0] == 'NEW ENGLAND FORESTRY FOUNDATION' or row[0] == 'New England Forestry Foundation ' or 
                  row[0] == 'New England Forestry Foundation, Inc.'):
                row[0] = 'New England Forestry Foundation'
            elif row[0] == 'New Marlborough Land Preservation Trust':
                row[0] = 'New Marlborough Land Trust'
            elif row[0] == 'NEW ROXBURY LAND TRUST':
                row[0] = 'New Roxbury Land Trust'
            elif row[0] == 'NGO':
                row[0] = 'Unknown Non-governmental Organization'
            elif row[0] == 'Norcross' or row[0] == 'NORCROSS WILDLIFE FOUNDATION' or row[0] == 'NORCROSS WILDLIFE SANCTUARY':
                row[0] = 'Norcross Wildlife Foundation'
            elif row[0] == 'NORTHERN CT LAND TRUST':
                row[0] = 'Northern Connecticut Land Trust Inc'
            elif row[0] == 'Northwest Connecticut land Conservancy':
                row[0] = 'Northwest Connecticut Land Conservancy'
            elif row[0] == 'OLD LYME CONSERVATION TRUST':
                row[0] = 'Old Lyme Conservation Trust'
            elif row[0] == 'Pratt Nature Center  In' or row[0] == 'Pratt Nature Center  Inc' or row[0] == 'Pratt Nature Center  Inc.':
                row[0] = 'Pratt Nature Center'
            elif row[0] == 'Presumpscot Regional Land Trust (Formerly Gorham-Sebago Lake Regional Land Trust)':
                row[0] = 'Presumpscot Regional Land Trust (formerly Gorham-Sebago Lake Regional Land Trust)'
            elif row[0] == 'PVT' or row[0] == 'Private' or row[0] == 'PRIVATE' or row[0] == 'Private Landowner':
                row[0] = 'Private Land Owner'
            elif row[0] == 'The Prudence Conservancy':
                row[0] = 'Prudence Conservancy'
            elif row[0] == 'Reading Council For Girls Inc':
                row[0] = 'Reading Council for Girls Inc'
            elif row[0] == 'Roxbury Land Trust':
                row[0] = 'Roxbury Land Trust, Inc.'
            elif row[0] == 'SALEM LAND TRUST':
                row[0] = 'Salem Land Trust'
            elif row[0] == 'Salisbury Association':
                row[0] = 'Salisbury Association Land Trust'
            elif row[0] == 'Scarborough Land Conservation Trust':
                row[0] = 'Scarborough Land Trust'
            elif row[0] == 'The Scituate Land Trust':
                row[0] = 'Scituate Land Trust'
            elif row[0] == 'Society for the Protection of New Hampshire Forests':
                row[0] = 'Society for the Protection of NH Forests'
            elif row[0] == 'The South Kingstown Land Trust':
                row[0] = 'South Kingstown Land Trust'
            elif row[0] == 'Southbury Land Trust' or row[0] == 'Southnbury Last Trust':
                row[0] = 'Southbury Land Trust, Inc.'
            elif row[0] == 'Stamford Museum & Ntr Ctr Inc' or row[0] == 'Stmfd Museum & Nature Center':
                row[0] = 'Stamford Museum and Nature Center'
            elif row[0] == 'STONINGTON LAND TURST' or row[0] == 'STONINGTON LAND TRUST':
                row[0] = 'Stonington Land Trust'
            elif row[0] == 'SUFFIELD LAND CONSERVANCY':
                row[0] = 'Suffield Land Conservancy'
            elif row[0] == 'The Tiverton Land Trust':
                row[0] = 'Tiverton Land Trust'
            elif row[0] == 'THAMESVILLE RECREATION ASSOCIATION, INC.':
                row[0] = row[0].title()
            elif row[0] == 'The Chewonki Foundation':
                row[0] = 'Chewonki Foundation'
            elif row[0] == 'THE NATURE CONSERVANCY':
                row[0] = 'The Nature Conservancy'
            elif row[0] == 'The Opacum Land Trust':
                row[0] = 'Opacum Land Trust'
            elif row[0] == 'The Trustees Of Reservations':
                row[0] = 'The Trustees of Reservations'
            elif row[0] == 'Trusteees of Clark University':
                row[0] = 'Clark University Trustees'
            elif row[0] == 'Wallingford Land Trust':
                row[0] = 'Wallingford Land Trust, Inc.'
            elif row[0] == 'WAREHAM LAND TRUST':
                row[0] = 'Wareham Land Trust'
            elif row[0] == 'Warren Land Trust':
                row[0] = 'Warren Land Conservation Trust'
            elif row[0] == 'The Watch Hill Conservancy':
                row[0] = 'Watch Hill Conservancy'
            elif row[0] == 'WATERFORD LAND TRUST':
                row[0] = 'Waterford Land Trust'
            elif row[0] == 'The Westerly Land Trust':
                row[0] = 'Westerly Land Trust'
            elif row[0] == 'Whetstone Wood Trust':
                row[0] = 'Whetstone Wood Trust Fund'
            elif row[0] == 'WILBRAHAM CONSERVATION TRUST':
                row[0] = 'Wilbraham Conservation Trust'
            elif row[0] == 'Winchester Land trust':
                row[0] = 'Winchester Land Trust'
            elif row[0] == 'Woods Hole Marine Biology Laboratory':
                row[0] = 'Woods Hole Marine Biological Laboratory'
            elif row[0] == 'Worcester Natural Historical Society':
                row[0] = 'Worcester Natural History Society'
            elif row[0] == 'WYNDHAM LAND TRUST':
                row[0] = 'Wyndham Land Trust Inc.'
            elif row[0] == 'YALE UNIVERSITY':
                row[0] = 'Yale University'
            elif row[0] == 'YANTIC FIRE COMPANY A/K/A YANTIC FIRE ENGINE CO.':
                row[0] = 'Yantic Fire Engine Company'
            elif row[0] == 'YMCA OF MYSTIC':
                row[0] = 'YMCA of Mystic'
            elif (row[0] == "Young Men's Christian Association of Metropolitan*" or 
            row[0] == "Young Men's Christian Association Of Metropolitan*"):
                row[0] = "YMCA of Metropolitan Hartford, Inc."
            elif row[0] == "Young Mens Christian Association":
                row[0] = "Young Men's Christian Association, Inc."
            elif row[0] == "Young Men's Christian Association of Southern Con*":
                row[0] = "Young Men's Christian Association of Southern Connecticut"
            elif row[0] == 'YWCA':
                row[0] = "Young Women's Christian Association"
            cur.updateRow(row)
    print(f'Updated PNP names in {field}')

def correct_state_names(state, field, new_data_only = True):
    # Set query
    if new_data_only == False:
        query = "State = '" + state + "'"
    elif new_data_only == True:
        query = "State = '" + state + "' AND FinalID IS NULL"
    
    with arcpy.da.UpdateCursor(pos, field, query) as cur:
        for row in cur:
            # If field is an IntHolder field and is empty, go to the next row
            if (field == 'IntHolder1' or field == 'IntHolder2') and row[0] is None:
                continue
            if state == 'CT':
                # Some departments listed don't exist according to CT gov website of agencies and offices
                # In these cases we just give them the more general state name
                if (row[0] == 'CONNECTICUT STATE OF' or row[0] == 'STATE OF CONNECTICUT' or row[0] == 
                'State of Connencticut' or row[0] == 'State of Connecticut- Boat Launch Area' or 
                row[0] == 'State of Connecticut - Public Works' or row[0] == 'OTHS' or row[0] == 
                'State of Connecticut Dept Land Acq & Mgt' or row[0] == 'State of CT' or 
                row[0] == 'Connecticut State Government' or row[0] == 'Connecticut State of' or
                row[0] == 'STATE OF CT' or row[0] == 'STATE OF CT - OSWA'):
                    row[0] = 'State of Connecticut'
                elif row[0] == 'UCONN':
                    row[0] = 'University of Connecticut'
                # The DEP was consolidated w/ energy dept many years ago and is now CT DEEP
                elif (row[0] == 'CT Department of Environmental Protection' or row[0] == 
                'State of Connecticut - DEEP' or row[0] == 'Connecticut Department of Energy and Environmental Protection'
                or row[0] == 'CT DEEP' or row[0] == 'CT DEEP - OSWA GRANT'):
                    row[0] = 'CT Department of Energy and Environmental Protection'
                elif row[0] == 'State of Connecticut Board of Fishery & Game':
                    row[0] = 'CT Board of Fisheries and Game'
                elif row[0] == 'State of Connecticut Department of Transportation' or row[0] == 'CT DOT':
                    row[0] = 'CT Department of Transportation'
                elif (row[0] == 'CT State Parks' or row[0] == 'State Park Commission' or row[0] == 
                'State Park Commision'):
                    row[0] = 'CT DEEP - Bureau of Outdoor Recreation, State Parks & Public Outreach Division'
                elif row[0] == 'Connecticut Department of Agriculture' or row[0] == 'CT DEPT OF AG' or row[0] == 'CT DOAG?':
                    row[0] = 'CT Department of Agriculture'
                elif row[0] == 'Connecticut State of  DPS':
                    row[0] = 'CT Department of Public Services'
            elif state == 'MA':
                if row[0] == 'Commonwealth of Massachusetts Department of Development Services':
                    row[0] = 'MA Department of Development Services'
                elif (row[0] == 'Department of Fish and Game & MA DCR - Division of State Parks and Recreation' or 
                      row[0] == 'MA DCR - Division of State Parks and Recreation / MA Department of Fish and Game'):
                    row[0] = 'MA Department of Fish and Game & MA DCR - Division of State Parks and Recreation'
                elif (row[0] == 'Department of Fish and Game & MA DCR - Division of Water Supply Protection' or 
                      row[0] == 'MA DCR - Division of Water Supply Protection & MA Department of Fish and Game'):
                    row[0] = 'MA Department of Fish and Game & MA DCR - Division of Water Supply Protection'
                elif row[0] == 'Massachusetts Department of Transportation':
                    row[0] = 'MA Department of Transportation'
            elif state == 'ME':
                if (row[0].lower() == 'maine department of inland fisheries and wildlife' or row[0] == 'MDIFW'):
                    row[0] = 'ME Department of Inland Fisheries and Wildlife'
                elif (row[0].lower() == 'maine bureau of parks and lands' or 
                      row[0] == 'Maine Bureau Of Parks And Lands & Maine Bureau of Parks and Lands'):
                    row[0] = 'ME DACF - Bureau of Parks and Lands'
                elif row[0] == 'Maine Department of Transportation':
                    row[0] = 'ME Department of Transportation'
                elif row[0] == 'State of Maine - Dept. of Marine Resources - Bureau of Sea Run Fisheries and Habitat':
                    row[0] = 'ME DMR - Bureau of Sea Run Fisheries and Habitat'
                elif row[0] == 'Maine Department of Agriculture':
                    row[0] = 'ME Department of Agriculture, Conservation, and Forestry'
                elif row[0] == 'University of Maine System':
                    row[0] = 'The University of Maine'
                elif row[0].lower() == 'maine department of marine resources':
                    row[0] = 'ME Department of Marine Resources'
                elif row[0] == 'Maine Department of Environmental Protection':
                    row[0] = 'ME Department of Environmental Protection'
            elif state == 'NH':
                if row[0] == 'NH DES, Water Resources Division' or row[0] == 'NH DES, WRC':
                    row[0] = 'NH DES - Water Resources Division'
                elif row[0] == 'NH University of New Hampshire (Durham)':
                    row[0] = 'University of New Hampshire (Durham)'
                elif row[0] == 'State of New Hampshire - DNCR/DFL + DPR':
                    row[0] = 'NH Department of Natural & Cultural Resources'
                elif row[0] == 'New Hampshire Department of Environmental Services':
                    row[0] = 'NH Department of Environmental Services'
                elif row[0] == 'New Hampshire Department of Resources & Economic Development' or row[0] == 'NH Department of Resources & Economic Development':
                    row[0] = 'NH Department of Resources and Economic Development'
                elif row[0] == 'New Hampshire Fish & Game':
                    row[0] = 'NH Fish and Game Department'
            elif state == 'RI':
                if row[0] == 'URI Board of Governors':
                    row[0] = 'University of Rhode Island'
                elif row[0] == 'DEM':
                    row[0] = 'RI Department of Environmental Management'
            elif state == 'VT':
                if row[0] == 'VT Agency of Natural Resources (ANR)':
                    row[0] = 'VT Agency of Natural Resources'
                elif row[0] == 'VT ANR - Dept. of Environmental Conservation (DEC)':
                    row[0] = 'VT ANR - Department of Environmental Conservation'
                elif (row[0] == 'VT ANR - Dept. of Fish and Wildlife (DFW)' or row[0] == 'VT Department of Fish and Wildlife' 
                      or row[0] == 'VT Fish and Wildlife Department'):
                    row[0] = 'VT ANR - Department of Fish and Wildlife'
                elif row[0] == 'VT ANR - Dept. of Forest Parks and Recreation (FPR)' or row[0] == 'VT Department of Forests, Parks and Recreation':
                    row[0] = 'VT ANR - Department of Forests, Parks and Recreation'
                elif row[0] == 'VT ANR - Dept. of Forest Parks and Recreation (FPR) & VT AOA - Dept. of Buildings and General Services':
                    row[0] = 'VT ANR - Department of Forest Parks and Recreation & VT AOA - Department of Buildings and General Services'
                elif row[0] == 'VT Division for Historic Preservation':
                    row[0] = 'VT ACCD - Division for Historic Preservation'
                elif row[0] == 'VT Housing and Conservation Board (VHCB)' or row[0] == 'Vermont Housing and Conservation Board':
                    row[0] = 'VT Housing and Conservation Board'
            cur.updateRow(row)
    print(f'Updated STP names in {field}')

def correct_fed_names(state, field, new_data_only = True):
    # Set query
    if new_data_only == False:
        query = "State = '" + state + "'"
    elif new_data_only == True:
        query = "State = '" + state + "' AND FinalID IS NULL"

    with arcpy.da.UpdateCursor(pos, field, query) as cur:
        for row in cur:
            # If field is an IntHolder field and is empty, go to the next row
            if (field == 'IntHolder1' or field == 'IntHolder2') and row[0] is None:
                continue
            if (row[0] == 'FWS' or row[0] == 'U.S. Fish and Wildlife Service' or 
                row[0].lower() == 'us fish and wildlife service' or row[0] == 
                'US Fish & Wildlife Service, Moosehorn National Wildlife Refuge'):
                row[0] = 'US DOI - Fish and Wildlife Service'
            elif (row[0] == 'NPS' or row[0] == 'U.S. National Park Service' or 
                  row[0] == 'US National Park Service'):
                row[0] = 'US DOI - National Park Service'
            elif (row[0] == 'NRCS - Admin State CT' or row[0] == 'U.S. Natural Resources Conservation Service'
                  or row[0] == 'NRCS - Admin State MA' or row[0] == 'NRCS - Admin State ME' or 
                  row[0] == 'NRCS - Admin State NH' or row[0] == 'NRCS - Admin State RI' or row[0] == 'NRCS - Admin State VT'):
                row[0] = 'USDA - Natural Resources Conservation Service'
            elif row[0] == 'U.S. Forest Service' or row[0] == 'USFS' or row[0] == 'USDA FOREST SERVICE':
                row[0] = 'USDA - Forest Service'
            elif (row[0] == 'USA' or row[0] == 'UNITED STATES OF AMERICA' or row[0] == "U.S. Federal Government"):
                row[0] = 'United States of America'
            elif row[0] == 'United States Department of Agriculture':
                row[0] = 'US Department of Agriculture'
            elif row[0] == 'US Department of Interior':
                row[0] = 'US Department of the Interior'
            cur.updateRow(row)
    print('Updated FED names in {}'.format(field))

### Function to consolidate unknown names ###
# It is useful to summarize the owner or interest holder name field in NEPOS and look
# for any rows that mean 'unknown' that are not captured in the code below so those cam be added.
# This functions uses 'Unknown' as the standard unknown owner/holder name.
# Arguments:
#  - state (text): two-letter state abbreviation in capital letters (CT, MA, RI, VT, NH, ME)
#  - field (text): FeeOwner, IntHolder1, or IntHolder2
#  - new_data_only (boolean): should names be corrected only for new data?
#                             new rows are identified based on lack of FinalID
def consolidate_unk_names(state, field, new_data_only = True):
    # Set query
    if new_data_only == False:
        query = "State = '" + state + "'"
    elif new_data_only == True:
        query = "State = '" + state + "' AND FinalID IS NULL"
    
    with arcpy.da.UpdateCursor(pos, field, query) as cur:
        for row in cur:
            if (row[0] == 'Owner Unknown' or row[0] == 'UNK' or row[0] == 'UNKNOWN'
                or row[0] == 'UNKNOWN OR TOWN' or row[0] == '' or row[0] == ' '):
                row[0] = 'Unknown'
                cur.updateRow(row)

### Function to correct owner/holder types for specific names ###
# Within the function, there are multiple conditionals grouped by owner/holder type
# (for example, all names that should be coded as PNP are in on conditional).
# This function sets PFP owners based on the presence of keywords where possible
# and also be specific names.
# The conditionals can be updated to add any misclassified owners so that in the future
# they get automatically corrected with this function.
# Arguments:
#  - state (text): two-letter state abbreviation in capital letters (CT, MA, RI, VT, NH, ME)
#  - fields (list of 2 strings): a list of the name field and corresponding type field
#                                for example ['FeeOwner', 'FeeOwnType'] or ['IntHolder1', 'IntHolder1Type']
#  - new_data_only (boolean): should type be corrected only for new data?
#                             new rows are identified based on lack of FinalID
def correct_name_type(state, fields, new_data_only = True):
    # Set data query
    if new_data_only == False:
        query = "State = '" + state + "'"
    elif new_data_only == True:
        query = "State = '" + state + "' AND FinalID IS NULL"

    # List of PFP keywords - all lowercase to avoid capitalization issues
    # NOTE: ' inc' used to be in this list but I took it out...
    pfp_keywords = [' llc', ' corp', 'company', 'business', 'realty', 'ltd',
                    'limited partners', ' lp', 'railroad' ' power', ' farm',
                    'developers', 'construction', ' llp']
    
    with arcpy.da.UpdateCursor(pos, fields, query) as cur:
        for row in cur:
            # Set empty FeeOwner rows to Unknown
            if fields[0] == 'FeeOwner' and row[0] is None:
                row[0] = 'Unknown'
                row[1] = 'UNK'
                cur.updateRow(row)
            
            # If its an IntHolder field and its empty, go to next row
            if (fields[0] == 'IntHolder1' or fields[0] == 'IntHolder2') and row[0] is None:
                continue

            # Set some PFP rows to PVT if they do not have a PFP keyword in owner name
            # This is mainly an issue in MassGIS which has a binary private for/non profit for private owners
            # We have three private categories, for-profit, non-profit, and general private 
            if row[1] == 'PFP' and any(word in row[0].lower() for word in pfp_keywords) == False:
                row[1] = 'PVT'
            # Set some PVT rows to PFP based on the same keywords
            elif row[1] == 'PVT' and any(word in row[0].lower() for word in pfp_keywords) == True:
                row[1] = 'PFP'
            # Set PFP by specific names
            elif (row[0] == 'Aquarion Water Company' or row[0] == 'BHC Company' or 
            row[0] == 'Connecticut-American Water Co' or row[0] == 'Connecticut Water and Gas Company' 
            or row[0] == 'Connecticut Water Company' or row[0] == 'Groton Dev Associates' or 
            row[0] == 'Norwalk Power LLC' or row[0] == 'Preston Plains Water Co' or row[0] == 
            'Miami Beach Water Co' or row[0] == 'Wildwood Water Co Inc' or row[0] == 'Champion International Corporation'
            or row[0] == 'Hull Forest Products' or row[0] == 'Hull Forestlands LP' or row[0] == 'Northern Utilities Co' 
            or row[0] == 'Seven Islands Land Co' or row[0] == 'Tall Timber Trust' or row[0] == 'Weyerhaeuser' or 
            row[0] == 'Brookfield Power' or 'eversource' in row[0].lower() or row[0] == 'FirstLight Power' or 
            'general electric' in row[0].lower() or row[0] == 'Great River Hydro' or ' Cowls ' in row[0] or 
            row[0] == 'Lyme Timber' or row[0] == 'Aurora Sustainable Lands' or row[0] == 'Timbervest' or
            row[0] == 'E.J. Carrier, Inc.'):
                row[1] = 'PFP'
            # Set PNP
            elif (row[0] == 'Canton Land Conservation Trust' or row[0] == 'Deep River Historical Society' 
            or row[0] == 'East Haddam Land Trust' or row[0] == 'Litchfield Land Trust, Inc.' or row[0] == 
            'Manchester Land Conservation Trust, Inc.' or row[0] == 'Middlesex Land Trust, Inc.' or 
            row[0] == 'Northern Connecticut Land Trust Inc' or row[0] == 'Norwalk Land Conservation Trust Inc' or 
            row[0] == 'Stamford Museum and Nature Center' or row[0] == 'Maccurdy Salisbury Ed Fund' or 
            row[0] == 'Heritage Land Preservation Trust' or row[0] == 'Connecticut College' or row[0] == 
            'Connecticut River Conservancy' or row[0] == 'Greenwich Land Trust' or row[0] == 'Steep Rock Association' or 
            row[0] == 'Branford Land Trust' or row[0] == 'Denison Pequotsepos Nature Center Inc' or row[0] == 
            'Big Pond Preservation Association, Inc.' or row[0] == 'Pratt Nature Center' or row[0] == 'YMCA of Mystic' or 
            row[0] == 'Connecticut Farmland Trust' or row[0] == 'Connecticut Trust for Historic Preservation' or 
            row[0] == 'The Denison Society, Inc.' or row[0] == 'Roxbury Cemetery Association' or row[0] == 
            'Pond Mountain Trust, Inc.' or row[0] == 'Warren Land Trust' or row[0] == 'Winchester Land Trust' or 
            row[0] == 'Lake Maspenock Preservation Association' or row[0] == 'Downeast Salmon Federation' or 
            row[0] == 'Downeast Coastal Conservancy' or row[0] == 'Kittery Land Trust' or 
            row[0] == 'Maine Coast Heritage Trust' or row[0] == 'Francis Small Heritage Trust, Inc.' or 
            row[0] == 'Island Heritage Trust' or row[0] == 'Coastal Rivers Conservation Trust' or 
            row[0] == 'Falmouth Land Trust' or row[0] == 'Forest Society of Maine' or row[0] == 'Great Works Regional Land Trust' or
            row[0] == 'Maine Farmland Trust' or row[0] == 'Orono Land Trust' or row[0].lower() == 'curry college' or 
            row[0].lower() == 'endicott college' or row[0].lower() == 'green mountain club' or row[0] == 'Massachusetts Institute of Technology' or 
            'mount holyoke college' in row[0].lower() or 'smith college' in row[0].lower() or 'boston university' in row[0].lower() or 
            'wellesley college' in row[0].lower() or 'williams college' in row[0].lower() or row[0] == 'Great Mountain Forest Corporation' or 
            row[0] == 'Dark Pond, Inc.' or row[0] == 'Bow Open Spaces, Inc.' or row[0] == 'Carl Siemon Family Charitable Trust' or 
            row[0] == 'Land Bank of Wolfboro-Tuftonboro'):
                row[1] = 'PNP'
            # Set PVT
            elif (row[0] == 'Fairfield County Fish and Game Protective Assoc.' or row[0] == 'Thamesville Recreation Association, Inc.' or
                  row[0] == 'Private Land Owner' or row[0] == 'Dahlke Daphne Harding' or row[0] == 'Jordan Laurel B' or 
                  row[0] == 'Lassen Estelle B Trustee' or row[0] == 'Regina Laudis Abbey' or 
                  row[0] == 'Emerson Gertrude Harding' or row[0] == 'Yantic Fire Engine Company' or 
                  row[0] == 'South Windham Fire Department' or row[0] == 'Squires Grove Association' or
                  row[0] == 'Nauset Rod and Gun Club Inc'):
                row[1] = 'PVT'
            # Set STP
            elif (row[0] == 'State of Connecticut' or row[0] == 'CT Department of Energy and Environmental Protection' or 
                  row[0] == 'University of Massachusetts' or row[0] == 'ME Department of Inland Fisheries and Wildlife' or 
                  row[0] == 'ME DACF - Bureau of Parks and Lands' or row[0] == 'RI Department of Environmental Management'):
                row[1] = 'STP'
            # Set OTH (joint ownership usually)
            elif (row[0] == 'Town of Berlin and Berlin Land Trust, Inc.' or row[0] == 
            'Town of Redding and The Nature Conservancy' or row[0] == 'Town of Somers and N. Connecticut Land Trust' 
            or row[0] == 'Town of Farmington and Shaw, Charles H.' or row[0] == 'Wyndham Land Trust (1/5th Interest) and Fitze, Pa*' 
            or row[0] == 'Salem Land Trust/David and Anne Bingham' or 
            row[0] == 'Laudholm Trust & Wells National Estuarine Research Reserve'):
                row[1] = 'OTH'
            # Set UNK
            elif row[0] == 'Open Space' or row[0] == 'Unknown' or row[0] == 'UNKNOWN':
                row[0] = 'Unknown'
                row[1] = 'UNK'
            # Set LOC -- not sure if HG&E should be QP?
            elif (row[0] == 'Route 11 Greenway Authority' or row[0] == 'Cape Code Regional Technical High School' or
                  row[0].lower() == 'holyoke gas and electric' or row[0] == 'Dedham-Westwood Water District' or
                  row[0] == 'Connecticut Metropolitan District Commission' or row[0] == 'Bangor Water District' or 
                  row[0] == 'Bath Water District' or row[0] == 'Maine Minor Civil Division' or row[0] == 'Portland Water District' or 
                  row[0] == 'Yarmouth Water District' or row[0] == 'York Water District' or row[0] == 'Kittery Water District' or 
                  row[0] == 'Hampton Water Works' or row[0] == 'Richmond Rural Preservation Land Trust' or row[0] == 'Smithfield Land Trust' or 
                  row[0] == 'Town of North Kingstown' or row[0] == 'Little Compton Ag. Conservancy Trust' or 
                  row[0] == 'Town of Hopkinton Land Trust' or row[0] == 'East Greenwich Land Trust' or row[0] == 'Town of West Greenwich'):
                row[1] = 'LOC'
            # Set QP
            elif (row[0] == 'Southeastern Connecticut Water Authority' or row[0] == 'South Central Connecticut Water Authority' or 
                  row[0] == 'Springfield Water and Sewer Commission' or row[0] == 'Weymouth-Braintree Recreation-Conservation District'
                  or row[0] == 'Weymouth-Braintree Regional Recreation Conservation District'):
                row[1] = 'QP'
            # Set TRB rows
            elif (row[0] == 'Herring Pond Wampanoag Tribe' or row[0] == 'Mashpee Tribal Council' or 
                  row[0] == 'Passamaquoddy Indian Tribe'):
                row[1] = 'TRB'
            cur.updateRow(row)
            

#### INTEREST HOLDER 1 ####
### Function to update IntHolder1 and IntHolder1Type ###
# In contrast to previous functions, this one can be subset by null only int holder (int holder is missing but
# perhaps should not be) instead of unknown as with fee owner/area name.
# There aren't many unknown interest holders and it's more likely that an interest holder is just missed in a
# source rather than being listed as "unknown". In the 2024 update, there were some unknown interest holder
# that were corrected manually but the bigger problem was a bunch of "interest holders" called "Private Land Owner"
# in TNC data. These issues are best corrected with manual QA/QC because its often not something that can be
# corrected by just using another source, but rather is a more complicated issue.
# There are not (m)any manual IntHolder1 rows in NEPOS, so any rows with that have 'Harvard Forest'
# in Source_IntHolder1 are skipped and can be checked manually to see if they should be updated.
# Arguments:
#  - state (text): two-letter state abbreviation in capital letters (CT, MA, RI, VT, NH, ME)
#  - state_fc (text): name of the preprocessed, singlepart state data source
#                     it should be in same GDB as NEPOS so should only need name, not full path
#  - match_table (pandas df): dataframe resulting from reading in match table CSV with pd.read_csv()
#  - null_only (boolean): should holder name be updated only for rows where IntHolder1 is empty?
#  - new_data_only (boolean): should holder name be updated only for new rows?
#                             new rows are identified by lack of FinalID
#  - take_only_known (boolean): should we only update NEPOS if the source has a known value (not unknown)?
#  - local_fc (text): RI only - name of preprocessed, singlepart RI Local data source
def update_int_holder1(state, state_fc, match_table, null_only=True, new_data_only=True, take_only_known=True, local_fc=None):
    # Subset source data by state and (optionally) value
    if take_only_known == False:
        src_query = "State = '" + state + "'"
    elif take_only_known == True:
        src_query = "State = '" + state + "' AND IntHolder1 IS NOT NULL AND IntHolder1 <> 'Private Land Owner'"
    
    # Create dictionary of {UID: IntHolder1} and {UID: IntHolder1Type} for each source
    tnc_int_holder_names = {key: value for (key, value) in arcpy.da.SearchCursor(tnc, ['UID', 'IntHolder1'], where_clause=src_query)}
    tnc_int_holder_types = {key: value for (key, value) in arcpy.da.SearchCursor(tnc, ['UID', 'IntHolder1Type'], where_clause=src_query)}
    nced_int_holder_names = {key: value for (key, value) in arcpy.da.SearchCursor(nced, ['UID', 'IntHolder1'], where_clause=src_query)}
    nced_int_holder_types = {key: value for (key, value) in arcpy.da.SearchCursor(nced, ['UID', 'IntHolder1Type'], where_clause=src_query)}
    state_int_holder_names = {key: value for (key, value) in arcpy.da.SearchCursor(state_fc, ['UID', 'IntHolder1'], where_clause=src_query)}
    state_int_holder_types = {key: value for (key, value) in arcpy.da.SearchCursor(state_fc, ['UID', 'IntHolder1Type'], where_clause=src_query)}
    padus_int_holder_names = {key: value for (key, value) in arcpy.da.SearchCursor(padus, ['UID', 'IntHolder1'], where_clause=src_query)}
    padus_int_holder_types = {key: value for (key, value) in arcpy.da.SearchCursor(padus, ['UID', 'IntHolder1Type'], where_clause=src_query)}
    if local_fc is not None:
        local_int_holder_names = {key: value for (key, value) in arcpy.da.SearchCursor(local_fc, ['UID', 'IntHolder1'], where_clause=src_query)}
        local_int_holder_types = {key: value for (key, value) in arcpy.da.SearchCursor(local_fc, ['UID', 'IntHolder1Type'], where_clause=src_query)}
        local_items = get_local_info()
        local_id_col = local_items[0]
        local_code_col = local_items[1]
        local_pct_overlap_col = local_items[2]
        local_src = local_items[3]
    
    # Get state match table columns
    state_items = get_state_info(state)
    state_id_col = state_items[0]
    state_code_col = state_items[1]
    state_pct_overlap_col = state_items[2]
    state_src = state_items[3]
    
    # Subset NEPOS rows and columns for UpdateCursor
    if null_only == True and new_data_only == True:
        query = "State = '" + state + "' AND IntHolder1 IS NULL AND FinalID IS NULL"
    elif null_only == True and new_data_only == False:
        query = "State = '" + state + "' AND IntHolder1 IS NULL"
    elif null_only == False and new_data_only == True:
        query = "State = '" + state + "' AND FinalID IS NULL"
    elif null_only == False and new_data_only == False:
        query = "State = '" + state + "'"

    fields = ['FinalID2', 'PolySource', 'PolySource_FeatID', 'IntHolder1', 
              'Source_IntHolder1', 'Source_IntHolder1_FeatID', 'IntHolder1Type']
    c = 0   # For counting updated rows
    with arcpy.da.UpdateCursor(pos, fields, query) as cur:
        for row in cur:
            try:
                if 'Harvard Forest' in row[4]:
                    continue

                if local_fc is not None:
                    local_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [local_id_col, local_code_col, local_pct_overlap_col]].drop_duplicates()
                    local_matched_src_id = local_match_ss.iloc[0, 0]
                    local_match_code = local_match_ss.iloc[0, 1]
                    local_pct_overlap = local_match_ss.iloc[0, 2]
                    local_orig_id = get_src_orig_id(state, local_matched_src_id)
                    try:
                        local_int_holder1_name = get_source_attribute(local_int_holder_names, local_orig_id)
                        local_int_holder1_type = get_source_attribute(local_int_holder_types, local_orig_id)
                    except Exception:
                        print(f'Setting match code to -1 for {state} Local feature {local_orig_id}')
                        local_match_code = -1

                state_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [state_id_col, state_code_col, state_pct_overlap_col]].drop_duplicates()
                state_matched_src_id = state_match_ss.iloc[0, 0]
                state_match_code = state_match_ss.iloc[0, 1]
                state_pct_overlap = state_match_ss.iloc[0, 2]
                state_orig_id = get_src_orig_id(state, state_matched_src_id)
                try:
                    state_int_holder1_name = get_source_attribute(state_int_holder_names, state_orig_id)
                    state_int_holder1_type = get_source_attribute(state_int_holder_types, state_orig_id)
                except Exception:
                    print(f'Setting match code to -1 for {state} feature {state_orig_id}')
                    state_match_code = -1     # Setting this to -1 ensures the source won't be used for this row
                
                # TNC match codes
                tnc_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["tnc_id", "tnc_match_code", "tnc_pct_overlap"]].drop_duplicates()
                tnc_matched_src_id = tnc_match_ss.iloc[0, 0]
                tnc_match_code = tnc_match_ss.iloc[0, 1]
                tnc_pct_overlap = tnc_match_ss.iloc[0, 2]
                tnc_orig_id = get_src_orig_id('TNC', tnc_matched_src_id)
                try:
                    tnc_int_holder1_name = get_source_attribute(tnc_int_holder_names, tnc_orig_id)
                    tnc_int_holder1_type = get_source_attribute(tnc_int_holder_types, tnc_orig_id)
                except Exception:
                    print(f'Setting match code to -1 for TNC feature {tnc_orig_id}')
                    tnc_match_code = -1
                
                # NCED match codes
                nced_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["nced_id", "nced_match_code", "nced_pct_overlap"]].drop_duplicates()
                nced_matched_src_id = nced_match_ss.iloc[0, 0]
                nced_match_code = nced_match_ss.iloc[0, 1]
                nced_pct_overlap = nced_match_ss.iloc[0, 2]
                nced_orig_id = get_src_orig_id('NCED', nced_matched_src_id)
                try:
                    nced_int_holder1_name = get_source_attribute(nced_int_holder_names, nced_orig_id)
                    nced_int_holder1_type = get_source_attribute(nced_int_holder_types, nced_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to NCED feature {nced_orig_id}')
                    nced_match_code = -1
                # PADUS match codes
                padus_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["padus_id", "padus_match_code", "padus_pct_overlap"]].drop_duplicates()
                padus_matched_src_id = padus_match_ss.iloc[0, 0]
                padus_match_code = padus_match_ss.iloc[0, 1]
                padus_pct_overlap = padus_match_ss.iloc[0, 2]
                padus_orig_id = get_src_orig_id('padus', padus_matched_src_id)
                try:
                    padus_int_holder1_name = get_source_attribute(padus_int_holder_names, padus_orig_id)
                    padus_int_holder1_type = get_source_attribute(padus_int_holder_types, padus_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to PADUS feature {padus_orig_id}')
                    padus_match_code = -1
                
                # If match code is between 1-8, we populate with the matched polygon attribute (any source)
                if local_fc is not None:
                    if (min_match_code <= local_match_code <= max_match_code or (local_match_code == 10 and local_pct_overlap >= min_pct_overlap)):
                        row[3] = local_int_holder1_name
                        row[6] = local_int_holder1_type
                        row[4] = local_src
                        row[5] = local_orig_id
                        c = c + 1
                        cur.updateRow(row)
                        continue
                if (min_match_code <= state_match_code <= max_match_code or (state_match_code == 10 and state_pct_overlap >= min_pct_overlap)):
                    row[3] = state_int_holder1_name
                    row[6] = state_int_holder1_type
                    row[4] = state_src
                    row[5] = state_orig_id
                    c = c + 1
                elif (min_match_code <= tnc_match_code <= max_match_code or (tnc_match_code == 10 and tnc_pct_overlap >= min_pct_overlap)):
                    row[3] = tnc_int_holder1_name
                    row[6] = tnc_int_holder1_type
                    row[4] = tnc_src
                    row[5] = tnc_orig_id
                    c = c + 1
                elif (min_match_code <= nced_match_code <= max_match_code or (nced_match_code == 10 and nced_pct_overlap >= min_pct_overlap)):
                    row[3] = nced_int_holder1_name
                    row[6] = nced_int_holder1_type
                    row[4] = nced_src
                    row[5] = nced_orig_id
                    c = c + 1
                elif (min_match_code <= padus_match_code <= max_match_code or (padus_match_code == 10 and padus_pct_overlap >= min_pct_overlap)):
                    row[3] = padus_int_holder1_name
                    row[6] = padus_int_holder1_type
                    row[4] = padus_src
                    row[5] = padus_orig_id
                    c = c + 1
                cur.updateRow(row)
            except Exception:
                print(traceback.format_exc())
                continue
    print(f'Updated IntHolder1 for {c} rows')


#### INTEREST HOLDER 2 ####
### Function to update IntHolder2 and IntHolder2Type ###
# The only multi-state dataset that has IntHolder2 is NCED, however it does not have IntHolder2Type.
# MA, ME, NH, and VT can have multiple interest holders in their state data.
# The 'state' argument must always be populated to inform NCED and NEPOS query
# but state_fc can be null (and is null by default).
# Arguments:
#  - state (text): two-letter state abbreviation in capital letters (CT, MA, RI, VT, NH, ME)
#  - match_table (pandas df): dataframe resulting from reading in match table CSV with pd.read_csv()
#  - state_fc (text): name of the preprocessed, singlepart state data source
#                     it should be in same GDB as NEPOS so should only need name, not full path
#                     can be left empty for states like CT and RI where this info is not avail in state layer
#  - null_only (boolean): should holder name be updated only for rows where IntHolder2 is empty?
#  - new_data_only (boolean): should holder name be updated only for new rows?
#                             new rows are identified by lack of FinalID
#  - take_only_known (boolean): should we only update NEPOS if the source has a known value (not unknown)?
#  - local_fc (text): RI only - name of preprocessed, singlepart RI Local data source
#
# Note: It looks like I commented out the NCED portions - presumably because I did not find the data
#  reliable. If that changes those lines can simply be uncommented. Similar to other fields so far,
# any rows with manual IntHolder2 (there are none as of 2025) are skipped and can be updated manually.
def update_int_holder2(state, match_table, state_fc=None, null_only=True, new_data_only=True, take_only_known=True, local_fc=None):
    # Subset source data by state and (optionally) attribute value
    if take_only_known == False:
        src_query = "State = '" + state + "'"
    elif take_only_known == True:
        src_query = "State = '" + state + "' AND IntHolder2 IS NOT NULL AND IntHolder2 <> 'Private Land Owner'"
    
    # Create {UID: IntHolder2} and {UID: IntHolder2Type} for each source
    nced_int_holder_names = {key: value for (key, value) in arcpy.da.SearchCursor(nced, ['UID', 'IntHolder2'], where_clause=src_query)}
    nced_int_holder_types = {key: value for (key, value) in arcpy.da.SearchCursor(nced, ['UID', 'IntHolder2Type'], where_clause=src_query)}
    if state_fc is not None:
        state_int_holder_names = {key: value for (key, value) in arcpy.da.SearchCursor(state_fc, ['UID', 'IntHolder2'], where_clause=src_query)}
        state_int_holder_types = {key: value for (key, value) in arcpy.da.SearchCursor(state_fc, ['UID', 'IntHolder2Type'], where_clause=src_query)}
        state_items = get_state_info(state)
        state_id_col = state_items[0]
        state_code_col = state_items[1]
        state_pct_overlap_col = state_items[2]
        state_src = state_items[3]
    if local_fc is not None:
        local_int_holder_names = {key: value for (key, value) in arcpy.da.SearchCursor(local_fc, ['UID', 'IntHolder2'], where_clause=src_query)}
        local_int_holder_types = {key: value for (key, value) in arcpy.da.SearchCursor(local_fc, ['UID', 'IntHolder2Type'], where_clause=src_query)}
        local_items = get_local_info()
        local_id_col = local_items[0]
        local_code_col = local_items[1]
        local_pct_overlap_col = local_items[2]
        local_src = local_items[3]
    
    # Subset NEPOS rows and columns for UpdateCursor
    if null_only == True and new_data_only == True:
        query = "State = '" + state + "' AND IntHolder2 IS NULL AND FinalID IS NULL"
    elif null_only == True and new_data_only == False:
        query = "State = '" + state + "' AND IntHolder2 IS NULL"
    elif null_only == False and new_data_only == True:
        query = "State = '" + state + "' AND FinalID IS NULL"
    elif null_only == False and new_data_only == False:
        query = "State = '" + state + "'"

    fields = ['FinalID2', 'PolySource', 'PolySource_FeatID', 'IntHolder2', 
              'Source_IntHolder2', 'Source_IntHolder2_FeatID', 'IntHolder2Type']
    c = 0   # For counting updated rows
    with arcpy.da.UpdateCursor(pos, fields, query) as cur:
        for row in cur:
            try:
                if 'Harvard Forest' in row[4]:
                    continue

                if local_fc is not None:
                    local_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [local_id_col, local_code_col, local_pct_overlap_col]].drop_duplicates()
                    local_matched_src_id = local_match_ss.iloc[0, 0]
                    local_match_code = local_match_ss.iloc[0, 1]
                    local_pct_overlap = local_match_ss.iloc[0, 2]
                    local_orig_id = get_src_orig_id(state, local_matched_src_id)
                    try:
                        local_int_holder2_name = get_source_attribute(local_int_holder_names, local_orig_id)
                        local_int_holder2_type = get_source_attribute(local_int_holder_types, local_orig_id)
                    except Exception:
                        print(f'Setting match code to -1 for {state} Local feature {local_orig_id}')
                        local_match_code = -1
                # If the state has multiple interest holders (structurally)
                # then we proceed with the normal extraction of state polygon match/attribute
                if state in ['MA', 'ME', 'NH', 'VT']:
                    # State match codes
                    state_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [state_id_col, state_code_col, state_pct_overlap_col]].drop_duplicates()
                    state_matched_src_id = state_match_ss.iloc[0, 0]
                    state_match_code = state_match_ss.iloc[0, 1]
                    state_pct_overlap = state_match_ss.iloc[0, 2]
                    state_orig_id = get_src_orig_id(state, state_matched_src_id)
                    try:
                        state_int_holder2_name = get_source_attribute(state_int_holder_names, state_orig_id)
                        state_int_holder2_type = get_source_attribute(state_int_holder_types, state_orig_id)
                    except Exception:
                        print(f'Setting match code to -1 for {state} feature {state_orig_id}')
                        state_match_code = -1     # Setting this to -1 ensures the source won't be used for this row
                # If CT or RI, we set the match code to -1 so they are never used in conditionals below
                else:
                    state_match_code = -1
                # NCED match codes
                # nced_match_ss = ct_match_table.loc[ct_match_table['FinalID2'] == row[0], ["nced_id", "nced_match_code", "nced_pct_overlap"]].drop_duplicates()
                # nced_matched_src_id = nced_match_ss.iloc[0, 0]
                # nced_match_code = nced_match_ss.iloc[0, 1]
                # nced_pct_overlap = nced_match_ss.iloc[0, 2]
                # nced_orig_id = get_src_orig_id('NCED', nced_matched_src_id)
                # try:
                #     nced_int_holder2_name = get_source_attribute(nced_int_holder_names, nced_orig_id)
                #     nced_int_holder2_type = get_source_attribute(nced_int_holder_types, nced_orig_id)
                # except Exception:
                #     print(f'Assigning match code -1 to NCED feature {nced_orig_id}')
                #     nced_match_code = -1
                # If match code is between 1-8, we populate with the matched polygon attribute (any source)
                if local_fc is not None:
                    if (min_match_code <= local_match_code <= max_match_code or (local_match_code == 10 and local_pct_overlap >= min_pct_overlap)):
                        row[3] = local_int_holder2_name
                        row[6] = local_int_holder2_type
                        row[4] = local_src
                        row[5] = local_orig_id
                        c = c + 1
                        cur.updateRow(row)
                        continue
                if (min_match_code <= state_match_code <= max_match_code or (state_match_code == 10 and state_pct_overlap >= min_pct_overlap)):
                    row[3] = state_int_holder2_name
                    row[6] = state_int_holder2_type
                    row[4] = state_src
                    row[5] = state_orig_id
                    c = c + 1
                    cur.updateRow(row)
                # elif (min_match_code <= nced_match_code <= max_match_code or (nced_match_code == 10 and nced_pct_overlap >= min_pct_overlap)):
                #     row[3] = nced_int_holder2_name
                #     row[6] = nced_int_holder2_type
                #     row[4] = nced_src
                #     row[5] = nced_orig_id
                #     c = c + 1
                #     cur.updateRow(row)
                else:
                    continue
            except Exception:
                print(traceback.format_exc())
                continue
    print(f'Updated IntHolder2 for {c} rows')


#### PROTECTION TYPE ####
### Function to update ProtType and ProtTypeComments ###
# This function contains a lot of similarities to previous functions but has some differences related
# to updating ProtTypeComments.
# The function is designed to update either ProtType only (comments_only=False - the default value)
# OR update only ProtTypeComments (comments_only=True). So for each state, you expect to run this
# function a second time if you want to update ProtTypeComments.
# By default, when updating ProtTypeComments, the existing ProtTypeComments will NOT be overwritten.
# Instead, text from the current source ProtTypeComments will be appended to the existing comment.
# There is some functionality to eliminate repeat comments, although it doesn't work perfectly and
# ProtTypeComments should be summarized as part of general QA/QC and cleaned manually as needed
# just like with all other fields.
# Setting overwrite_comments to True should be used with extreme discretion -- there are many rows
# where we have detailed ProtTypeComments resulting from research into specific areas that we don't want to lose.
# If you really need to overwrite comments because there's a lot that need to be updated, it is
# strongly recommended to make a copy of the function and in that copy you can edit the query used
# to subset NEPOS (or add extra conditions to the UpdateCursor) so you are only overwriting 
# ProtTypeComments for the rows that you are certain it would be beneficial to do so.
# One possible way to see how much comments have changed to is make a copy of NEPOS and run the function
# to update ProtTypeComments, then check how many rows have the text ' -- ' (or whatever string is
# used to separate old comment from new comment). You can also make a copy of this function
# and have it add a new field called ProtType2 and populate that field instead of ProtType in
# the UpdateCursor -- then you can compare the existing ProtType to ProtType2 and check for rows
# where these are not the same.
# Arguments:
#  - state (text): two-letter state abbreviation in capital letters (CT, MA, RI, VT, NH, ME)
#  - state_fc (text): name of the preprocessed, singlepart state data source
#                     it should be in same GDB as NEPOS so should only need name, not full path
#  - match_table (pandas df): dataframe resulting from reading in match table CSV with pd.read_csv()
#  - local_fc (text): RI only - name of preprocessed, singlepart RI Local data source
#  - comments_only (boolean): should the function only update ProtTypeComments and not also ProtType?
#  - overwrite_comments (boolean): should the function overwrite any existing ProtTypeComments?
#  - unknown_only (boolean): should protection type be updated only for rows where ProtType is Unknown?
#  - new_data_only (boolean): should protection type be updated only for new rows?
#                             new rows are identified by lack of FinalID
#  - take_only_known (boolean): should we only update NEPOS if the source has a known value (not unknown)?
def update_prot_type(state, state_fc, match_table, local_fc=None, comments_only=False, overwrite_comments=False, 
                     unknown_only=True, new_data_only=True, take_only_known=True):
    # Subset source data by state and (optionally) by attribute value
    if take_only_known == False:
        src_query = "State = '" + state + "'"
    elif take_only_known == True:
        src_query = "State = '" + state + "' AND LOWER(ProtType) NOT LIKE '%unknown%' AND ProtType <> '' AND ProtType <> ' ' AND ProtType IS NOT NULL"
    
    # Create dictionary of {UID: ProtType} pairs for each source
    tnc_prot_types = {key: value for (key, value) in arcpy.da.SearchCursor(tnc, ['UID', 'ProtType'], where_clause=src_query)}
    nced_prot_types = {key: value for (key, value) in arcpy.da.SearchCursor(nced, ['UID', 'ProtType'], where_clause=src_query)}
    padus_prot_types = {key: value for (key, value) in arcpy.da.SearchCursor(padus, ['UID', 'ProtType'], where_clause=src_query)}
    state_prot_types = {key: value for (key, value) in arcpy.da.SearchCursor(state_fc, ['UID', 'ProtType'], where_clause=src_query)}

    # Create dictionary of {UID: ProtTypeComments} pairs for state source if they have these
    states_with_prot_type_comments = ['MA', 'NH', 'VT', 'RI', 'ME']
    if state in states_with_prot_type_comments:
        state_prot_type_comments = {key: value for (key, value) in arcpy.da.SearchCursor(state_fc, ['UID', 'ProtTypeComments'], where_clause=src_query)}

    if local_fc is not None:
        local_prot_types = {key: value for (key, value) in arcpy.da.SearchCursor(local_fc, ['UID', 'ProtType'], where_clause=src_query)}
        local_prot_type_comments = {key: value for (key, value) in arcpy.da.SearchCursor(local_fc, ['UID', 'ProtTypeComments'], where_clause=src_query)}
        local_items = get_local_info()
        local_id_col = local_items[0]
        local_code_col = local_items[1]
        local_pct_overlap_col = local_items[2]
        local_src = local_items[3]
    
    # Check that comments_only arg is aligned with state arg
    if comments_only == True and state not in states_with_prot_type_comments:
        print(f'comments_only is True but {state} does not have ProtTypeComments populated')
        sys.exit()
    
    # Get match table column names for state and the the state source description
    state_items = get_state_info(state)
    state_id_col = state_items[0]
    state_code_col = state_items[1]
    state_pct_overlap_col = state_items[2]
    state_src = state_items[3]
    
    # Subset rows and columns for UpdateCursor
    if unknown_only == True and new_data_only == True:
        query = "State = '" + state + "' AND ProtType = 'Unknown' AND FinalID IS NULL"
    elif unknown_only == True and new_data_only == False:
        query = "State = '" + state + "' AND ProtType = 'Unknown'"
    elif unknown_only == False and new_data_only == True:
        query = "State = '" + state + "' AND FinalID IS NULL"
    elif unknown_only == False and new_data_only == False:
        query = "State = '" + state + "'"
    
    fields = ['FinalID2', 'PolySource', 'PolySource_FeatID', 
              'ProtType', 'Source_ProtType', 'Source_ProtType_FeatID', 'ProtTypeComments']
    c = 0
    with arcpy.da.UpdateCursor(pos, fields, query) as cur:
        for row in cur:
            # State match codes
            state_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [state_id_col, state_code_col, state_pct_overlap_col]].drop_duplicates()
            state_matched_src_id = state_match_ss.iloc[0, 0]
            state_match_code = state_match_ss.iloc[0, 1]
            state_pct_overlap = state_match_ss.iloc[0, 2]
            state_orig_id = get_src_orig_id(state, state_matched_src_id)
            try:
                state_prot_type = get_source_attribute(state_prot_types, state_orig_id)
                if state in states_with_prot_type_comments:
                    state_prot_type_comment = get_source_attribute(state_prot_type_comments, state_orig_id)
            except Exception:
                print(f'Assinging match code -1 to {state} feature {state_orig_id}')
                state_match_code = -1
            
            if local_fc is not None:
                local_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [local_id_col, local_code_col, local_pct_overlap_col]].drop_duplicates()
                local_matched_src_id = local_match_ss.iloc[0, 0]
                local_match_code = local_match_ss.iloc[0, 1]
                local_pct_overlap = local_match_ss.iloc[0, 2]
                local_orig_id = get_src_orig_id(state, local_matched_src_id)
                try:
                    local_prot_type = get_source_attribute(local_prot_types, local_orig_id)
                    local_prot_type_comment = get_source_attribute(local_prot_type_comments, local_orig_id)
                except Exception:
                    print(f'Assinging match code -1 to {state} Local feature {local_orig_id}')
                    local_match_code = -1
            
            # Since only state (and local for RI) sources have ProtTypeComments info, we can check for that here if
            # comments_only == True, to save time checking matches with other sources
            if comments_only == True:
                if ((min_match_code <= state_match_code <= max_match_code or (state_match_code == 10 and state_pct_overlap >= min_pct_overlap)) 
                and state_prot_type_comment is not None):
                    if row[6] is not None and overwrite_comments == False:
                        row[6] = row[6] + ' -- ' + state_prot_type_comment
                    else:
                        row[6] = state_prot_type_comment
                    cur.updateRow(row)
                if ((min_match_code <= local_match_code <= max_match_code or (local_match_code == 10 and local_pct_overlap >= min_pct_overlap)) 
                and local_prot_type_comment is not None):
                    if row[6] is not None and overwrite_comments == False:
                        row[6] = row[6] + ' -- ' + local_prot_type_comment
                    else:
                        row[6] = local_prot_type_comment
                    cur.updateRow(row)
                
                # Reduce redundancy in ProtTypeComments (not perfectly but will handle situations
                # where new comments are the same as old comments)
                # e.g., if old comment is Easement is CE and nothing has changed, the new comment
                # will be Easement is CE -- Easement is CE --> simplified to Easement is CE again
                # Separate comments from different updates by the string used to connect them above
                all_comments = ' -- '.split(row[6])
                unique_comments = list(set(all_comments))       # Get the unique items as a list
                final_comment = (' -- ').join(unique_comments)  # Recombine the unique items with same separator
                row[6] = final_comment                          # Update ProtTypeComments
                cur.updateRow(row)
                continue    # Push to next row so code below is not run
            
            # TNC match codes
            tnc_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["tnc_id", "tnc_match_code", "tnc_pct_overlap"]].drop_duplicates()
            tnc_matched_src_id = tnc_match_ss.iloc[0, 0]
            tnc_match_code = tnc_match_ss.iloc[0, 1]
            tnc_pct_overlap = tnc_match_ss.iloc[0, 2]
            tnc_orig_id = get_src_orig_id('tnc', tnc_matched_src_id)
            try:
                tnc_prot_type = get_source_attribute(tnc_prot_types, tnc_orig_id)
            except Exception:
                print(f'Assigning match code -1 to TNC feature {tnc_orig_id}')
                tnc_match_code = -1
            # NCED match codes
            nced_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["nced_id", "nced_match_code", "nced_pct_overlap"]].drop_duplicates()
            nced_matched_src_id = nced_match_ss.iloc[0, 0]
            nced_match_code = nced_match_ss.iloc[0, 1]
            nced_pct_overlap = nced_match_ss.iloc[0, 2]
            nced_orig_id = get_src_orig_id('nced', nced_matched_src_id)
            try:
                nced_prot_type = get_source_attribute(nced_prot_types, nced_orig_id)
            except Exception:
                print(f'Assigning match code -1 to NCED feature {nced_orig_id}')
                nced_match_code = -1
            # PADUS match codes
            padus_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["padus_id", "padus_match_code", "padus_pct_overlap"]].drop_duplicates()
            padus_matched_src_id = padus_match_ss.iloc[0, 0]
            padus_match_code = padus_match_ss.iloc[0, 1]
            padus_pct_overlap = padus_match_ss.iloc[0, 2]
            padus_orig_id = get_src_orig_id('padus', padus_matched_src_id)
            try:
                padus_prot_type = get_source_attribute(padus_prot_types, padus_orig_id)
            except Exception:
                print(f'Assigning match code -1 to PADUS feature {padus_orig_id}')
                padus_match_code = -1
            
            # If match code is between 1-6, we populate with the matched polygon attribute (any source)
            # RI local has the most info so for RI, we run that first
            if local_fc is not None:
                if (min_match_code <= local_match_code <= max_match_code or (local_match_code == 10 and local_pct_overlap >= min_pct_overlap)):
                    row[3] = local_prot_type
                    row[4] = local_src
                    row[5] = local_orig_id
                    c = c + 1
                    cur.updateRow(row)   # Update row
                    continue             # And continue to next so code below is not run
            if (min_match_code <= state_match_code <= max_match_code or (state_match_code == 10 and state_pct_overlap >= min_pct_overlap)):
                row[3] = state_prot_type
                row[4] = state_src
                row[5] = state_orig_id
                if state in states_with_prot_type_comments and state_prot_type_comment is not None:
                    if row[6] is not None:
                        row[6] = row[6] + '; ' + state_prot_type_comment
                    else:
                        row[6] = state_prot_type_comment
                c = c + 1
            if (min_match_code <= tnc_match_code <= max_match_code or (tnc_match_code == 10 and tnc_pct_overlap >= min_pct_overlap)):
                row[3] = tnc_prot_type
                row[4] = tnc_src
                row[5] = tnc_orig_id
                c = c + 1
            elif (min_match_code <= nced_match_code <= max_match_code or (nced_match_code == 10 and nced_pct_overlap >= min_pct_overlap)):
                row[3] = nced_prot_type
                row[4] = nced_src
                row[5] = nced_orig_id
                c = c + 1
            elif (min_match_code <= padus_match_code <= max_match_code or (padus_match_code == 10 and padus_pct_overlap >= min_pct_overlap)):
                row[3] = padus_prot_type
                row[4] = padus_src
                row[5] = padus_orig_id
                c = c + 1
            cur.updateRow(row)
    if comments_only == True:
        print(f'Updated ProtType ProtTypeComments for {c} rows...')
    else:
        print(f'Updated ProtType for {c} rows...')


##### GAP STATUS ######
### Function to update GapStatus ###
# As of 2025, there is no manually set GapStatus so those rows are skipped and checked manually
# if they arise in the future.
# NOTE: There are some rows in RI where GapStatus was changed from 2 to 3 based on personal
# communication with TNC staff - if you query State = 'RI' AND Comments LIKE '%TNC%' you will
# find them and can set these rows back to GapStatus of 3 when they are updated in the future.
# (I didn't mark these as manual because the information is still from TNC and not HF research
# or knowledge.)
# Arguments:
#  - state (text): two-letter state abbreviation in capital letters (CT, MA, RI, VT, NH, ME)
#  - state_fc (text): name of the preprocessed, singlepart state data source
#                     it should be in same GDB as NEPOS so should only need name, not full path
#  - match_table (pandas df): dataframe resulting from reading in match table CSV with pd.read_csv()
#  - unknown_only (boolean): should gap status be updated only for rows where GapStatus is unknown?
#  - new_data_only (boolean): should gap status be updated only for new rows?
#                             new rows are identified by lack of FinalID
#  - take_only_known (boolean): should we only update NEPOS if the source has a known value (not unknown)?
def update_gap_status(state, state_fc, match_table, local_fc=None, unknown_only=True, new_data_only=True, take_only_known=True):
    # Subset source data by state and (optionally) attribute value
    if take_only_known == False:
        src_query = "State = '" + state + "'"
    elif take_only_known == True:
        src_query = "State = '" + state + "' AND GapStatus <> 0"
    tnc_gap = {key: value for (key, value) in arcpy.da.SearchCursor(tnc, ['UID', 'GapStatus'], where_clause=src_query)}
    nced_gap = {key: value for (key, value) in arcpy.da.SearchCursor(nced, ['UID', 'GapStatus'], where_clause=src_query)}
    padus_gap = {key: value for (key, value) in arcpy.da.SearchCursor(padus, ['UID', 'GapStatus'], where_clause=src_query)}
    state_gap = {key: value for (key, value) in arcpy.da.SearchCursor(state_fc, ['UID', 'GapStatus'], where_clause=src_query)}

    if local_fc is not None:
        local_gap = {key: value for (key, value) in arcpy.da.SearchCursor(local_fc, ['UID', 'GapStatus'], where_clause=src_query)}
        local_items = get_local_info()
        local_id_col = local_items[0]
        local_code_col = local_items[1]
        local_pct_overlap_col = local_items[2]
        local_src = local_items[3]

    # Get state match table column
    state_items = get_state_info(state)
    state_id_col = state_items[0]
    state_code_col = state_items[1]
    state_pct_overlap_col = state_items[2]
    state_src = state_items[3]

    # Subset columns and rows for NEPOS UpdateCursor
    fields = ['FinalID2', 'PolySource', 'PolySource_FeatID', 
              'GapStatus', 'Source_GapStatus', 'Source_GapStatus_FeatID']
    if new_data_only == False and unknown_only == False:
        query = "State = '" + state + "'"
    elif new_data_only == True and unknown_only == False:
        query = "State = '" + state + "' AND FinalID IS NULL"
    elif new_data_only == True and unknown_only == True:
        query = "State = '" + state + "' AND FinalID IS NULL AND GapStatus = 0"
    elif new_data_only == False and unknown_only == True:
        query = "State = '" + state + "' AND GapStatus = 0"

    c = 0
    with arcpy.da.UpdateCursor(pos, fields, query) as cur:
        for row in cur:
            try:
                if 'Harvard Forest' in row[4]:
                    continue
                
                if local_fc is not None:
                    local_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [local_id_col, local_code_col, local_pct_overlap_col]].drop_duplicates()
                    local_matched_src_id = local_match_ss.iloc[0, 0]
                    local_match_code = local_match_ss.iloc[0, 1]
                    local_pct_overlap = local_match_ss.iloc[0, 2]
                    local_orig_id = get_src_orig_id(state, local_matched_src_id)
                    try:
                        local_gap_status = get_source_attribute(local_gap, local_orig_id)
                    except Exception:
                        print(f'Assinging match code -1 to {state} Local feature {local_orig_id}')
                        local_match_code = -1

                # State match codes
                state_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [state_id_col, state_code_col, state_pct_overlap_col]].drop_duplicates()
                state_matched_src_id = state_match_ss.iloc[0, 0]
                state_match_code = state_match_ss.iloc[0, 1]
                state_pct_overlap = state_match_ss.iloc[0, 2]
                state_orig_id = get_src_orig_id(state, state_matched_src_id)
                try:
                    state_gap_status = get_source_attribute(state_gap, state_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to {state} feature {state_orig_id}')
                    state_match_code = -1
                # TNC match codes
                tnc_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["tnc_id", "tnc_match_code", "tnc_pct_overlap"]].drop_duplicates()
                tnc_matched_src_id = tnc_match_ss.iloc[0, 0]
                tnc_match_code = tnc_match_ss.iloc[0, 1]
                tnc_pct_overlap = tnc_match_ss.iloc[0, 2]
                tnc_orig_id = get_src_orig_id('TNC', tnc_matched_src_id)
                try:
                    tnc_gap_status = get_source_attribute(tnc_gap, tnc_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to TNC feature {tnc_orig_id}')
                    tnc_match_code = -1
                # NCED match codes
                nced_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["nced_id", "nced_match_code", "nced_pct_overlap"]].drop_duplicates()
                nced_matched_src_id = nced_match_ss.iloc[0, 0]
                nced_match_code = nced_match_ss.iloc[0, 1]
                nced_pct_overlap = nced_match_ss.iloc[0, 2]
                nced_orig_id = get_src_orig_id('NCED', nced_matched_src_id)
                try:
                    nced_gap_status = get_source_attribute(nced_gap, nced_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to NCED feature {nced_orig_id}')
                    nced_match_code = -1
                # PADUS match codes
                padus_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["padus_id", "padus_match_code", "padus_pct_overlap"]].drop_duplicates()
                padus_matched_src_id = padus_match_ss.iloc[0, 0]
                padus_match_code = padus_match_ss.iloc[0, 1]
                padus_pct_overlap = padus_match_ss.iloc[0, 2]
                padus_orig_id = get_src_orig_id('PADUS', padus_matched_src_id)
                try:
                    padus_gap_status = get_source_attribute(padus_gap, padus_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to PADUS feature {padus_orig_id}')
                    padus_match_code = -1

                if local_fc is not None:
                    if (min_match_code <= local_match_code <= max_match_code or (local_match_code == 10 and local_pct_overlap >= min_pct_overlap)):
                        row[3] = local_gap_status
                        row[4] = local_src
                        row[5] = local_orig_id
                        c = c + 1
                        cur.updateRow(row)   # Update row
                        continue             # And continue to next so code below is not run
                if (min_match_code <= state_match_code <= max_match_code or (state_match_code == 10 and state_pct_overlap >= min_pct_overlap)):
                    row[3] = state_gap_status
                    row[4] = state_src
                    row[5] = state_orig_id
                    c = c + 1
                elif (min_match_code <= tnc_match_code <= max_match_code or (tnc_match_code == 10 and tnc_pct_overlap >= min_pct_overlap)):
                    row[3] = tnc_gap_status
                    row[4] = tnc_src
                    row[5] = tnc_orig_id
                    c = c + 1
                elif (min_match_code <= padus_match_code <= max_match_code or (padus_match_code == 10 and padus_pct_overlap >= min_pct_overlap)):
                    row[3] = padus_gap_status
                    row[4] = padus_src
                    row[5] = padus_orig_id
                    c = c + 1
                elif (min_match_code <= nced_match_code <= max_match_code or (nced_match_code == 10 and nced_pct_overlap >= min_pct_overlap)):
                    row[3] = nced_gap_status
                    row[4] = nced_src
                    row[5] = nced_orig_id
                    c = c + 1
                cur.updateRow(row)
            except Exception:
                print(traceback.format_exc())
                continue
    print(f'Updated GapStatus for {c} rows!')

### Function to update MA GAP 3 rows ###
# Since MassGIS can only be classified as GAP 3 or 4, we want to go over these
# rows specifically and try to update any GAP 3 rows to GAP 1 or 2 if there's a matching polygon
# from another source.
# This function looks only at rows where GapStatus = 3 and PolySource begins with 'MassGIS',
# then looks for matching polygons that have a GapStatus of 1, 2, or 39.
# Arguments:
#  - match_table (pandas df): dataframe resulting from reading in match table CSV with pd.read_csv()
#  - new_data_only (boolean): should gap status be updated only for new rows?
#                             new rows are identified by lack of FinalID
def update_massgis_gap_status(match_table, new_data_only=True):
    src_query = "State = 'MA'"
    tnc_gap = {key: value for (key, value) in arcpy.da.SearchCursor(tnc, ['UID', 'GapStatus'], where_clause=src_query)}
    nced_gap = {key: value for (key, value) in arcpy.da.SearchCursor(nced, ['UID', 'GapStatus'], where_clause=src_query)}
    padus_gap = {key: value for (key, value) in arcpy.da.SearchCursor(padus, ['UID', 'GapStatus'], where_clause=src_query)}

    fields = ['FinalID2', 'PolySource', 'PolySource_FeatID', 
              'GapStatus', 'Source_GapStatus', 'Source_GapStatus_FeatID']
    if new_data_only == False:
        query = "State = 'MA'"
    elif new_data_only == True:
        query = "State = 'MA' AND FinalID IS NULL"

    c = 0
    with arcpy.da.UpdateCursor(pos, fields, query) as cur:
        for row in cur:
            # For GAPs from MassGIS, we want to see if we can get more detail (GAPs 1 and 2)
            if row[3] == 3 and row[1][:7] == 'MassGIS':
                # TNC match codes
                tnc_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["tnc_id", "tnc_match_code", "tnc_pct_overlap"]].drop_duplicates()
                tnc_matched_src_id = tnc_match_ss.iloc[0, 0]
                tnc_match_code = tnc_match_ss.iloc[0, 1]
                tnc_pct_overlap = tnc_match_ss.iloc[0, 2]
                tnc_orig_id = get_src_orig_id('TNC', tnc_matched_src_id)
                try:
                    tnc_gap_status = get_source_attribute(tnc_gap, tnc_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to TNC feature {tnc_orig_id}')
                    tnc_match_code = -1
                # NCED match codes
                nced_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["nced_id", "nced_match_code", "nced_pct_overlap"]].drop_duplicates()
                nced_matched_src_id = nced_match_ss.iloc[0, 0]
                nced_match_code = nced_match_ss.iloc[0, 1]
                nced_pct_overlap = nced_match_ss.iloc[0, 2]
                nced_orig_id = get_src_orig_id('NCED', nced_matched_src_id)
                try:
                    nced_gap_status = get_source_attribute(nced_gap, nced_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to NCED feature {nced_orig_id}')
                    nced_match_code = -1
                # PADUS match codes
                padus_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["padus_id", "padus_match_code", "padus_pct_overlap"]].drop_duplicates()
                padus_matched_src_id = padus_match_ss.iloc[0, 0]
                padus_match_code = padus_match_ss.iloc[0, 1]
                padus_pct_overlap = padus_match_ss.iloc[0, 2]
                padus_orig_id = get_src_orig_id('PADUS', padus_matched_src_id)
                try:
                    padus_gap_status = get_source_attribute(padus_gap, padus_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to PADUS feature {padus_orig_id}')
                    padus_match_code = -1
                if ((min_match_code <= tnc_match_code <= max_match_code or (tnc_match_code == 10 and tnc_pct_overlap >= min_pct_overlap)) and
                    (tnc_gap_status == 1 or tnc_gap_status == 2 or tnc_gap_status == 39)):
                    row[3] = tnc_gap_status
                    row[4] = tnc_src
                    row[5] = tnc_orig_id
                    c = c + 1
                elif ((min_match_code <= padus_match_code <= max_match_code or (padus_match_code == 10 and padus_pct_overlap >= min_pct_overlap)) 
                      and (padus_gap_status == 1 or padus_gap_status == 2 or padus_gap_status == 39)):
                    row[3] = padus_gap_status
                    row[4] = padus_src
                    row[5] = padus_orig_id
                    c = c + 1
                elif ((min_match_code <= nced_match_code <= max_match_code or (nced_match_code == 10 and nced_pct_overlap >= min_pct_overlap)) 
                        and (nced_gap_status == 1 or nced_gap_status == 2 or nced_gap_status == 39)):
                    row[3] = nced_gap_status
                    row[4] = nced_src
                    row[5] = nced_orig_id
                    c = c + 1
                cur.updateRow(row)
    print(f'Updated GapStatus for {c} rows')


#### PUBLIC ACCESS ####
### Function to update PubAccess ###
# Again, this function is the same basic function as the others. There is nothing unique about this one.
# There are very few rows where PubAccess is manually set so these can be checked manually and 
# are skipped in this function.
# Arguments:
#  - state (text): two-letter state abbreviation in capital letters (CT, MA, RI, VT, NH, ME)
#  - state_fc (text): name of the preprocessed, singlepart state data source
#                     it should be in same GDB as NEPOS so should only need name, not full path
#  - match_table (pandas df): dataframe resulting from reading in match table CSV with pd.read_csv()
#  - unknown_only (boolean): should public access be updated only for rows where PubAccess is unknown?
#  - new_data_only (boolean): should PubAccess be updated only for new rows?
#                             new rows are identified by lack of FinalID
#  - take_only_known (boolean): should we only update NEPOS if the source has a known value (not unknown)?
def update_public_access(state, state_fc, match_table, local_fc=None, 
                         unknown_only=True, new_data_only=True, take_only_known=True):
    # Subset source data by state and (optionally) attribute value
    if take_only_known == False:
        src_query = "State = '" + state + "'"
    elif take_only_known == True:
        src_query = "State = '" + state + "' AND PubAccess <> 'Unknown' AND PubAccess IS NOT NULL"
        nced_query = src_query + " AND PubAccess <> 'No'"
    
    # Create dictionary of {UID: PubAccess} pairs for each source
    tnc_pub_access = {key: value for (key, value) in arcpy.da.SearchCursor(tnc, ['UID', 'PubAccess'], where_clause=src_query)}
    # NCED is the one source that is a little different for this attribute - NCED has many more 'No' public access records
    # than other sources - it seems like this might be a default value / not trustworthy
    # If we are updating values regardless of what the source attribute is, we can use src_query
    # But if we only want to use valid values from sources, we used nced_query to exclude No rows in addition to Unknown
    # (otherwise, we end up with a bunch of rows where PubAccess = No from NCED that are probably really just unknown)
    if take_only_known == False:
        nced_pub_access = {key: value for (key, value) in arcpy.da.SearchCursor(nced, ['UID', 'PubAccess'], where_clause=src_query)}
    else:
        nced_pub_access = {key: value for (key, value) in arcpy.da.SearchCursor(nced, ['UID', 'PubAccess'], where_clause=nced_query)}
    padus_pub_access = {key: value for (key, value) in arcpy.da.SearchCursor(padus, ['UID', 'PubAccess'], where_clause=src_query)}
    state_pub_access = {key: value for (key, value) in arcpy.da.SearchCursor(state_fc, ['UID', 'PubAccess'], where_clause=src_query)}
    if local_fc is not None:
        local_pub_access = {key: value for (key, value) in arcpy.da.SearchCursor(local_fc, ['UID', 'PubAccess'], where_clause=src_query)}
        local_items = get_local_info()
        local_id_col = local_items[0]
        local_code_col = local_items[1]
        local_pct_overlap_col = local_items[2]
        local_src = local_items[3]
    
    # Subset NEPOS for UpdateCursor based on function parameters
    if unknown_only == True and new_data_only == True:
        query = "State = '" + state + "' AND PubAccess = 'Unknown' AND FinalID IS NULL"
    elif unknown_only == True and new_data_only == False:
        query = "State = '" + state + "' AND PubAccess = 'Unknown'"
    elif unknown_only == False and new_data_only == True:
        query = "State = '" + state + "' AND FinalID IS NULL"
    elif unknown_only == False and new_data_only == False:
        query = "State = '" + state + "'"

    # Get state column name in match table
    state_items = get_state_info(state)
    state_id_col = state_items[0]
    state_code_col = state_items[1]
    state_pct_overlap_col = state_items[2]
    state_src = state_items[3]
    
    fields = ['FinalID2', 'PolySource', 'PolySource_FeatID', 
              'PubAccess', 'Source_PubAccess', 'Source_PubAccess_FeatID']
    c = 0
    with arcpy.da.UpdateCursor(pos, fields, query) as cur:
        for row in cur:
            try:
                if 'Harvard Forest' in row[4]:
                    continue

                if local_fc is not None:
                    local_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [local_id_col, local_code_col, local_pct_overlap_col]].drop_duplicates()
                    local_matched_src_id = local_match_ss.iloc[0, 0]
                    local_match_code = local_match_ss.iloc[0, 1]
                    local_pct_overlap = local_match_ss.iloc[0, 2]
                    local_orig_id = get_src_orig_id(state, local_matched_src_id)
                    try:
                        local_access = get_source_attribute(local_pub_access, local_orig_id)
                    except Exception:
                        print(f'Setting match code to -1 for {state} Local feature {local_orig_id}')
                        local_match_code = -1
                # State match codes
                state_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [state_id_col, state_code_col, state_pct_overlap_col]].drop_duplicates()
                state_matched_src_id = state_match_ss.iloc[0, 0]
                state_match_code = state_match_ss.iloc[0, 1]
                state_pct_overlap = state_match_ss.iloc[0, 2]
                state_orig_id = get_src_orig_id(state, state_matched_src_id)
                try:
                    state_access = get_source_attribute(state_pub_access, state_orig_id)
                except Exception:
                        print(f'Assigned match code -1 to {state} feature {state_orig_id}')
                        state_match_code = -1
                
                # TNC match codes
                tnc_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["tnc_id", "tnc_match_code", "tnc_pct_overlap"]].drop_duplicates()
                tnc_matched_src_id = tnc_match_ss.iloc[0, 0]
                tnc_match_code = tnc_match_ss.iloc[0, 1]
                tnc_pct_overlap = tnc_match_ss.iloc[0, 2]
                tnc_orig_id = get_src_orig_id('TNC', tnc_matched_src_id)
                try:
                    tnc_access = get_source_attribute(tnc_pub_access, tnc_orig_id)
                except Exception:
                    print(f'Assigned match code -1 to TNC feature {tnc_orig_id}')
                    tnc_match_code = -1
                
                # NCED match codes
                nced_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["nced_id", "nced_match_code", "nced_pct_overlap"]].drop_duplicates()
                nced_matched_src_id = nced_match_ss.iloc[0, 0]
                nced_match_code = nced_match_ss.iloc[0, 1]
                nced_pct_overlap = nced_match_ss.iloc[0, 2]
                nced_orig_id = get_src_orig_id('NCED', nced_matched_src_id)
                try:
                    nced_access = get_source_attribute(nced_pub_access, nced_orig_id)
                except Exception:
                    print(f'Assigned match code -1 to NCED feature {nced_orig_id}')
                    nced_match_code = -1
                
                # PADUS match codes
                padus_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["padus_id", "padus_match_code", "padus_pct_overlap"]].drop_duplicates()
                padus_matched_src_id = padus_match_ss.iloc[0, 0]
                padus_match_code = padus_match_ss.iloc[0, 1]
                padus_pct_overlap = padus_match_ss.iloc[0, 2]
                padus_orig_id = get_src_orig_id('PADUS', padus_matched_src_id)
                try:
                    padus_access = get_source_attribute(padus_pub_access, padus_matched_src_id)
                except Exception:
                    print(f'Assigned match code -1 to PADUS feature {padus_orig_id}')
                    padus_match_code = -1
                
                # Use the match codes and values to assign a new value if possible
                # Any -1 match codes will not execute in these conditionals
                if local_fc is not None:
                    if (min_match_code <= local_match_code <= max_match_code or (local_match_code == 10 and local_pct_overlap >= min_pct_overlap)):
                        row[3] = local_access
                        row[4] = local_src
                        row[5] = local_orig_id
                        c = c + 1
                        cur.updateRow(row)
                        continue
                if (min_match_code <= state_match_code <= max_match_code or (state_match_code == 10 and state_pct_overlap >= min_pct_overlap)):
                    row[3] = state_access
                    row[4] = state_src
                    row[5] = state_orig_id
                    c = c + 1
                elif (min_match_code <= tnc_match_code <= max_match_code or (tnc_match_code == 10 and tnc_pct_overlap >= min_pct_overlap)):
                    row[3] = tnc_access
                    row[4] = tnc_src
                    row[5] = tnc_orig_id
                    c = c + 1
                # NOTE: For NCED we use additional criteria that PubAccess != No
                # It seems like NCED uses No far more than most sources and it might be a default
                # value. It often conflicts with all other sources and doesn't seem reliable.
                # Could change this in the future if that changes.
                elif (min_match_code <= nced_match_code <= max_match_code or (nced_match_code == 10 and nced_pct_overlap >= min_pct_overlap)):
                    row[3] = nced_access
                    row[4] = nced_src
                    row[5] = nced_orig_id
                    c = c + 1
                elif (min_match_code <= padus_match_code <= max_match_code or (padus_match_code == 10 and padus_pct_overlap >= min_pct_overlap)):
                    row[3] = padus_access
                    row[4] = padus_src
                    row[5] = padus_orig_id
                    c = c + 1
                cur.updateRow(row)
            except Exception:
                print(traceback.format_exc())
                continue   # Go to the next row
    print(f'Updated PubAccess for {c} rows!')


#### YEAR PROTECTED ####
### Function to update YearProt ###
# This function does the same update process as the others - the only difference is that it also
# checks if YearProt_Final = 1 prior to updating YearProt. If YearProt_Final = 1 
# (or there is a manual YearProt, just like with other attributes) and the source year conflicts with NEPOS
# year, we flag this (field Year_Flag is created in this function for this purpose) and don't update
# YearProt. If it's an SRM year or YearProt_Final year only (not a manual edit), and the years match,
# we update NEPOS year and Source_YearProt (we want our data to remain current and update to latest source if possible).
# If it's a manually edited year (usually these are LPTs where we get the year from the easement), we DON'T
# want to update even if the years match - we want to retain these manual edits. In this case, we note in YearFlag
# that the manual year matches the source year just to leave a trace of this instance in the data and be able to identify
# these rows if desired.
# As an additional tool to check this field, the function creates the field Year_Diff and calculates the
# difference between the existing YearProt and the new YearProt. Large changes can indicate some sort of ownership
# or protection mechanism change rather than a true change in the year of original protection.
# Currently, the columns created in this script do not get deleted in any other script - they
# need to be deleted manually once no longer needed (can be left in place until all states are done).
# Arguments:
#  - state (text): two-letter state abbreviation in capital letters (CT, MA, RI, VT, NH, ME)
#  - state_fc (text): name of the preprocessed, singlepart state data source
#                     it should be in same GDB as NEPOS so should only need name, not full path
#  - match_table (pandas df): dataframe resulting from reading in match table CSV with pd.read_csv()
#  - local_fc (text): name of RI Local dataset
#  - unknown_only (boolean): should year protected be updated only for rows where YearProt is unknown?
#  - new_data_only (boolean): should YearProt be updated only for new rows?
#                             new rows are identified by lack of FinalID
#  - take_only_known (boolean): should we only update NEPOS if the source has a known value (not unknown)?
def update_year_prot(state, state_fc, match_table, local_fc=None, 
                     unknown_only=True, new_data_only=True, take_only_known=True):
    if take_only_known == False:
        src_query = "State = '" + state + "'"
    elif take_only_known == True:
        src_query = "State = '" + state + "' AND YearProt > 0 AND YearProt IS NOT NULL"
    
    tnc_year_prot = {key: value for (key, value) in arcpy.da.SearchCursor(tnc, ['UID', 'YearProt'], where_clause=src_query)}
    nced_year_prot = {key: value for (key, value) in arcpy.da.SearchCursor(nced, ['UID', 'YearProt'], where_clause=src_query)}
    padus_year_prot = {key: value for (key, value) in arcpy.da.SearchCursor(padus, ['UID', 'YearProt'], where_clause=src_query)}
    state_year_prot = {key: value for (key, value) in arcpy.da.SearchCursor(state_fc, ['UID', 'YearProt'], where_clause=src_query)}
    # For MA, we also want to take YearProtComments because that field records if the date is from FY_FUNDING
    if state == 'MA':
        state_year_prot_comments = {key: value for (key, value) in arcpy.da.SearchCursor(state_fc, ['UID', 'YearProtComments'], where_clause=src_query)}
    if local_fc is not None:
        local_year_prot = {key: value for (key, value) in arcpy.da.SearchCursor(local_fc, ['UID', 'YearProt'], where_clause=src_query)}
        local_items = get_local_info()
        local_id_col = local_items[0]
        local_code_col = local_items[1]
        local_pct_overlap_col = local_items[2]
        local_src = local_items[3]

    # Subset NEPOS for UpdateCursor based on function parameters
    if unknown_only == True and new_data_only == True:
        query = "State = '" + state + "' AND YearProt = 0 AND FinalID IS NULL"
    elif unknown_only == True and new_data_only == False:
        query = "State = '" + state + "' AND YearProt = 0"
    elif unknown_only == False and new_data_only == True:
        query = "State = '" + state + "' AND FinalID IS NULL"
    elif unknown_only == False and new_data_only == False:
        query = "State = '" + state + "'"

    # Get state column name in match table
    state_items = get_state_info(state)
    state_id_col = state_items[0]
    state_code_col = state_items[1]
    state_pct_overlap_col = state_items[2]
    state_src = state_items[3]

    # Create the Year_Flag and Year_Diff fields to store extra info about years
    # These fields can be left until updating years is complete for all states
    # but need to be deleted manually once attribute updates are done
    try:
        arcpy.management.AddField(pos, "Year_Flag", "TEXT", field_length="225")
        arcpy.management.AddField(pos, "Year_Diff", "SHORT")
    except Exception:
        pass

    fields = ['FinalID2', 'PolySource', 'PolySource_FeatID', 
              'YearProt', 'Source_YearProt', 'Source_YearProt_FeatID', 
              'YearProtComments', 'YearProt_Final', "Year_Flag", "Year_Diff"]
    c = 0
    with arcpy.da.UpdateCursor(pos, fields, query) as cur:
        for row in cur:
            try:
                # Get current year
                old_yearprot = row[3]

                if local_fc is not None:
                    local_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [local_id_col, local_code_col, local_pct_overlap_col]].drop_duplicates()
                    local_matched_src_id = local_match_ss.iloc[0, 0]
                    local_match_code = local_match_ss.iloc[0, 1]
                    local_pct_overlap = local_match_ss.iloc[0, 2]
                    local_orig_id = get_src_orig_id(state, local_matched_src_id)
                    try:
                        local_year = get_source_attribute(local_year_prot, local_orig_id)
                    except Exception:
                        print(f'Setting match code to -1 for {state} Local feature {local_orig_id}')
                        local_match_code = -1
                # State match codes
                state_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [state_id_col, state_code_col, state_pct_overlap_col]].drop_duplicates()
                state_matched_src_id = state_match_ss.iloc[0, 0]
                state_match_code = state_match_ss.iloc[0, 1]
                state_pct_overlap = state_match_ss.iloc[0, 2]
                state_orig_id = get_src_orig_id(state, state_matched_src_id)
                try:
                    state_year = get_source_attribute(state_year_prot, state_orig_id)
                    # If state is MA (MassGIS), we also want to get YearProtComments
                    if state == 'MA':
                        massgis_year_prot_comm = get_source_attribute(state_year_prot_comments, state_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to {state} feature P{state_orig_id}')
                    state_match_code = -1

                # TNC match codes
                tnc_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["tnc_id", "tnc_match_code", "tnc_pct_overlap"]].drop_duplicates()
                tnc_matched_src_id = tnc_match_ss.iloc[0, 0]
                tnc_match_code = tnc_match_ss.iloc[0, 1]
                tnc_pct_overlap = tnc_match_ss.iloc[0, 2]
                tnc_orig_id = get_src_orig_id('TNC', tnc_matched_src_id)
                try:
                    tnc_year = get_source_attribute(tnc_year_prot, tnc_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to TNC feature {tnc_orig_id}')
                    tnc_match_code = -1
                
                # NCED match codes
                nced_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["nced_id", "nced_match_code", "nced_pct_overlap"]].drop_duplicates()
                nced_matched_src_id = nced_match_ss.iloc[0, 0]
                nced_match_code = nced_match_ss.iloc[0, 1]
                nced_pct_overlap = nced_match_ss.iloc[0, 2]
                nced_orig_id = get_src_orig_id('NCED', nced_matched_src_id)
                try:
                    nced_year = get_source_attribute(nced_year_prot, nced_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to NCED feature {nced_orig_id}')
                    nced_match_code = -1
                
                # PADUS match codes
                padus_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["padus_id", "padus_match_code", "padus_pct_overlap"]].drop_duplicates()
                padus_matched_src_id = padus_match_ss.iloc[0, 0]
                padus_match_code = padus_match_ss.iloc[0, 1]
                padus_pct_overlap = padus_match_ss.iloc[0, 2]
                padus_orig_id = get_src_orig_id('PADUS', padus_matched_src_id)
                try:
                    padus_year = get_source_attribute(padus_year_prot, padus_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to PADUS feature {padus_orig_id}')
                    padus_match_code = -1
                
                # If match code if 1-6 and a valid year, update NEPOS YearProt
                # Also take values if match code is 10 and percent overlap is 90-100%
                # These are instances where NEPOS has more detail than source and high % overlap
                # suggests the polygon is within the source polygon and it is safe to take attributes
                # If there is a decent match with a source polygon that has a non-zero year,
                # we further check to see if YearProt_Final = 1 or if it has a manual (Harvard Forest) or 
                # SRM year. If it does, and the source year conflicts with NEPOS year, we flag this
                # and do not update the year (we will check manually). If this condition is not met
                # (i.e., there are no special interest flags OR the source year matches NEPOS year)
                # then we update NEPOS YearProt.
                if local_fc is not None:
                    if (min_match_code <= local_match_code <= max_match_code or (local_match_code == 10 and local_pct_overlap >= min_pct_overlap)):
                        if (row[7] == 1 or 'Harvard Forest' in row[4] or 'SRM' in row[4]) and local_year != row[3]:
                            row[8] = f"Year conflict b/w NEPOS and {local_src}"
                        elif 'Harvard Forest' in row[4] and local_year == row[3]:
                            row[8] = f"Harvard Forest year matches {local_src}"
                        else:
                            row[3] = local_year
                            row[4] = local_src
                            row[5] = local_orig_id
                            c = c + 1
                        cur.updateRow(row) # Update row
                        continue           # Go to next row so conditionals below are not run
                if (min_match_code <= state_match_code <= max_match_code or (state_match_code == 10 and state_pct_overlap >= min_pct_overlap)):
                    if (row[7] == 1 or 'Harvard Forest' in row[4] or 'SRM' in row[4]) and state_year != row[3]:
                            row[8] = f"Year conflict b/w NEPOS and {state_src}"
                    elif 'Harvard Forest' in row[4] and state_year == row[3]:
                            row[8] = f"Harvard Forest year matches {state_src}"
                    else:
                        row[3] = state_year
                        row[4] = state_src
                        row[5] = state_orig_id
                        c = c + 1
                    # For MassGIS, we want to carry over comments in case the date
                    # is from FY_FUNDING for our reference
                    if state == 'MA':
                        if massgis_year_prot_comm is not None:
                            if row[6] is None:
                                row[6] = massgis_year_prot_comm
                            else:
                                row[6] = row[6] + " -- " + massgis_year_prot_comm
                elif (min_match_code <= tnc_match_code <= max_match_code or (tnc_match_code == 10 and tnc_pct_overlap >= min_pct_overlap)):
                    if (row[7] == 1 or 'Harvard Forest' in row[4] or 'SRM' in row[4]) and tnc_year != row[3]:
                            row[8] = f"Year conflict b/w NEPOS and {tnc_src}"
                    elif 'Harvard Forest' in row[4] and tnc_year == row[3]:
                            row[8] = f"Harvard Forest year matches {tnc_src}"
                    else:
                        row[3] = tnc_year
                        row[4] = tnc_src
                        row[5] = tnc_orig_id
                        c = c + 1
                elif (min_match_code <= nced_match_code <= max_match_code or (nced_match_code == 10 and nced_pct_overlap >= min_pct_overlap)):
                    if (row[7] == 1 or 'Harvard Forest' in row[4] or 'SRM' in row[4]) and nced_year != row[3]:
                            row[8] = f"Year conflict b/w NEPOS and {nced_src}"
                    elif 'Harvard Forest' in row[4] and nced_year == row[3]:
                            row[8] = f"Harvard Forest year matches {nced_src}"
                    else:
                        row[3] = nced_year
                        row[4] = nced_src
                        row[5] = nced_orig_id
                        c = c + 1
                elif (min_match_code <= padus_match_code <= max_match_code or (padus_match_code == 10 and padus_pct_overlap >= min_pct_overlap)):
                    if (row[7] == 1 or 'Harvard Forest' in row[4] or 'SRM' in row[4]) and padus_year != row[3]:
                            row[8] = f"Year conflict b/w NEPOS and {padus_src}"
                    elif 'Harvard Forest' in row[4] and padus_year == row[3]:
                            row[8] = f"Harvard Forest year matches {padus_src}"
                    else:
                        row[3] = padus_year
                        row[4] = padus_src
                        row[5] = padus_orig_id
                        c = c + 1
                cur.updateRow(row)  # Update YearProt

                # Calculate the difference between the old YearProt and the updated YearProt
                # A very large change can indicate that there is a new owner or protection mechanism
                # so we can check these out for any suspiciously large changes in YearProt
                year_diff = row[3] - old_yearprot
                row[9] = year_diff
                cur.updateRow(row)
            except Exception:
                print(traceback.format_exc())
                continue
    print(f'Updated YearProt for {c} rows!')

### Function to correct 2003 CT rows ###
# There is a data error that will hopefully be resolves in the future and this code
# will no longer be needed. The issue is that in 2018 TNC data, there are many rows
# in CT where YearProt = 2003 but the DATE_PREC says "pre" -- this means the row was
# protected at some point before 2003. In 2022 TNC data (and also PADUS 4.0 data)
# the YEAR_EST field is 2003 but there's no indication this is latest possible year.
# Therefore, we don't rely on this data and we need to either 1) get a different year
# for these CT rows, or 2) set it to 0 as we don't have a reliable estimate.
#
# The argument unknown_only is not relevant for this function because we are focused
# on rows where YearProt = 2003 in NEPOS.
#
# take_only_known is still relevant because you can run the function once with set to
# True and see if there are any non-2003 AND non-zero years available in other sources.
# And if not (which is likely the case) you can run it again with take_only_known set
# to False. new_data_only is also relevant as this could be done for new rows only
# or all data.
#
# NOTE: After this, FeeYear and EaseYear should be checked for these rows (if it is
# possible that FeeYear or EaseYear was set based on the 2003 value). If this function
# is always run before running function for FeeYear/EaseYear, that shouldn't be an issue.
def correct_ct_2003_rows(state_fc, match_table, take_only_known=True, new_data_only=True):
    state = 'CT'

    # Subset source data
    if take_only_known == False:
        src_query = "State = '" + state + "'"
    elif take_only_known == True:
        src_query = "State = '" + state + "' AND YearProt > 0 AND YearProt IS NOT NULL"

    nced_year_prot = {key: value for (key, value) in arcpy.da.SearchCursor(nced, ['UID', 'YearProt'], where_clause=src_query)}
    padus_year_prot = {key: value for (key, value) in arcpy.da.SearchCursor(padus, ['UID', 'YearProt'], where_clause=src_query)}
    state_year_prot = {key: value for (key, value) in arcpy.da.SearchCursor(state_fc, ['UID', 'YearProt'], where_clause=src_query)}

    # Subset NEPOS for UpdateCursor based on function parameters
    # We are just focused on CT rows where YearProt is 2003, but this
    # can include either just new data or all data
    if new_data_only == True:
        query = "State = 'CT' AND FinalID IS NULL AND YearProt = 2003"
    elif new_data_only == False:
        query = "State = 'CT' AND YearProt = 2003"

    # Get state column name in match table
    state_items = get_state_info(state)
    state_id_col = state_items[0]
    state_code_col = state_items[1]
    state_pct_overlap_col = state_items[2]
    state_src = state_items[3]

    fields = ['FinalID2', 'PolySource', 'PolySource_FeatID', 
              'YearProt', 'Source_YearProt', 'Source_YearProt_FeatID']
    c = 0
    with arcpy.da.UpdateCursor(pos, fields, query) as cur:
        for row in cur:
            try:
                # Get current year
                old_yearprot = row[3]

                # State match codes
                state_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [state_id_col, state_code_col, state_pct_overlap_col]].drop_duplicates()
                state_matched_src_id = state_match_ss.iloc[0, 0]
                state_match_code = state_match_ss.iloc[0, 1]
                state_pct_overlap = state_match_ss.iloc[0, 2]
                state_orig_id = get_src_orig_id(state, state_matched_src_id)
                try:
                    state_year = get_source_attribute(state_year_prot, state_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to {state} feature P{state_orig_id}')
                    state_match_code = -1
                
                # NCED match codes
                nced_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["nced_id", "nced_match_code", "nced_pct_overlap"]].drop_duplicates()
                nced_matched_src_id = nced_match_ss.iloc[0, 0]
                nced_match_code = nced_match_ss.iloc[0, 1]
                nced_pct_overlap = nced_match_ss.iloc[0, 2]
                nced_orig_id = get_src_orig_id('NCED', nced_matched_src_id)
                try:
                    nced_year = get_source_attribute(nced_year_prot, nced_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to NCED feature {nced_orig_id}')
                    nced_match_code = -1
                
                # PADUS match codes
                padus_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["padus_id", "padus_match_code", "padus_pct_overlap"]].drop_duplicates()
                padus_matched_src_id = padus_match_ss.iloc[0, 0]
                padus_match_code = padus_match_ss.iloc[0, 1]
                padus_pct_overlap = padus_match_ss.iloc[0, 2]
                padus_orig_id = get_src_orig_id('PADUS', padus_matched_src_id)
                try:
                    padus_year = get_source_attribute(padus_year_prot, padus_orig_id)
                except Exception:
                    print(f'Assigning match code -1 to PADUS feature {padus_orig_id}')
                    padus_match_code = -1
                
                # For each source, we compare the match code and % overlap and then update the row as long
                # as the year is not from a manual source. The conditionals do not check for the value
                # of the source year -- that should be handled in take_only_known and unknown_only arguments!
                if (min_match_code <= state_match_code <= max_match_code or (state_match_code == 10 and state_pct_overlap >= min_pct_overlap)):
                    if (row[7] == 1 or 'Harvard Forest' in row[4] or 'SRM' in row[4]) and state_year != row[3]:
                            row[8] = f"Year conflict b/w NEPOS and {state_src}"
                    elif 'Harvard Forest' in row[4] and state_year == row[3]:
                            row[8] = f"Harvard Forest year matches {state_src}"
                    else:
                        row[3] = state_year
                        row[4] = state_src
                        row[5] = state_orig_id
                        c = c + 1
                elif (min_match_code <= nced_match_code <= max_match_code or (nced_match_code == 10 and nced_pct_overlap >= min_pct_overlap)):
                    if (row[7] == 1 or 'Harvard Forest' in row[4] or 'SRM' in row[4]) and nced_year != row[3]:
                            row[8] = f"Year conflict b/w NEPOS and {nced_src}"
                    elif 'Harvard Forest' in row[4] and nced_year == row[3]:
                            row[8] = f"Harvard Forest year matches {nced_src}"
                    else:
                        row[3] = nced_year
                        row[4] = nced_src
                        row[5] = nced_orig_id
                        c = c + 1
                elif (min_match_code <= padus_match_code <= max_match_code or (padus_match_code == 10 and padus_pct_overlap >= min_pct_overlap)):
                    if (row[7] == 1 or 'Harvard Forest' in row[4] or 'SRM' in row[4]) and padus_year != row[3]:
                            row[8] = f"Year conflict b/w NEPOS and {padus_src}"
                    elif 'Harvard Forest' in row[4] and padus_year == row[3]:
                            row[8] = f"Harvard Forest year matches {padus_src}"
                    else:
                        row[3] = padus_year
                        row[4] = padus_src
                        row[5] = padus_orig_id
                        c = c + 1
                cur.updateRow(row)  # Update YearProt
            except Exception:
                print(traceback.format_exc())
                continue
    print(f'Correct YearProt for {c} rows!')


#### FEE YEAR AND EASE YEAR ####
# FeeYear and EaseYear are populated based on ProtType.
# This is only possible to do automatically for rows where ProtType is either Fee OR Ease.
# FeeYear and EaseYear are only populated is there is a known YearProt.
# Similar to other fields, there is option to run this function only for new data
# and/or only for data where FeeYear or EaseYear is empty.
# In either case, the function checks to see if FeeYear_Final or EaseYear_Final is 1.
# If FeeYear or EaseYear is not final, the field is updated. If FeeYear or EaseYear
# is final and the updated FeeYear/EaseYear matches, the source fields are updated to
# keep data current. If FeeYear/EaseYear is final and the potential new Fee/Ease year
# conflicts with existing, the year is not updated and it is flagged.
# The flagging fields FeeYear_Flag and EaseYear_Flag created in this script need to be
# deleted manually once no longer needed.
# Arguments:
#  - state (text): two-letter state abbreviation in capital letters (CT, MA, RI, VT, NH, ME)
#  - null_only (boolean): should fee/ease year be updated only for rows where it is empty?
#  - new_data_only (boolean): should fee/ease year be updated only for new rows?
#                             new rows are identified by lack of FinalID
def populate_fee_ease_year(state, null_only=True, new_data_only=True):
    # Set query
    if new_data_only == False:
        query = "State = '" + state + "'"
    elif new_data_only == True:
        query = "State = '" + state + "' AND FinalID IS NULL"

    try:
        arcpy.management.AddField(pos, "FeeYear_Flag", "TEXT", field_length=225)
        arcpy.management.AddField(pos, "EaseYear_Flag", "TEXT", field_length=225)
    except Exception:
        pass

    # First we will do FeeYear
    fields = ['YearProt', 'Source_YearProt', 'Source_YearProt_FeatID',
              'FeeYear', 'Source_FeeYear', 'Source_FeeYear_FeatID', 
              'FeeYear_Final', "FeeYear_Flag"]
    
    if null_only == False:
        fee_query = query + " AND ProtType = 'Fee'"
    elif null_only == True:
        fee_query = query + " AND ProtType = 'Fee' AND FeeYear IS NULL"

    c = 0
    with arcpy.da.UpdateCursor(pos, fields, fee_query) as cur:
        for row in cur:
            if row[0] is None:       # In case any null YearProt slipped through
                row[0] = 0           # Change to 0 so can be evaluated in conditional below
                cur.updateRow(row)   # And update the row so the change takes effect
            
            # If YearProt is not zero, and FeeYear_Final is not 1, we update FeeYear with YearProt
            if row[0] > 0 and row[6] != 1:
                row[3] = row[0]
                row[4] = row[1]
                row[5] = row[2]
                c = c + 1
            elif row[0] > 0 and row[6] == 1:
                if row[0] != row[3]:
                    row[7] = f"FeeYear is final and there is conflict b/w current FeeYear and potential new FeeYear (updated YearProt)"
                elif row[0] == row[3]:
                    row[3] = row[0]
                    row[4] = row[1]
                    row[5] = row[2]
                    c = c + 1
            cur.updateRow(row)
    del row, cur
    print(f'Populated FeeYear for {c} rows where ProtType = Fee and YearProt > 0')

    fields = ['YearProt', 'Source_YearProt', 'Source_YearProt_FeatID',
              'EaseYear', 'Source_EaseYear', 'Source_EaseYear_FeatID',
              "EaseYear_Final", "EaseYear_Flag"]
    # For EaseYear we only take ProtType Ease... unclear whether DR, ROW, Lease should be included here...
    if null_only == False:
        ease_query = query + " AND ProtType = 'Ease'"
    elif null_only == True:
        ease_query = query + " AND ProtType = 'Ease' AND EaseYear IS NULL"

    c = 0
    with arcpy.da.UpdateCursor(pos, fields, ease_query) as cur:
        for row in cur:
            if row[0] is None:
                row[0] = 0
                cur.updateRow(row)
            
            if row[0] > 0 and row[6] != 1:
                row[3] = row[0]
                row[4] = row[1]
                row[5] = row[2]
                c = c + 1
            elif row[0] > 0 and row[6] == 1:
                if row[0] != row[3]:
                    row[7] = f"EaseYear is final and there is conflict b/w current EaseYear and potential new EaseYear (updated YearProt)"
                elif row[0] == row[3]:
                    row[3] = row[0]
                    row[4] = row[1]
                    row[5] = row[2]
                    c = c + 1
                cur.updateRow(row)
    del row, cur
    print(f'Populated EaseYear for {c} rows where ProtType = Ease and YearProt > 0')

    # What about Fee and Ease rows???? Can something be done here to try and capture those years? Is it worth trying to figure out?

### Function to populate FeeYear and EaseYear in RI ###
# RI is a little different since there are 2 state sources we have other
# ways of inferring FeeYear and EaseYear using both sources.
# This function doesn't check for manual years but does check for FeeYear_Final and EaseYear_Final
# and flags in a corresponing flag field if there is a conflict b/w a final ease/fee year
# and a potential new year.
# Arguments:
#  - ri_state (text): RI State data layer name
#  - ri_local (text): RI Local data layer name
#  - match_table (pandas df): dataframe resulting from reading in match table CSV with pd.read_csv()
#  - null_only (boolean): should fee/ease year be populated only for rows where it is currently empty?
#  - new_data_only (boolean): should fee/ease year be populated for new rows only? (based on lack of FinalID)
def populate_fee_ease_year_ri(ri_state, ri_local, match_table, null_only=True, new_data_only=True):
    ri_state_fee_years = {key: value for (key, value) in arcpy.da.SearchCursor(ri_state, ['UID', 'FeeYear'])}
    ri_local_fee_years = {key: value for (key, value) in arcpy.da.SearchCursor(ri_local, ['UID', 'FeeYear'])}
    ri_state_ease_years = {key: value for (key, value) in arcpy.da.SearchCursor(ri_state, ['UID', 'EaseYear'])}
    ri_local_ease_years = {key: value for (key, value) in arcpy.da.SearchCursor(ri_local, ['UID', 'EaseYear'])}

    state_items = get_state_info('RI')
    state_id_col = state_items[0]
    state_code_col = state_items[1]
    state_pct_overlap_col = state_items[2]
    state_src = state_items[3]

    local_items = get_local_info()
    local_id_col = local_items[0]
    local_code_col = local_items[1]
    local_pct_overlap_col = local_items[2]
    local_src = local_items[3]

    if null_only == True and new_data_only == True:
        fee_query = "State = 'RI' AND FeeYear IS NULL AND FinalID IS NULL"
        ease_query = "State = 'RI' AND EaseYear IS NULL AND FinalID IS NULL"
    elif null_only == False and new_data_only == True:
        fee_query = "State = 'RI' AND FinalID IS NULL"
        ease_query = "State = 'RI' AND FinalID IS NULL"
    elif null_only == True and new_data_only == False:
        fee_query = "State = 'RI' AND FeeYear IS NULL"
        ease_query = "State = 'RI' AND EaseYear IS NULL"
    elif null_only == False and new_data_only == False:
        fee_query = "State = 'RI'"
        ease_query = "State = 'RI'"

    c = 0
    fields = ['FinalID2', 'FeeYear', 'Source_FeeYear', 'Source_FeeYear_FeatID', 'FeeYear_Final', 'FeeYear_Flag']
    with arcpy.da.UpdateCursor(pos, fields, fee_query) as cur:
        for row in cur:
            # Get state and local year if possible
            state_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [state_id_col, state_code_col, state_pct_overlap_col]].drop_duplicates()
            state_matched_src_id = state_match_ss.iloc[0, 0]
            state_match_code = state_match_ss.iloc[0, 1]
            state_pct_overlap = state_match_ss.iloc[0, 2]
            state_orig_id = get_src_orig_id('RI', state_matched_src_id)
            try:
                state_fee_year = get_source_attribute(ri_state_fee_years, state_orig_id)
            except Exception:
                print(f'Assigning match code -1 to RI State feature {state_orig_id}')
                state_match_code = -1
            local_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [local_id_col, local_code_col, local_pct_overlap_col]].drop_duplicates()
            local_matched_src_id = local_match_ss.iloc[0, 0]
            local_match_code = local_match_ss.iloc[0, 1]
            local_pct_overlap = local_match_ss.iloc[0, 2]
            local_orig_id = get_src_orig_id('RI', local_matched_src_id)
            try:
                local_fee_year = get_source_attribute(ri_local_fee_years, local_orig_id)
            except Exception:
                print(f'Assigning match code -1 to RI Local feature {local_orig_id}')
                local_match_code = -1
            # Assign FeeYear
            if ((min_match_code <= state_match_code <= max_match_code or (state_match_code == 10 and state_pct_overlap >= min_pct_overlap)) 
                and state_fee_year is not None):
                if row[4] != 1 or (row[4] == 1 and row[1] == state_fee_year):
                    row[1] = state_fee_year
                    row[2] = state_src
                    row[3] = state_orig_id
                    c = c + 1
                elif row[4] == 1 and row[1] != state_fee_year:
                    row[5] = "FeeYear is final and there is conflict b/w current FeeYear and potential new FeeYear (updated YearProt)"
            elif ((min_match_code <= local_match_code <= max_match_code or (local_match_code == 10 and local_pct_overlap >= min_pct_overlap)) 
                and local_fee_year is not None):
                if row[4] != 1 or (row[4] == 1 and row[1] == local_fee_year):
                    row[1] = local_fee_year
                    row[2] = local_src
                    row[3] = local_orig_id
                    c = c + 1
                elif row[4] == 1 and row[1] != local_fee_year:
                    row[5] = "FeeYear is final and there is conflict b/w current FeeYear and potential new FeeYear (updated YearProt)"
            cur.updateRow(row)
    print(f'Updated FeeYear for {c} rows...')
    del row, cur
    
    c = 0
    fields = ['FinalID2', 'EaseYear', 'Source_EaseYear', 'Source_EaseYear_FeatID', 'EaseYear_Final', 'EaseYear_Flag']
    with arcpy.da.UpdateCursor(pos, fields, ease_query) as cur:
        for row in cur:
            # Get state and local year if possible
            state_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [state_id_col, state_code_col, state_pct_overlap_col]].drop_duplicates()
            state_matched_src_id = state_match_ss.iloc[0, 0]
            state_match_code = state_match_ss.iloc[0, 1]
            state_pct_overlap = state_match_ss.iloc[0, 2]
            state_orig_id = get_src_orig_id('RI', state_matched_src_id)
            try:
                state_ease_year = get_source_attribute(ri_state_ease_years, state_orig_id)
            except Exception:
                print(f'Assigning match code -1 to RI State feature {state_orig_id}')
                state_match_code = -1
            local_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [local_id_col, local_code_col, local_pct_overlap_col]].drop_duplicates()
            local_matched_src_id = local_match_ss.iloc[0, 0]
            local_match_code = local_match_ss.iloc[0, 1]
            local_pct_overlap = local_match_ss.iloc[0, 2]
            local_orig_id = get_src_orig_id('RI', local_matched_src_id)
            try:
                local_ease_year = get_source_attribute(ri_local_ease_years, local_orig_id)
            except Exception:
                print(f'Assigning match code -1 to RI Local feature {local_orig_id}')
                local_match_code = -1
            # Assign EaseYear
            if ((min_match_code <= state_match_code <= max_match_code or (state_match_code == 10 and state_pct_overlap >= min_pct_overlap)) 
                and state_ease_year is not None):
                if row[4] != 1 or (row[4] == 1 and row[1] == state_ease_year):
                    row[1] = state_ease_year
                    row[2] = state_src
                    row[3] = state_orig_id
                    c = c + 1
                elif row[4] == 1 and row[1] != state_ease_year:
                    row[5] = "EaseYear is final and there is conflict b/w current EaseYear and potential new EaseYear (updated YearProt)"
            elif ((min_match_code <= local_match_code <= max_match_code or (local_match_code == 10 and local_pct_overlap >= min_pct_overlap)) 
                and local_ease_year is not None):
                if row[4] != 1 or (row[4] == 1 and row[1] == local_ease_year):
                    row[1] = local_ease_year
                    row[2] = local_src
                    row[3] = local_orig_id
                    c = c + 1
                elif row[4] == 1 and row[1] != local_ease_year:
                    row[5] = "EaseYear is final and there is conflict b/w current EaseYear and potential new EaseYear (updated YearProt)"
            cur.updateRow(row)
    print(f'Updated EaseYear for {c} rows...')


#### PROTECTION DURATION #####
# Some states have ProtDuration as a direct attribute while others are based on GapStatus
# so we have two different functions that can be used depending on the circumstance

### Function to update ProtDuration ###
# MA and NH directly provide information about ProtDuration while all other states infer
# ProtDuration from GapStatus.
# This function uses only the ProtDuration fields in the source data and is most suitable 
# for a general update of ProtDuration. If you are updating GapStatus and want to update
# ProtDuration based on changes made to GapStatus, you should use the next function which
# updates ProtDuration based on GapStatus.
# NOTE: There are some rows in VT where TEMP duration was determined from checking source data
# comments - query LOWER(ProtTypeComments) LIKE '%temporary%' to find these and make sure they are correct
# Arguments:
#  - state (text): two-letter state abbreviation in capital letters (CT, MA, RI, VT, NH, ME)
#  - state_fc (text): name of the preprocessed, singlepart state data source
#                     it should be in same GDB as NEPOS so should only need name, not full path
#  - match_table (pandas df): dataframe resulting from reading in match table CSV with pd.read_csv()
#  - local_fc (text): name of RI Local dataset
#  - unknown_only (boolean): should duration be updated only for rows where ProtDuration is unknown?
#  - new_data_only (boolean): should ProtDuration be updated only for new rows?
#                             new rows are identified by lack of FinalID
#  - take_only_known (boolean): should we only update NEPOS if the source has a known value (not unknown)?
def update_prot_duration(state, state_fc, match_table, local_fc=None, 
                         unknown_only=True, new_data_only=True, take_only_known=True):
    if take_only_known == False:
        src_query = "State = '" + state + "'"
    elif take_only_known == True:
        src_query = "State = '" + state + "' AND ProtDuration <> 'UNK' AND ProtDuration IS NOT NULL"
    
    tnc_prot_duration = {key: value for (key, value) in arcpy.da.SearchCursor(tnc, ['UID', 'ProtDuration'], where_clause=src_query)}
    nced_prot_duration = {key: value for (key, value) in arcpy.da.SearchCursor(nced, ['UID', 'ProtDuration'], where_clause=src_query)}
    padus_prot_duration = {key: value for (key, value) in arcpy.da.SearchCursor(padus, ['UID', 'ProtDuration'], where_clause=src_query)}
    state_prot_duration = {key: value for (key, value) in arcpy.da.SearchCursor(state_fc, ['UID', 'ProtDuration'], where_clause=src_query)}
    if local_fc is not None:
        local_prot_duration = {key: value for (key, value) in arcpy.da.SearchCursor(local_fc, ['UID', 'ProtDuration'], where_clause=src_query)}
        local_items = get_local_info()
        local_id_col = local_items[0]
        local_code_col = local_items[1]
        local_pct_overlap_col = local_items[2]
        local_src = local_items[3]
    
    # Subset NEPOS for UpdateCursor based on function parameters
    if unknown_only == True and new_data_only == True:
        query = "State = '" + state + "' AND ProtDuration = 'UNK' AND FinalID IS NULL"
    elif unknown_only == True and new_data_only == False:
        query = "State = '" + state + "' AND ProtDuration = 'UNK'"
    elif unknown_only == False and new_data_only == True:
        query = "State = '" + state + "' AND FinalID IS NULL"
    elif unknown_only == False and new_data_only == False:
        query = "State = '" + state + "'"

    # Get state column name in match table
    state_items = get_state_info(state)
    state_id_col = state_items[0]
    state_code_col = state_items[1]
    state_pct_overlap_col = state_items[2]
    state_src = state_items[3]
    
    fields = ['FinalID2', 'PolySource', 'PolySource_FeatID', 
              'ProtDuration', 'Source_ProtDuration', 'Source_ProtDuration_FeatID']
    c = 0
    with arcpy.da.UpdateCursor(pos, fields, query) as cur:
        for row in cur:
            try:
                if 'Harvard Forest' in row[4]:
                    continue

                if local_fc is not None:
                    local_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [local_id_col, local_code_col, local_pct_overlap_col]].drop_duplicates()
                    local_matched_src_id = local_match_ss.iloc[0, 0]
                    local_match_code = local_match_ss.iloc[0, 1]
                    local_pct_overlap = local_match_ss.iloc[0, 2]
                    local_orig_id = get_src_orig_id(state, local_matched_src_id)
                    try:
                        local_duration = get_source_attribute(local_prot_duration, local_orig_id)
                    except Exception:
                        print(f'Setting match code to -1 for {state} Local feature {local_orig_id}')
                        local_match_code = -1
                # State match codes
                state_match_ss = match_table.loc[match_table['FinalID2'] == row[0], [state_id_col, state_code_col, state_pct_overlap_col]].drop_duplicates()
                state_matched_src_id = state_match_ss.iloc[0, 0]
                state_match_code = state_match_ss.iloc[0, 1]
                state_pct_overlap = state_match_ss.iloc[0, 2]
                state_orig_id = get_src_orig_id(state, state_matched_src_id)
                try:
                    state_duration = get_source_attribute(state_prot_duration, state_orig_id)
                except Exception:
                        print(f'Assigned match code -1 to {state} feature {state_orig_id}')
                        state_match_code = -1
                
                # TNC match codes
                tnc_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["tnc_id", "tnc_match_code", "tnc_pct_overlap"]].drop_duplicates()
                tnc_matched_src_id = tnc_match_ss.iloc[0, 0]
                tnc_match_code = tnc_match_ss.iloc[0, 1]
                tnc_pct_overlap = tnc_match_ss.iloc[0, 2]
                tnc_orig_id = get_src_orig_id('TNC', tnc_matched_src_id)
                try:
                    tnc_duration = get_source_attribute(tnc_prot_duration, tnc_orig_id)
                except Exception:
                    print(f'Assigned match code -1 to TNC feature {tnc_orig_id}')
                    tnc_match_code = -1
                
                # NCED match codes
                nced_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["nced_id", "nced_match_code", "nced_pct_overlap"]].drop_duplicates()
                nced_matched_src_id = nced_match_ss.iloc[0, 0]
                nced_match_code = nced_match_ss.iloc[0, 1]
                nced_pct_overlap = nced_match_ss.iloc[0, 2]
                nced_orig_id = get_src_orig_id('NCED', nced_matched_src_id)
                try:
                    nced_duration = get_source_attribute(nced_prot_duration, nced_orig_id)
                except Exception:
                    print(f'Assigned match code -1 to NCED feature {nced_orig_id}')
                    nced_match_code = -1
                
                # PADUS match codes
                padus_match_ss = match_table.loc[match_table['FinalID2'] == row[0], ["padus_id", "padus_match_code", "padus_pct_overlap"]].drop_duplicates()
                padus_matched_src_id = padus_match_ss.iloc[0, 0]
                padus_match_code = padus_match_ss.iloc[0, 1]
                padus_pct_overlap = padus_match_ss.iloc[0, 2]
                padus_orig_id = get_src_orig_id('PADUS', padus_matched_src_id)
                try:
                    padus_duration = get_source_attribute(padus_prot_duration, padus_matched_src_id)
                except Exception:
                    print(f'Assigned match code -1 to PADUS feature {padus_orig_id}')
                    padus_match_code = -1
                
                # Use the match codes and values to assign a new value if possible
                # Any -1 match codes will not execute in these conditionals
                if local_fc is not None:
                    if (min_match_code <= local_match_code <= max_match_code or (local_match_code == 10 and local_pct_overlap >= min_pct_overlap)):
                        row[3] = local_duration
                        row[4] = local_src
                        row[5] = local_orig_id
                        c = c + 1
                        cur.updateRow(row)
                        continue
                if (min_match_code <= state_match_code <= max_match_code or (state_match_code == 10 and state_pct_overlap >= min_pct_overlap)):
                    row[3] = state_duration
                    row[4] = state_src
                    row[5] = state_orig_id
                    c = c + 1
                elif (min_match_code <= tnc_match_code <= max_match_code or (tnc_match_code == 10 and tnc_pct_overlap >= min_pct_overlap)):
                    row[3] = tnc_duration
                    row[4] = tnc_src
                    row[5] = tnc_orig_id
                    c = c + 1
                # NOTE: For NCED we use additional criteria that PubAccess != No
                # It seems like NCED uses No far more than most sources and it might be a default
                # value. It often conflicts with all other sources and doesn't seem reliable.
                # Could change this in the future if that changes.
                elif (min_match_code <= nced_match_code <= max_match_code or (nced_match_code == 10 and nced_pct_overlap >= min_pct_overlap)):
                    row[3] = nced_duration
                    row[4] = nced_src
                    row[5] = nced_orig_id
                    c = c + 1
                elif (min_match_code <= padus_match_code <= max_match_code or (padus_match_code == 10 and padus_pct_overlap >= min_pct_overlap)):
                    row[3] = padus_duration
                    row[4] = padus_src
                    row[5] = padus_orig_id
                    c = c + 1
                cur.updateRow(row)
            except Exception:
                print(traceback.format_exc())
                continue   # Go to the next row
    print(f'Updated ProtDuration for {c} rows!')


### Function to update ProtDuration from GapStatus ###
# There are a few states (CT, RI, VT, ME) where protection duration is not directly available.
# In these cases, we infer ProtDuration from GapStatus.
# This function can be used to update ProtDuration from GapStatus - for example, say you updated
# GapStatus in RI using update_gap_status() because no official RI-government source contains GAP
# info (so you use the update_gap_status function to try to get GapStatus from TNC and other sources).
# You can then use this function to update ProtDuration based on the updated GapStatus.
# This function is NOT recommended for use in NH and MA where there is ProtDuration available
# directly from the state source.
# Arguments:
#  - state (text): two-letter state abbreviation in capital letters (CT, MA, RI, VT, NH, ME)
#  - new_data_only (boolean): should GapStatus be updated for new rows only?
#                             new rows identified by lack of FinalID
#  - include_temp (boolean): should rows where ProtDuration = TEMP be included?
#                            recommend this be kept as False since TEMP ProtDuration is usually
#                            determined by ProtType (e.g., lease) or other direct edit
#                            there also aren't many TEMP rows so these can be checked manually
def update_prot_duration_from_gap_status(state, new_data_only=True, include_temp=False):
    if new_data_only == True and include_temp == True:
        query = "State = '" + state + "' AND FinalID IS NULL"
    elif new_data_only == True and include_temp == False:
        query = "State = '" + "' AND FinalID IS NULL AND ProtDuration <> 'TEMP'"
    elif new_data_only == False and include_temp == False:
        query = "State = '" + "' AND ProtDuration <> 'TEMP'"
    elif new_data_only == False and include_temp == True:
        query = "State = '" + state + "'"
    fields = ['GapStatus', 'Source_GapStatus', 'Source_GapStatus_FeatID', 'ProtDuration', 'Source_ProtDuration', 'Source_ProtDuration_FeatID']
    with arcpy.da.UpdateCursor(pos, fields, query) as cur:
        for row in cur:
            try:
                if 0 < row[0] < 4 or row[0] == 39:
                    row[3] = 'PERM'
                else:
                    row[3] = 'UNK'
                row[4] = row[1]
                row[5] = row[2]
                cur.updateRow(row)
            except Exception:
                print(traceback.format_exc())
    print('Assigned ProtDuration')


####### CALL FUNCTIONS FOR EACH STATE #######
# CSVs of polygon ID matches
ct_match_table = pd.read_csv('D:/Lee/POS/Update_2023/Data/matching/nepos_ct_matches_20250306.csv')
ma_match_table = pd.read_csv("D:/Lee/POS/Update_2023/Data/matching/nepos_ma_matches_20250327.csv")

#### ME ####
pos = "POS_v2_24_sp"
me_conserved_lands = "Maine_Conserved_Lands_albers_sp"
me_match_table = pd.read_csv("D:/Lee/POS/Update_2023/Data/matching/nepos_me_matches_20250411.csv",
                             dtype={'PolySource_FeatID': 'string', 'megis_id': 'string'})
ma_match_table = pd.read_csv("D:/Lee/POS/Update_2023/Data/matching/nepos_ma_matches_2025-04-28.csv",
                             dtype={'FinalID2': 'string', 'PolySource': 'string', 'PolySource_FeatID': 'string',
                                    'massgis_id': 'string', 'tnc_id': 'string', 'nced_id': 'string', 'padus_id': 'string'})

pos = "POS_v2_25_sp"
nh_cpl = "NH_Conservation_Public_Lands_albers_sp"
nh_match_table = pd.read_csv("D:/Lee/POS/Update_2023/Data/matching/nepos_nh_matches_2025-05-05.csv",
                             dtype={'FinalID2': 'string', 'PolySource': 'string', 'PolySource_FeatID': 'string', 
                                    'nh_id': 'string', 'tnc_id': 'string', 'nced_id': 'string', 'padus_id': 'string'})

pos = "POS_v2_27_sp"
ri_state = "RI_State_albers_sp"
ri_local = "RI_Local_albers_sp"
ri_match_table = pd.read_csv("D:/Lee/POS/Update_2023/Data/matching/nepos_ri_matches_2025-05-09.csv",
                             dtype={"FinalID2": "string", "PolySource": "string", "PolySource_FeatID": "string",
                                    "ri_state_id" : "string", "ri_local_id": "string", "tnc_id": "string", "nced_id": "string", "padus_id": "string"})

pos = "POS_v2_28_sp"
vt_pld = "Cadastral_PROTECTEDLND_poly_albers_sp"
vt_match_table = pd.read_csv("D:/Lee/POS/Update_2023/Data/matching/nepos_vt_matches_2025-05-22.csv",
                             dtype={"FinalID2": "string", "PolySource": "string", "PolySource_FeatID": "string",
                                    "vt_id": "string", "tnc_id": "string", "nced_id": "string", "padus_id": "string"})

pos = "POS_v2_29_sp"
vt_match_table = pd.read_csv("D:/Lee/POS/Update_2023/Data/matching/nepos_vt_matches_2025-06-11.csv",
                             dtype={"FinalID2": "string", "PolySource": "string", "PolySource_FeatID": "string",
                                    "vt_id": "string", "tnc_id": "string", "nced_id": "string", "padus_id": "string"})

# March 2026 - observed some data issues with 2003 in CT
# Issue was related to TNC dates from 2003 - all CT rows w/ YearProt 2003 in TNC 2018 data have
# date precision "pre" which means it was protected before 2003 not during 2003. Unfortunately
# these got picked up by PADUS and there's now very little confidence in any CT data from 2003.
# So we are going to correct it and probably set most of these to 0 (unknown) unless there is
# another valid year from a different source (unlikely in CT)
pos = "D:\\Thompson_Lab_POS\\Data\\Old_GDBs_Data\\Update_2025_v2\\ct_2003_correction.gdb\\nepos_v2_0_sp_internal"
ct_match_table = pd.read_csv("D:\\Thompson_Lab_POS\\Data\\Old_GDBs_Data\\Update_2025_v2\\match_tables\\nepos_ct_matches_20250306.csv")
ct_deep = "D:\\Thompson_Lab_POS\\Data\\Old_GDBs_data\\Update_2025_v2\\source_and_aux_data.gdb\\CT_DEEP_Property_albers_sp_2025_01"

try:
    correct_ct_2003_rows(ct_deep, ct_match_table, take_only_known=False, new_data_only=False)
except Exception:
    print(traceback.format_exc())  # Print the error
    sys.exit()                     # Stop the script
finally:
    print_elapsed_time()
