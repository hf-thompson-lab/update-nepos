# Adds new rows that are identified as Group 2a and/or 2b based on Landvest
# spatial matching and grouping tools.
#
# Usually after running the Landvest tools for spatial matching and grouping,
# it is a good idea to rename the FC2_GROUPED output with the state and source
# (e.g., CT_TNC_FC2_GROUPED). That way the file is not overwritten when you
# run the next source in case you want to look at it again later for some
# reason.
#
# This script contains functions that add source fields(e.g., Source_AreaName)
# to the source data so those fields will be complete when the data is appended
# to NEPOS. It then deletes fields in source data not found in NEPOS (such
# as UID and UID2), appends rows, calculates area, etc.
#
# There is functionality to exclude rows from being appended based on
# where their original source is (to avoid circularity) and percent
# overlap (group 2b only).
#
# NOTE: You will need to comment out lines that you don't need! Recommend
# going one source within one state at a time.
#
# ***TO DO:*** Add functionality to add a new field to NEPOS called new_data
# and have this script calculate that as 1 for new rows added with this script.
# This will help isolate newly added rows in the future. The field can then
# be deleted once the update process is complete.
#
# Lucy Lee, 12/2025

import time
start_time = time.time()
import arcpy
import traceback

arcpy.env.workspace = "D:/Lee/POS/Update_2023/Data/new_data2.gdb"
arcpy.env.overwriteOutput = True

####### General helper functions #######
# Function that uses global var start_time to calculate elapsed time
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

####### Shared functions specific to analysis ######
# These functions are called within the add_group2a_data and
# add_group2b data functions

# Add and populate source fields for core attributes
def add_source_fields(new_data):
        arcpy.management.AddField(new_data, "PolySource", "TEXT", field_length = 50)
        arcpy.management.AddField(new_data, "PolySource_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField(new_data, "Source_AreaName", "TEXT", field_length = 50)
        arcpy.management.AddField(new_data, "Source_AreaName_FeatID", "TEXT", field_length = 50)
        arcpy.management.AddField(new_data, "Source_FeeOwner", "TEXT", field_length = 50)
        arcpy.management.AddField(new_data, "Source_FeeOwner_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField(new_data, "Source_ProtType", "TEXT", field_length = 50)
        arcpy.management.AddField(new_data, "Source_ProtType_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField(new_data, "Source_IntHolder1", "TEXT", field_length = 50)
        arcpy.management.AddField(new_data, "Source_IntHolder1_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField(new_data, "Source_IntHolder2", "TEXT", field_length = 50)
        arcpy.management.AddField(new_data, "Source_IntHolder2_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField(new_data, "Source_YearProt", "TEXT", field_length = 50)
        arcpy.management.AddField(new_data, "Source_YearProt_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField(new_data, "Source_FeeYear", "TEXT", field_length = 50)
        arcpy.management.AddField(new_data, "Source_FeeYear_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField(new_data, "Source_EaseYear", "TEXT", field_length = 50)
        arcpy.management.AddField(new_data, "Source_EaseYear_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField(new_data, "Source_GapStatus", "TEXT", field_length = 50)
        arcpy.management.AddField(new_data, "Source_GapStatus_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField(new_data, "Source_PubAccess", "TEXT", field_length = 50)
        arcpy.management.AddField(new_data, "Source_PubAccess_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField(new_data, "Source_ProtDuration", "TEXT", field_length = 50)
        arcpy.management.AddField(new_data, "Source_ProtDuration_FeatID", "TEXT", field_length = 25)
        print("Added source fields to group_2a...")

# Poulate source fields
def populate_source_fields(new_data, source_name):
        # Syntax with CalculateField is a PITA so going to use UpdateCursor instead
        fields = ["PolySource", "PolySource_FeatID", "Source_AreaName", "Source_AreaName_FeatID",
                  "Source_FeeOwner", "Source_FeeOwner_FeatID", "Source_ProtType", "Source_ProtType_FeatID",
                  "Source_YearProt", "Source_YearProt_FeatID", "Source_GapStatus", "Source_GapStatus_FeatID",
                  "Source_PubAccess", "Source_PubAccess_FeatID", "Source_ProtDuration", "Source_ProtDuration_FeatID",
                  "UID"]
        with arcpy.da.UpdateCursor(new_data, fields) as cur:
            for row in cur:
                row[0] = source_name   # PolySource
                row[1] = row[16]
                row[2] = source_name   # AreaName
                row[3] = row[16]
                row[4] = source_name   # FeeOwner
                row[5] = row[16]
                row[6] = source_name   # ProtType
                row[7] = row[16]
                row[8] = source_name   # YearProt
                row[9] = row[16]
                row[10] = source_name  # GapStatus
                row[11] = row[16]
                row[12] = source_name  # PubAccess
                row[13] = row[16]
                row[14] = source_name  # ProtDuration
                row[15] = row[16]
                cur.updateRow(row)
        print("Calculated most source fields...")

        # Use an UpdateCursor for fields that may or may not have a value
        fields = ["IntHolder1", "Source_IntHolder1", "Source_IntHolder1_FeatID",
                  "IntHolder2", "Source_IntHolder2", "Source_IntHolder2_FeatID",
                  "FeeYear", "Source_FeeYear", "Source_FeeYear_FeatID",
                  "EaseYear", "Source_EaseYear", "Source_EaseYear_FeatID", 
                  "UID"]
        with arcpy.da.UpdateCursor(new_data, fields) as cur:
            for row in cur:
                if row[0] is not None and row[0] != '' and row[0] != ' ':
                    row[1] = source_name
                    row[2] = row[12]
                if row[3] is not None and row[3] != '' and row[3] != ' ':
                    row[4] = source_name
                    row[5] = row[12]
                if row[6] is not None:
                    row[7] = source_name
                    row[8] = row[12]
                if row[9] is not None:
                    row[10] = source_name
                    row[11] = row[12]
                cur.updateRow(row)
        print("Calculated remaining source fields...")

# Delete fields not in NEPOS from the new data
# This shouldn't be necessary using NO_TEST schema type option in Append tool
# but it was still throwing a mysterious error saying "fields could not be found"
# Removing fields not in NEPOS from the source seems to solve this problem
def delete_fields(pos, new_data):
        nepos_field_names = [f.name for f in arcpy.ListFields(pos)]
        src_fields = arcpy.ListFields(new_data)

        # Dropping ORIG_FID because these data did not have a ORIG_FID in NEPOS
        # we can tell if new rows are from the same polygon by the PolySource_FeatID field
        drop_fields = ["ORIG_FID"]
        for s in src_fields:
            if s.name not in nepos_field_names:
                drop_fields.append(s.name)
                print(f"Added {s.name} to drop_fields")
            else:
                continue

        arcpy.management.DeleteField(new_data, drop_fields)
        print("Deleted source fields not in NEPOS...")

# Append the new rows!
def append_rows(pos, new_data):
        # Still using NO_TEST because there may be fields in NEPOS not in the source data
        # such as FinalID2. However, there should be no fields in the source data that are
        # not in NEPOS.
        arcpy.management.Append(new_data, pos, "NO_TEST")
        print("Appended new rows to NEPOS...")

def calculate_area(pos):
     arcpy.management.CalculateGeometryAttributes(pos, [["Area_Ac", "AREA"]], area_unit = "ACRES_US")
     arcpy.management.CalculateGeometryAttributes(pos, [["Area_Ha", "AREA"]], area_unit = "HECTARES")
     print("Calculated area...")

##### Function to add group 2a rows #####
def subset_group2a(fc2_grouped, excluded_sources):
        # Subset the data in a new FC - just making a feature layer
        # doesn't work, it has to be a new file on disk
        group_2a = "group_2a"
        arcpy.conversion.ExportFeatures(fc2_grouped, group_2a, "GROUP_2a = 'Y'")
        print(f"Subset {fc2_grouped} to group 2a polygons...")

        # Use UpdateCursor to delete rows with a source in the exclusions list
        # This is the easiest way to do it because the syntax is easiest compared to queries
        if len(excluded_sources) > 0:
            c = 0
            with arcpy.da.UpdateCursor(group_2a, "Source") as cur:
                for row in cur:
                    if row[0] in excluded_sources:
                        cur.deleteRow()
                        c = c + 1
            print(f"Deleted {c} group 2a rows...")

        return(group_2a)

####### Function to add group 2b rows #######
# Only difference is can specificy the maximum overlap threshold for 2b. If left None, all
# Group 2b data will be used (except from excluded sources). If max_overlap is set to a number,
# it should be set the maximum overlap that you WANT to allow - percent overlaps greater
# than this value will be deleted (pct overlap field is PctOvFC2HS_POS).
# Arguments:
#  - pos (text): name of the current NEPOS layer
#  - fc2_grouped (text): name of FC2_GROUPED layer for the source/state
#  - source_name (text): name of source to populate in PolySource, etc.
#                        should follow same structure as names in replace_geometry.py
#  - excluded_sources (list): a list of strings identifying the rows to NOT append
#                             based on their original source. Most source data have a 'source'
#                             field that describes where they got the data, and we want to check
#                             this each time to make sure we not participating in data circularity
#  - max_overlap (float): for group 2b function only - what is the maximum amount of overlap you
#                         want to accept for group 2b polygons? This is best identified should visual
#                         inspection of the data as it varies a lot - sometimes you don't want to take
#                         any group 2b polygons at all!
def subset_group2b(fc2_grouped, excluded_sources, max_overlap = None):
        # Subset the data in a new FC - just making a feature layer
        # doesn't work, it has to be a new file on disk
        group_2b = "group_2b"
        arcpy.conversion.ExportFeatures(fc2_grouped, group_2b, "GROUP_2b = 'Y'")
        print(f"Subset {fc2_grouped} to group 2b polygons...")

        # Use UpdateCursor to delete rows with a source in the exclusions list
        # This is the easiest way to do it because the syntax is easiest compared to queries
        if len(excluded_sources) > 0:
            c = 0
            with arcpy.da.UpdateCursor(group_2b, "Source") as cur:
                for row in cur:
                    if row[0] in excluded_sources:
                        cur.deleteRow()
                        c = c + 1
            print(f"Deleted {c} group 2b rows based on Source...")

        if max_overlap is not None:
            c = 0
            with arcpy.da.UpdateCursor(group_2b, "PctOvFC2NEPOS") as cur:
                for row in cur:
                    if row[0] > max_overlap:
                        cur.deleteRow()
                        c = c + 1
            print(f"Deleted {c} group 2b rows based on percent overlap...")

        return(group_2b)

def add_group_2a_data(pos, fc2_grouped, source_name, excluded_sources):
    try:
        print("Beginning group 2a...")
        new_group_2a = subset_group2a(fc2_grouped, excluded_sources)
        add_source_fields(new_group_2a)
        populate_source_fields(new_group_2a, source_name)
        delete_fields(pos, new_group_2a)
        append_rows(pos, new_group_2a)
        calculate_area(pos)
    except Exception:
        print(traceback.format_exc())
    else:
        arcpy.management.Delete(new_group_2a)
        print("Deleted interim files...")
    finally:
        print_elapsed_time()


# Function for adding new data from group 2b
def add_group_2b_data(pos, fc2_grouped, source_name, excluded_sources, max_overlap = None):
    try:
          print("Beginning group 2b...")
          new_group_2b = subset_group2b(fc2_grouped, excluded_sources, max_overlap)
          add_source_fields(new_group_2b)
          populate_source_fields(new_group_2b, source_name)
          delete_fields(pos, new_group_2b)
          append_rows(pos, new_group_2b)
          calculate_area(pos)
    except Exception:
          print(traceback.format_exc())
    else:
          arcpy.management.Delete(new_group_2b)
          print("Deleted interim files...")
    finally:
          print_elapsed_time()


###### NOTES ON ADDING DATA BELOW #####
# The lines of code below call the functions above.
# You will notice that for each state, the nepos variable is redefined
# to be the current version. It's a good idea to make copies frequently and at
# minimum between states so if you mess up, you at least don't have to redo
# a state you finished already!
#
# The next variable is the FC2_GROUPED file for that state & source. I ran
# the Landvest spatial matching for a specific states (I used definition queries
# to only show 1 state's data at a time in both NEPOS and the source). 
# Then you can rename the FC2_GROUPED
# output to the state and source (e.g., CT_TNC_FC2_GROUPED).
# FC2_GROUPED is the file that has group 2a and 2b polygons (new data).
#
# Also, each state/source has a list of exclusions which are based on the 'source'
# column in source data, if present. These can be reused each update and it's a good
# idea to check the source column of source data each update in case there are 
# any new values. The purpose of this is to avoid circularity in datasets 
# (e.g., polygons from MassGIS that are in TNC that are no longer in MassGIS,
# or polygons from PADUS that are in NCED but are no longer in PADUS). If in a future
# update, you find that there are values in the "exclusions" lists that are no longer
# present in the source data, you don't need to delete them from the list. It won't
# cause an error or anything. Personally I wouldn't delete things from the exclusions
# lists until/unless they became so large that they were difficult to work with.
#
# You will need to comment out lines that you aren't running!
# Also, between each source, you need to rerun the Landvest matching tools
# because now we've added new data to NEPOS.

######################## ADD DATA TO CT ######################
#### NCED DATA #####
nepos = "POS_v2_20_sp"
fc2_nced_ct = "CT_NCED_FC2_GROUPED"  # Contains Group 2a, 2b, and 6 polygons
# Sources from CT_NCED_FC2_GROUPED to exclude to avoid circularity
# Also not adding TPL Cons Almanac data because it doesn't seem good quality
ct_nced_exclusions = ["FWS_PADUS2_0Easement_FWSInterest_Simplified", "FWS_PADUS2_1Easement_FWSInterest_Simplified",
                   "GAP_PADUS1_4Easements_FWS_FWSInterest_Simplified_preprocess",
                   "GAP_PADUS1_4Easements_NPS_Tracts", "Harvard Forest POS Database",
                   "NPS_PADUS2_1Easement_NPS_Tracts", "The Nature Conservancy", "The Nature Conservancy Eastern Regional Office",
                   "TPL_PADUS2_1Easement_ParkServe_Parks", "USFS_ALP_PADUS2_0Easement_S_USA.PADUS_Easement2017",
                   "USGS_PADUS2_1Easements_NRCS_Easements", "The Trust for Public Land - Conservation Almanac Database"]
#add_group_2a_data(nepos, fc2_nced_ct, "NCED 7/2024", ct_nced_exclusions)
#add_group_2b_data(nepos, fc2_nced_ct, "NCED 7/2024", ct_nced_exclusions, None)   # Add all 2b rows except excluded sources


#### CLCC (BH) DATA ####
fc2_bh_ct = "CT_BH_FC2_GROUPED"
ct_bh_exclusions = ["", "CBIVERSIONPADUS V2P1", "CT DEEP AUG 2011", "CTPOS2011", "HARVARD FOREST / HIGHSTEAD GIS LAYER",
                    "NCED_20161005", "NCED_20180111", "PADUS_V4P1", "TNC SA2012", "TNC SA2014", "TNC SA2015", "TPL NCED 6/2014"]
max_pct_overlap = 7.0   # Based on visual inspection of data
#add_group_2a_data(nepos, fc2_bh_ct, "CLCC / Last Green Valley 2021", ct_bh_exclusions)
#add_group_2b_data(nepos, fc2_bh_ct, "CLCC / Last Green Valley 2021", ct_bh_exclusions, max_pct_overlap)


#### TNC DATA ###
fc2_tnc_ct = "CT_TNC_FC2_GROUPED"
ct_tnc_exclusions = ["Harvard Forest 2022"]   # This source refers to the BH / CLCC data incorporated above
max_pct_overlap = 4.2   # Based on visual inspection of Group 2b data
#add_group_2a_data(nepos, fc2_tnc_ct, "TNC SA2022", ct_tnc_exclusions)
#add_group_2b_data(nepos, fc2_tnc_ct, "TNC SA2022", ct_tnc_exclusions, max_pct_overlap)


#### CT DEEP DATA ####
fc2_deep_ct = "CT_DEEP_FC2_GROUPED"
ct_deep_exclusions = []
max_pct_overlap = 7.0  # Based on visual inspection
#add_group_2a_data(nepos, fc2_deep_ct, "CT DEEP 1/2025", ct_deep_exclusions)
#add_group_2b_data(nepos, fc2_deep_ct, "CT DEEP 1/2025", ct_deep_exclusions, max_pct_overlap)


##### CT PADUS DATA ####
fc2_padus_ct = "CT_PADUS_FC2_GROUPED"
ct_padus_exclusions = ["NCED_PADUS4_0Easement_NCED_for_PADUS_07282023", "NE_Secured_Areas_2022_TNC_CRCStoPADUS.gdb",
                       "TNC_PADUS2_0_SA2015_Public_gdb", "TNC_PADUS2_0_TNC_Lands 2017-05-14", 
                       "TNC_PADUS4_0Easement_TNC_Lands_PADUS_Layer", "TNC_PADUS4_0Fee_TNC_Lands_PADUS_Layer"]
max_pct_overlap = 5.0
#add_group_2a_data(nepos, fc2_padus_ct, "USGS PAD-US v4.0", ct_padus_exclusions)
nepos = "POS_v2_21_sp"   # Forgot to do PADUS 2b, ugh!
#add_group_2b_data(nepos, fc2_padus_ct, "USGS PAD-US v4.0", ct_padus_exclusions, max_pct_overlap)


################### ADD DATA TO MA #######################
# MassGIS
nepos = "POS_v2_22_sp"
fc2_massgis = "MA_MASSGIS_FC2_GROUPED"
massgis_exclusions = []
#add_group_2a_data(nepos, fc2_massgis, "MassGIS 1/2025", massgis_exclusions)
# All 2b rows are substantially new areas - frustratingly, a lot of these are adjacent
# to older MassGIS polygons that had their boundaries moved slightly to avoid overlap with the new polygons
#add_group_2b_data(nepos, fc2_massgis, "MassGIS 1/2025", massgis_exclusions, None)

# TNC
fc2_tnc_ma = "MA_TNC_FC2_GROUPED"
ma_tnc_exclusions = ["MassGIS Open Space v. 4/2022", 
                     "MassGIS Open Space v. 4/2022. Gap status assigned based on DCRLandscapDesig_March2012",
                     "TNC Secured Lands 2018, records are missing in MGIS open space 2022",
                     "Harvard Forest Wildlands v. 8/2022"]
#add_group_2a_data(nepos, fc2_tnc_ma, "TNC SA2022", ma_tnc_exclusions)
# One 2b polygon was added manually using the Append tool

# PADUS
fc2_padus_ma = "MA_PADUS_FC2_GROUPED"
ma_padus_exclusions = ["FWS_PADUS3_0Fee_FWSInterest_Simplified", "NCED_PADUS4_0Easement_NCED_for_PADUS_07282023",
                       "NE_Secured_Areas_2022_TNC_CRCStoPADUS.gdb", "NYNHP_PADUS4_0Fee_ NYPAD_2_Fee_NY",
                       "TNC_PADUS2_0_SA2015_Public_gdb", "TNC_PADUS4_0Easement_TNC_Lands_PADUS_Layer",
                       "TNC_PADUS4_0Fee_TNC_Lands_PADUS_Layer", "TPL_PADUS2_1_PADUS_DataDelivery_gdb",
                       "TPL_PADUS4_0Fee_ParkServe_DataShare_05152023"]
#add_group_2a_data(nepos, fc2_padus_ma, "USGS PAD-US v4.0", ma_padus_exclusions)
# 8 2b polygons were added manually using the Append tool

# Not adding any rows from NCED! It's not clear when the data
# in NCED is from, and a lot of it seems like if it were accurate MassGIS would have it.


########### ADD DATA TO ME #########
# MEGIS
nepos = "POS_v2_23_sp"
fc2_megis = "ME_MEGIS_FC2_GROUPED"
megis_exclusions = []
#add_group_2a_data(nepos, fc2_megis, 'MEGIS 3/2025', megis_exclusions)
#add_group_2b_data(nepos, fc2_megis, 'MEGIS 3/2025', megis_exclusions, None)

# TNC
fc2_tnc_me = "ME_TNC_FC2_GROUPED"
# Notes: Some polygons seem okay from: MHCT 2011, MEFO survey traverse, ' ' (empty),
# NEFF - 2022 SHAPEFILE - NHD24K Lakes/Ponds GE 10 ACRES removed, St John River removed and easement overwritten in section of St John with higher level protection (TNC funded),
# Those sources should be checked manually and use the AddRows tool to add them
# WHEW! This was a lot!!!
me_tnc_exclusions = ['City of Saco parcel data - TNC digitized', 'estimated from tax map',
                     'Fryeburg Parcel Maps regi', 'Bowdoinham Digital Parcel', 'BPL shapefile',
                     'LURC Digital Parcel Data', 'LURC Shapefile', 'Maine BPL 2013',
                     'Maine Office of GIS February 2011', 'MBPL shapefile from survey',
                     'MCHT 2011', 'MEFO 10-03', 'MEFO 2007 Debs Boundary Updates', 
                     'MEFO 2011 survey traverse', 'MEFO 2014 survey traverse w/coordinates from surveyor',
                     'MEFO 2015 survey traverse', 'MEFO 2015, 2020 survey traverse',
                     'MEFO 2020 edited to traverse of City buffer survey',
                     'MEFO LandVest Data and adj Survey 2015', 'MEFO survey traverse',
                     'MEFO survey traverse w/coordinates', 'MEFO survey traverse w/coordinates from surveyor',
                     'MEFO Surveys - surrounding', 'MEFO2016 survey traverse w/coordinates from surveyor',
                     'MEGIS (Sewall) 2016', 'MEGIS 2/24/2014', 'MEGIS 2015', 'MEGIS 2016', 'MEGIS 2018',
                     'MEGIS 4/28/2020 and City of Augusta Tax Maps Oct 2018 & commitment', 'megis and survey',
                     'MEGIS data 5/19/2022', 'MEGIS June 3 2021', 'megis parcel data', 'MNAP2013',
                     'NEFF', 'NEFF - 2022 SHAPEFILE - NHD24K Lakes/Ponds GE 10 ACRES removed, St John River removed and easement overwritten in section of St John with higher level protection (TNC funded)',
                     'parcel maps registered to', 'Sferra & adj to other cons lands, road, hydro',
                     'survey', 'Survey - adjusted to fit', 'Survey and ME Color 1F DOQs', 
                     'Survey fit to 24k', 'Survey registered to doq', 'survey traverse',
                     'surveyor shapefile/survey traverse', 'SWT - NEWT Sept 2015', 'topsham digital parcels - 2001',
                     'Town of Falmouth Parcels', 'U.S. Fish & Wildlife Service, 24K or Better', 'unknown',
                     'USFWS and Hatch Survey - 1985', 'York Digital Parcel Data', ' ']
# Based on visual inspection of data, we are only incorporating 2a polygons from TNC
# After adding 2a polys below, will go back to some sources and manually add data
#add_group_2a_data(nepos, fc2_tnc_me, 'TNC SA2022', me_tnc_exclusions)

# PADUS
fc2_padus_me = "ME_PADUS_FC2_GROUPED"
me_padus_exclusions = ['FWS_PADUS3_0Fee_FWSInterest_Simplified', 'MDACF_PADUS4_0_ME2022_Maine_Conserved_Lands_ME',
                       'NCED_PADUS4_0Easement_NCED_for_PADUS_07282023', 'NE_Secured_Areas_2022_TNC_CRCStoPADUS.gdb',
                       'TNC_PADUS2_0_SA2015_Public_gdb', 'TNC_PADUS2_0_TNC_Lands 2017-05-14', 'TNC_PADUS4_0Fee_TNC_Lands_PADUS_Layer',
                       'TPL_PADUS1_3_Conservation_Almanac_Database_US_Nov2011_gdb', 'USFS_ALP_PADUS3_0Fee_S_USA.PADUS_Fee2021',
                       'USFS_ALP_PADUS4_0Fee_S_USA.PADUS_Fee2023']
# Based on visual inspection, not going to add 2b rows automatically, but will look through that group and manually add
# Also, will need to check 2a rows for any data that should be deleted (particularly TPL data that includes paved areas)
#add_group_2a_data(nepos, fc2_padus_me, 'USGS PAD-US v4.0', me_padus_exclusions)


######## ADD DATA TO NH #######
nepos = "POS_v2_25_sp"
fc2_nhcpl = "NH_NHCPL_FC2_GROUPED"
nh_nhcpl_exclusions = []
max_pct_overlap = 2.0
#add_group_2a_data(nepos, fc2_nhcpl, "NH Conservation Public Lands 3/2025", nh_nhcpl_exclusions)
#add_group_2b_data(nepos, fc2_nhcpl, "NH Conservation Public Lands 3/2025", nh_nhcpl_exclusions, max_pct_overlap)

fc2_tnc_nh = "NH_TNC_FC2_GROUPED"
# NOTE: Some from these sources look okay and will be added manually: ' ', US FWS
nh_tnc_exclusions = [' ', 'http://www.granit.unh.edu/', 'US FWS', 'NH GRANIT', 'NH DRED shapefile']
#add_group_2a_data(nepos, fc2_tnc_nh, "TNC SA2022", nh_tnc_exclusions)
#add_group_2b_data(nepos, fc2_tnc_nh, "TNC SA2022", nh_tnc_exclusions, None)

fc2_padus_nh = "NH_PADUS_FC2_GROUPED"
# NOTE: Some from these sources look okay and will be added manually: USFS_ALP_PADUS4_0Fee_S_USA.PADUS_Fee2023
# After removing all polygons from these srouces, there are only 9 2b rows and they all look okay
nh_padus_exclusions = ["FWS_PADUS3_0Fee_FWSInterest_Simplified", "MDACF_PADUS4_0_ME2022_Maine_Conserved_Lands_ME",
                      "NCED_PADUS4_0Easement_NCED_for_PADUS_07282023", "NE_Secured_Areas_2022_TNC_CRCStoPADUS.gdb",
                      "TNC_PADUS2_0_SA2015_Public_gdb", "TNC_PADUS2_0_TNC_Lands 2017-05-14",
                      "TNC_PADUS4_0Fee_TNC_Lands_PADUS_Layer", "TPL_PADUS2_0_Almanac_PADUS_Submission_New_09142017_shp",
                      "USFS_ALP_PADUS3_0Fee_S_USA.PADUS_Fee2021", "USFS_ALP_PADUS4_0Fee_S_USA.PADUS_Fee2023",
                      "USGS_PADUS1_4Fee_USACE_174_fee"]
#add_group_2a_data(nepos, fc2_padus_nh, "USGS PAD-US v4.0", nh_padus_exclusions)
#add_group_2b_data(nepos, fc2_padus_nh, "USGS PAD-US v4.0", nh_padus_exclusions, None)


####### ADD DATA TO RI #######
nepos = "POS_v2_26_sp"
fc2_ri_local = "RI_LOCAL_FC2_GROUPED"
ri_local_exclusions = []
#add_group_2a_data(nepos, fc2_ri_local, "RI Local Conservation Areas 4/2025", ri_local_exclusions)
#add_group_2b_data(nepos, fc2_ri_local, "RI Local Conservation Areas 4/2025", ri_local_exclusions, None)   # Only 12 rows and all look good

fc2_ri_state = "RI_STATE_FC2_GROUPED"
ri_state_exclusions = []
#add_group_2a_data(nepos, fc2_ri_state, "RI State Conservation Areas 2/2025", ri_state_exclusions)
#add_group_2b_data(nepos, fc2_ri_state, "RI State Conservation Areas 2/2025", ri_state_exclusions, None)   # Only 7 rows and all look good

# Not adding TNC en masse -- instead, added only TNC interests from group 2a and 2b manually in ArcPro
# Same for PADUS -- only added some 2a/2b rows from USGS, FWS, and TPL manually
# After adding all the above data there were no NCED 2a/2b rows


####### ADD DATA TO VT ######
nepos = "POS_v2_28_sp"
fc2_vt = "VT_PLD_FC2_GROUPED"
vt_exclusions = []
#add_group_2a_data(nepos, fc2_vt, "VT Protected Lands Database 6/2021", vt_exclusions)
# Because of how complicated GMNF area is, we're doing Group 2a from VT and TNC first,
# then going back to check Group 2b

fc2_tnc_vt = "VT_TNC_FC2_GROUPED"
# Some rows from these sources added manually: Vermont Agency of Natural Resources, Vermont Land Trust (all three variations)
vt_tnc_exclusions = ['Protected Areas Database US Version 2.1', 'Vermont Agency of Natural Resources',
                     'Vermont Land Trust', 'Vermont Land Trust Conserved Land', 'Vermont Land Trust Conserved Lands']
# add_group_2a_data(nepos, fc2_tnc_vt, "TNC SA2022", vt_tnc_exclusions)

fc2_padus_vt = "VT_PADUS_FC2_GROUPED"
vt_padus_exclusions = ['FWS_PADUS3_0Fee_FWSInterest_Simplified', 'NCED_PADUS4_0Easement_NCED_for_PADUS_07282023', 'NE_Secured_Areas_2022_TNC_CRCStoPADUS.gdb',
                       'NYNHP_PADUS4_0Fee_ NYPAD_2_Fee_NY', 'TNC_PADUS2_0_SA2015_Public_gdb', 'TNC_PADUS2_0_TNC_Lands 2017-05-14',
                       'TNC_PADUS4_0Fee_TNC_Lands_PADUS_Layer', 'TPL_PADUS2_0_Almanac_PADUS_Submission_New_09142017_shp', 'USGS_PADUS1_4Fee_USACE_174_fee']
#add_group_2a_data(nepos, fc2_padus_vt, "USGS PAD-US v4.0", vt_padus_exclusions)

# After adding all these sources there was no good NCED data left to add

# CLI was added in much later to POS_v2_29_sp
# 2a and 2b rows were added manually
