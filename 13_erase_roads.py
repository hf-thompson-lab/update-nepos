##########################################################################################
# Erasing NHD water bodies from NEPOS
# This script uses state NHD files one by one to erase
# from NEPOS using the same query
#
# This code assumes a certain file structure, where there
# is a geodatabase containing NEPOS and a folder with the NHD
# geodatabases located at the same level. The workspace is set
# to the POS gdb and then goes up on level to find the folder of geodatabases.
#
# Prior to running this script, NHD was downloaded for the 6 states.
# Note that there is overlap in the state files but this does not matter for our purposes.
#
# Prior to running this script, TIGER primary and secondary roads data was downloaded
# for all 6 states from https://www.census.gov/cgi-bin/geo/shapefiles/index.php
# These shapefile are expected to be in one folder (roads_fol).
#
# NOTE: Ultimately I decided not to remove water from NEPOS, but I am leaving the code
# for reference.
#
# Lucy Lee, 7/2025
##########################################################################################

import time
start_time = time.time()
import arcpy
import os
import sys
import traceback

### IMPORTANT PATH VARIABLES ###
# GDB where multipart NEPOS lives (output of create_multipart_polygons.py)
nepos_gdb = "D:\\Lee\\POS\\Update_2023\\Data\\new_data2.gdb\\"
# Folder where the roads data live
roads_fol = "D:\\Lee\\POS\\Update_2023\\Data\\Roads\\"

# GDB where NEPOS lives
arcpy.env.workspace = nepos_gdb
arcpy.env.overwriteOutput = True

# NEPOS to erase features from (output of create_multipart_polygons.py)
pos = "POS_final"

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

#### FUNCTIONS RELATED TO WATER - NOT USED ####
# Subsets the NHD datasets to just the water areas of interest
# Saves the subsets as new FCs in the workspace GDB
def subset_nhd_waterbody(gdb):
    # Query for NHDWaterbody - only reservoirs and lakes/ponds
    waterbody_fc = f"{gdb}\\NHDWaterbody"
    waterbody_subset = f"{gdb}\\waterbody_subset"
    waterbody_query = "ftype = 436 OR ftype = 390"
    arcpy.conversion.ExportFeatures(waterbody_fc, waterbody_subset, waterbody_query)

    buffer_distance = "1 Meter"
    waterbody_subset_buffered = f"{gdb}\\waterbody_subset_buffer"
    arcpy.analysis.PairwiseBuffer(waterbody_subset, waterbody_subset_buffered, buffer_distance)
    print(f"Exported subset of {waterbody_fc} and buffered to {buffer_distance}...")
    return(waterbody_subset_buffered)

def subset_nhd_area(gdb):
    # Query for areas - stream/river only
    # There is overlap in NHD e.g., rapids are also included in stream/river
    areas_fc = f"{gdb}\\NHDArea"
    areas_subset = f"{gdb}\\areas_subset"
    areas_query = "ftype = 460"
    arcpy.conversion.ExportFeatures(areas_fc, areas_subset, areas_query)
    print(f"Exported subset to {areas_fc}...")
    return(areas_subset)

def prep_water():
    # Set workspace to the NHD folder which contains GDBs of 
    # each state's NHD data
    arcpy.env.workspace = "D:\\Lee\\POS\\Update_2023\\Data\\NHD\\"

    # Let GDBs in the workspace folder
    gdbs = arcpy.ListWorkspaces(workspace_type="FileGDB")

    # Get all the state NHD layer subsets
    all_nhd_data = []
    for gdb in gdbs:
        print(f"Current GDB: {os.path.basename(gdb)}")
        nhd_waterbody = subset_nhd_waterbody(gdb)
        all_nhd_data.append(nhd_waterbody)

        nhd_area = subset_nhd_area(gdb)
        all_nhd_data.append(nhd_area)
    
    # Change working directory to be location where NEPOS lives
    arcpy.env.workspace = "D:\\Lee\\POS\\Update_2023\\Data\\new_data2.gdb\\"

    # Merge all NHD subset files together
    # NOTE: There are overlaps in the data (i.e., a feature is in multiple state layers) 
    # but that doesn't matter for erasing features
    nhd_subset = "nhd_subset"
    arcpy.management.Merge(all_nhd_data, nhd_subset)
    print("Merged all NHD subsets together...")

    # Project to POS CRS
    nhd_subset_proj = "nhd_subset_albers"
    pos_crs = arcpy.Describe(pos).spatialReference
    arcpy.management.Project(nhd_subset, nhd_subset_proj, pos_crs)
    print("Projected to NEPOS CRS...")

    return(nhd_subset_proj)

#### FUNCTION TO PREP ROADS DATA ####
# Must have downloaded TIGER prim/sec roads before running this function
# and put all the roads shapefiles (1 per state) in roads_fol path.
# NOTE: This function calls the merged roads "prim_sec_roads_tiger_2024".
#       You can update this to whatever year you are using in line 141.
def prep_roads():
    # Set workspace to folder where roads shapefiles live
    arcpy.env.workspace = roads_fol

    # List shapefiles in the workspace folder
    road_shps = arcpy.ListFeatureClasses()

    # Function above only returns fc names -- join workspace path
    # to make full paths to the shapefiles
    road_shps = [os.path.join(roads_fol, x) for x in road_shps]

    # Change working directory to be location where NEPOS lives
    arcpy.env.workspace = nepos_gdb

    # Merge all roads shapefiles together
    # NOTE: There are overlaps in the data (i.e., a feature is in multiple state layers) 
    # but that doesn't matter for erasing features
    all_roads = "prim_sec_roads_tiger_2024"
    arcpy.management.Merge(road_shps, all_roads)
    print("Merged all roads shapefiles together...")

    # Project to POS CRS
    all_roads_proj = all_roads + "_albers"
    pos_crs = arcpy.Describe(pos).spatialReference
    arcpy.management.Project(all_roads, all_roads_proj, pos_crs)
    print("Projected roads to NEPOS CRS...")

    # Calculate buffer field - buffer distances based on visual inspection
    arcpy.management.AddField(all_roads_proj, "buffer", "TEXT", field_length=10)
    with arcpy.da.UpdateCursor(all_roads_proj, ["MTFCC", "buffer"]) as cur:
        for row in cur:
            if row[0] == "S1100":
                row[1] = "40 Feet"
            elif row[0] == "S1200":
                row[1] = "20 Feet"
            cur.updateRow(row)

    # Buffer roads by buffer field value
    all_roads_buffered = all_roads_proj + "_buffer"
    arcpy.analysis.Buffer(all_roads_proj, all_roads_buffered, "buffer")
    print("Buffered roads...")

    return(all_roads_buffered)


try:
    # Prep water bodies and roads from raw data
    #nhd = prep_water()
    roads = prep_roads()

    # Merge together - dp not run!
    #roads_and_water = "roads_water_combined"
    #arcpy.management.Merge([nhd, roads], roads_and_water)

    # Erase from NEPOS
    arcpy.analysis.PairwiseErase(pos, roads, f"{pos}_erase_roads")
    print("Erased buffered roads bodies from NEPOS")

except Exception:
    print(traceback.format_exc())
    sys.exit()
finally:
    print_elapsed_time()
