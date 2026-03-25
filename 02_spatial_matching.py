#############################################################################################################
# Script to do the joins and spatial intersections that are used to define spatial relationships
# between NEPOS and source data. The functions in this script are also used for spatial matching with
# the deleted polygons layer, to ensure that previously deleted polygons don't get readded to NEPOS.
#
# For each state, this code gets run TWICE: first time is for geometry updates, and then again after the
# geometry updates are completed and before attributes are updated.
#
# Prior to running this code, data should be projected, restructured/recoded, and made into
# singlepart features.
#
# Also prior to running this script, you must make a folder where the CSVs and DBFs will be saved.
# It is easiest to make this in the same parent folder as the working GDB.
#
# Overall framework: Use polygon and point files of the singlepart datasets to do a series of spatial
# joins for each source (4 joins total - 1:1 and 1:many with NEPOS and source data serving as target dataset).
# And use the TabulateIntersection tool on the polygon layers to get % overlap on a polygon-basis.
# The output tables from these tools are then read into R and used to create the match table for the state.
#
# Lucy Lee, 12/2025
##############################################################################################################

#### LIBRARIES AND SETTINGS ####
import time
start_time = time.time()
import arcpy
import traceback
import os

# Parent folder containing the relevant folders/GDBs to read/write in
# Some outputs of this script will go to a GDB and others to a folder,
# so make sure the workspace is a path from which you can access both
arcpy.env.workspace = "D:/Thompson_Lab_POS/Data/Old_GDBs_Data/Update_2025_v2/ct_2003_correction"
arcpy.env.overwriteOutput = True   # Because I be messing up FREQUENTLY

#### GLOBAL VARIABLES ####
# The name of the folder and the GDB where outputs will be saved
# Ideally, these are just one level below the workspace folder - if
# they are more than one level, you need to add the intermediate
# folder(s) here as part of the path. You can also make these 
# complete paths if you wish or if you run into errors (sometimes
# it seems like ArcPy doesn't recognize the workspace).
# These variables are used in calc_pct_overlap() and conduct_joins()
# to put the tool outputs in the correct places.
# UPDATE AS NEEDED FOR FUTURE UPDATES!
output_folder = "tables"
output_gdb = "ct_2003_correction.gdb"

# Function that uses global var start_time to calculate elapsed time
# This function is run regardless of whether a previous part of the code throws an error or not
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


# Function to run the Tabulate Intersection tool on the single part polygon features
# The resulting table includes percent overlap information at the feature level
# and a copy of that table is saved as a CSV for use in R
# Arguments:
#  - pos_fc (text): path to current singlepart NEPOS
#  - source (text): path to singlepart source data
# Both input datasets should be in the same GDB
def calc_pct_overlap(pos_fc, source):
    # Run tabulate intersection
    out_tbl = f'{output_gdb}/tab_intersect_{os.path.basename(pos_fc)}_{os.path.basename(source)}'
    arcpy.analysis.TabulateIntersection(pos_fc, ['FinalID2'], source, out_tbl, class_fields=['UID2'])
    print(f'Tabulated intersection of {os.path.basename(pos_fc)} and {os.path.basename(source)}...')

    # Make CSV copy for use in R
    out_csv = f'{output_folder}/{os.path.basename(out_tbl)}.csv'
    arcpy.conversion.ExportTable(out_tbl, out_csv)
    print('And saved a copy of the result as a CSV')


# Function to make points from a polygon layer for use in spatial joins
# Arguments:
#  - data (text): path to the singlepart polygon data to turn into points
def make_points(data):
    out_fc = f'{data}_pt'
    # It is VERY IMPORTANT that the third parameter be set to 'INSIDE'
    # This ensures that the points actually fall within the polygon they represent
    arcpy.management.FeatureToPoint(data, out_fc, 'INSIDE')
    print('Created points of {}'.format(os.path.basename(data)))
    return out_fc


# Function to conduct the four spatial joins and export copies of the
# results to DBF format for use in R.
# The point layer of the one dataset is joined to the polygon layer of the other
# using the CONTAINS match option (this is why it's so important that the points
# be INSIDE the polygon they come from). Each type of join (1:1 and 1:M) is done twice
# once with NEPOS as the target dataset and once with the source as the target dataset.
# For these joins, we set KEEP_COMMON so that only the rows that have a join
# are retained in the output tables.
# The join results are also overwritten each time if the source name is the same - 
# this hasn't been an issue and has been helpful because it reduces clutter but
# if you wish to save a particular iteration (for example of TNC which gets repeated
# for multiple states) you can rename the files in ArcPro/Windows Explorer.
# Arguments:
#  - pos_poly (text): path to the singlepart NEPOS polygon layer
#  - pos_pt (text): path to the singlepart NEPOS point layer
#                   Can also be the object returned by make_points()
#  - source_poly (text): path to the singlepart source polygon layer
#  - source_pt (text): path to the singlepart source point layer
#                      Can also be the object retuned by make_points()
def conduct_joins(pos_poly, pos_pt, source_poly, source_pt):
    print(f'Beginning 1:1 spatial joins of POS and {os.path.basename(source_poly)}...')
    out_fc1 = f'{output_gdb}/POS_join_{os.path.basename(source_pt)}_1to1'
    arcpy.analysis.SpatialJoin(pos_poly, source_pt, out_fc1, 'JOIN_ONE_TO_ONE', 'KEEP_COMMON', match_option='CONTAINS')

    out_fc2 = f'{output_gdb}/{os.path.basename(source_poly)}_join_POS_pt_1to1'
    arcpy.analysis.SpatialJoin(source_poly, pos_pt, out_fc2, 'JOIN_ONE_TO_ONE', 'KEEP_COMMON', match_option='CONTAINS')

    print('Beginning 1:many spatial joins...')
    out_fc3 = f'{output_gdb}/POS_join_{os.path.basename(source_pt)}_1toM'
    arcpy.analysis.SpatialJoin(pos_poly, source_pt, out_fc3, 'JOIN_ONE_TO_MANY', 'KEEP_COMMON', match_option='CONTAINS')

    out_fc4 = f'{output_gdb}/{os.path.basename(source_poly)}_join_POS_pt_1toM'
    arcpy.analysis.SpatialJoin(source_poly, pos_pt, out_fc4, 'JOIN_ONE_TO_MANY', 'KEEP_COMMON', match_option='CONTAINS')

    print('Completed spatial joins... copying results to DBF')
    arcpy.conversion.ExportTable(out_fc1, f'{output_folder}/{os.path.basename(out_fc1)}.dbf')
    arcpy.conversion.ExportTable(out_fc2, f'{output_folder}/{os.path.basename(out_fc2)}.dbf')
    arcpy.conversion.ExportTable(out_fc3, f'{output_folder}/{os.path.basename(out_fc3)}.dbf')
    arcpy.conversion.ExportTable(out_fc4, f'{output_folder}/{os.path.basename(out_fc4)}.dbf')

    print(f"C'est finit pour POS et {os.path.basename(source_poly)}!")


try:
    # NOTE: You need to comment out the lines you don't need with each iteration!
    # Usually you just want to do one state at a time (state sources, TNC, NCED, PADUS)
    # And wildlands is its own thing

    # Singlepart preprocessed data
    pos_sp = f"{output_gdb}/nepos_v2_0_sp_internal"
    #nced_sp = f"{output_gdb}/NCED_albers_sp" 
    nced_sp = f"{output_gdb}/NCED_albers_sp_2024_07"  # From archived GDB used 3/2026
    tnc_sp = f"{output_gdb}/TNC_SA2022_albers_sp"
    massgis_sp = f"{output_gdb}/MassGIS_OpenSpace_albers_sp"
    maine_sp = f"{output_gdb}/Maine_Conserved_Lands_albers_sp"
    ri_local_sp = f"{output_gdb}/RI_Local_albers_sp"
    ri_state_sp = f"{output_gdb}/RI_State_albers_sp"
    nh_sp = f"{output_gdb}/NH_Conservation_Public_Lands_albers_sp"
    vt_sp = f"{output_gdb}/Cadastral_PROTECTEDLND_poly_albers_sp"
    padus_sp = f'{output_gdb}/PADUS4_0Fee_Easement_NE_sp'
    #ct_sp = f'{output_gdb}/CT_DEEP_albers_sp'
    ct_sp = f'{output_gdb}/CT_DEEP_Property_albers_sp_2025_01'  # From archived GDB used 3/2026
    bh_sp = f'{output_gdb}/POS_from_Brian_Hall_albers_sp'
    wildlands_sp = f"{output_gdb}/wildlands_albers_sp"
    srm_sp = f"{output_gdb}/SRM_Cons_120114_sp"
    deleted = f"{output_gdb}/deleted_polygons"

    # Tabulate intersection (% overlap) between polygon features
    calc_pct_overlap(pos_sp, nced_sp)
    calc_pct_overlap(pos_sp, tnc_sp)
    #calc_pct_overlap(pos_sp, massgis_sp)
    #calc_pct_overlap(pos_sp, maine_sp)
    #calc_pct_overlap(pos_sp, ri_local_sp)
    #calc_pct_overlap(pos_sp, ri_state_sp)
    #calc_pct_overlap(pos_sp, nh_sp)
    #calc_pct_overlap(pos_sp, vt_sp)
    calc_pct_overlap(pos_sp, padus_sp)
    calc_pct_overlap(pos_sp, ct_sp)
    #calc_pct_overlap(pos_sp, bh_sp)
    #calc_pct_overlap(pos_sp, wildlands_sp)
    #calc_pct_overlap(pos_sp, srm_sp)

    # Point layers - you can use make_points() to initialize a points
    # layer and then reassign the variable to the string once it exists
    # (or just run make_points() every time - it doesn't take too long)
    #pos_sp_pt = "new_data2.gdb/POS_v2_25_sp_pt"
    pos_sp_pt = make_points(pos_sp)
    nced_sp_pt = make_points(nced_sp)
    tnc_sp_pt = make_points(tnc_sp)
    #massgis_sp_pt = f"{output_gdb}/MassGIS_OpenSpace_albers_sp_pt"
    #massgis_sp_pt = make_points(massgis_sp)
    #maine_sp_pt = f"{output_gdb}/Maine_Conserved_Lands_albers_sp_pt"
    #maine_sp_pt = make_points(maine_sp)
    #ri_local_sp_pt = f"{output_gdb}/RI_Local_albers_sp_pt"
    #ri_state_sp_pt = f"{output_gdb}/RI_State_albers_sp_pt"
    #nh_sp_pt = f"{output_gdb}/NH_Conservation_Public_Lands_albers_sp_pt"
    #nh_sp_pt = make_points(nh_sp)
    #vt_sp_pt = f"{output_gdb}/Cadastral_PROTECTEDLND_poly_albers_sp_pt"
    padus_sp_pt = make_points(padus_sp)
    ct_sp_pt = make_points(ct_sp)
    #bh_sp_pt = f"{output_gdb}/POS_from_Brian_Hall_albers_sp_pt"
    #wildlands_sp_pt = f"{output_gdb}/wildlands_albers_sp_pt"
    #srm_sp_pt = make_points(srm_sp)

    # Conduct spatial joins - joins with the relevant state data, NCED, and TNC are run both iterations
    # to account for the updated geometries in NEPOS
    conduct_joins(pos_sp, pos_sp_pt, nced_sp, nced_sp_pt)
    conduct_joins(pos_sp, pos_sp_pt, tnc_sp, tnc_sp_pt)
    #conduct_joins(pos_sp, pos_sp_pt, massgis_sp, massgis_sp_pt)
    #conduct_joins(pos_sp, pos_sp_pt, maine_sp, maine_sp_pt)
    #conduct_joins(pos_sp, pos_sp_pt, ri_local_sp, ri_local_sp_pt)
    #conduct_joins(pos_sp, pos_sp_pt, ri_state_sp, ri_state_sp_pt)
    #conduct_joins(pos_sp, pos_sp_pt, nh_sp, nh_sp_pt)
    #conduct_joins(pos_sp, pos_sp_pt, vt_sp, vt_sp_pt)
    conduct_joins(pos_sp, pos_sp_pt, padus_sp, padus_sp_pt)
    conduct_joins(pos_sp, pos_sp_pt, ct_sp, ct_sp_pt)
    #conduct_joins(pos_sp, pos_sp_pt, bh_sp, bh_sp_pt)
    #conduct_joins(pos_sp, pos_sp_pt, wildlands_sp, wildlands_sp_pt)
    #conduct_joins(pos_sp, pos_sp_pt, srm_sp, srm_sp_pt)
except Exception:
    print(traceback.format_exc())
finally:
    print_elapsed_time()
