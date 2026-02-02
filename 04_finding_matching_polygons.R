# This code reads in outputs from spatial_matching.py and produces,
# for each state, a "match table" that contains all the sources and match codes
# for that state.
#
# This script is not designed to be run in its entirety. Specific lines are run
# based on the match table you are trying to create.
#
# Prior to running this script, you should have run copy_rows.py which
# generates a DBF file of NEPOS rows that is used to retrieve various attributes including area
# for use in the functions in find_matching_polygons_latest_code.R
#
# There are two sections of the code:
#  - creating a spatial match table for each source
#  - combining these spatial match tables into one table for an entire state
#
# For a state, you will run the lines for the sources relevant to that state to create
# the match tables for each source in the state. Then, combine them using code
# further down in the script to create the state match table that is used in
# the attribute and geometry update functions.
#
# Lucy Lee, 7/2025

# Directory where outputs from spatial_matching.py were sent
# Output tables will also be saved here
setwd("D:/Lee/POS/Update_2023/Data/matching/")

# Script containing functions used to summarize output tables
source("D:/Lee/POS/Update_2023/Code/04_find_matching_polygons_functions.R")

#### DBF OF LATEST SINGLEPART NEPOS VERSION ####
# Create this using copy_rows.py and save to working directory above
nepos <- 'POS_v2_29_sp.dbf'

#### MATCHING TABLES FOR MULTISTATE SOURCES #####
# It is important to update the file names of the tabulate intersection
# output csvs (% overlap csv) to make sure
# you are using the right version - e.g., if you were doing one state
# with NEPOS_v10, but then the next state you have v11 or v12, you
# need to update the CSV name to reflect this since the table names
# contain the POS file name used in the tool.
# The join DBFs all have the same name (they get overwritten each time)
# so those don't need to be updated with each iteration unless that changes.

# TNC
dbf1.tnc <- 'POS_join_TNC_SA2022_albers_sp_pt_1to1.dbf'
dbf2.tnc <- 'TNC_SA2022_albers_sp_join_POS_pt_1to1.dbf'
dbf3.tnc <- 'POS_join_TNC_SA2022_albers_sp_pt_1toM.dbf'
dbf4.tnc <- 'TNC_SA2022_albers_sp_join_POS_pt_1toM.dbf'
pct.csv.tnc <- 'tab_intersect_POS_v2_29_sp_TNC_SA2022_albers_sp.csv'  # Make sure using correct file
tnc <- create_spatial_match_table(dbf1.tnc, dbf2.tnc, dbf3.tnc, dbf4.tnc, pct.csv.tnc, nepos,
                                  source = 'tnc', state = 'VT', save_csv = T)

# NCED
dbf1.nced <- 'POS_join_NCED_albers_sp_pt_1to1.dbf'
dbf2.nced <- 'NCED_albers_sp_join_POS_pt_1to1.dbf'
dbf3.nced <- 'POS_join_NCED_albers_sp_pt_1toM.dbf'
dbf4.nced <- 'NCED_albers_sp_join_POS_pt_1toM.dbf'
pct.csv.nced <- 'tab_intersect_POS_v2_29_sp_NCED_albers_sp.csv'  # Make sure using correct file
nced <- create_spatial_match_table_nced(dbf1.nced, dbf2.nced, dbf3.nced, dbf4.nced, pct.csv.nced, nepos,
                                        source = 'nced', state = 'VT', save_csv = T)

# PADUS
dbf1.padus <- 'POS_join_PADUS4_0Fee_Easement_NE_sp_pt_1to1.dbf'
dbf2.padus <- 'PADUS4_0Fee_Easement_NE_sp_join_POS_pt_1to1.dbf'
dbf3.padus <- 'POS_join_PADUS4_0Fee_Easement_NE_sp_pt_1toM.dbf'
dbf4.padus <- 'PADUS4_0Fee_Easement_NE_sp_join_POS_pt_1toM.dbf'
pct.csv.padus <- 'tab_intersect_POS_v2_29_sp_PADUS4_0Fee_Easement_NE_sp.csv'  # Make sure using correct file
padus <- create_spatial_match_table(dbf1.padus, dbf2.padus, dbf3.padus, dbf4.padus, pct.csv.padus, nepos,
                                    source = 'padus', state = 'VT', save_csv = T)

###### MATCHING TABLES FOR STATE SOURCES
# RI Local
dbf1.ri.local <- 'POS_join_RI_Local_albers_sp_pt_1to1.dbf'
dbf2.ri.local <- 'RI_Local_albers_sp_join_POS_pt_1to1.dbf'
dbf3.ri.local <- 'POS_join_RI_Local_albers_sp_pt_1toM.dbf'
dbf4.ri.local <- 'RI_Local_albers_sp_join_POS_pt_1toM.dbf'
pct.csv.ri.local <- 'tab_intersect_POS_v2_26_sp_RI_Local_albers_sp.csv'  # Make sure using correct file
ri.local <- create_spatial_match_table(dbf1.ri.local, dbf2.ri.local, dbf3.ri.local, dbf4.ri.local,
                                       pct.csv.ri.local, nepos, source = 'ri_local', state = 'RI', save_csv = T)

# RI State
dbf1.ri.state <- 'POS_join_RI_State_albers_sp_pt_1to1.dbf'
dbf2.ri.state <- 'RI_State_albers_sp_join_POS_pt_1to1.dbf'
dbf3.ri.state <- 'POS_join_RI_State_albers_sp_pt_1toM.dbf'
dbf4.ri.state <- 'RI_State_albers_sp_join_POS_pt_1toM.dbf'
pct.csv.ri.state <- 'tab_intersect_POS_v2_26_sp_RI_State_albers_sp.csv'  # Make sure using correct file
ri.state <- create_spatial_match_table(dbf1.ri.state, dbf2.ri.state, dbf3.ri.state, dbf4.ri.state,
                                       pct.csv.ri.state, nepos, source = 'ri_state', state = 'RI', save_csv = T)

# MA
dbf1.massgis <- 'POS_join_MassGIS_OpenSpace_albers_sp_pt_1to1.dbf'
dbf2.massgis <- 'MassGIS_OpenSpace_albers_sp_join_POS_pt_1to1.dbf'
dbf3.massgis <- 'POS_join_MassGIS_OpenSpace_albers_sp_pt_1toM.dbf'
dbf4.massgis <- 'MassGIS_OpenSpace_albers_sp_join_POS_pt_1toM.dbf'
pct.csv.massgis <- 'tab_intersect_POS_v2_29_sp_MassGIS_OpenSpace_albers_sp.csv'  # Make sure using correct file
massgis <- create_spatial_match_table(dbf1.massgis, dbf2.massgis, dbf3.massgis, dbf4.massgis, pct.csv.massgis, nepos,
                                 source = 'massgis', state = 'MA', save_csv = T)

# NH
dbf1.nh <- 'POS_join_NH_Conservation_Public_Lands_albers_sp_pt_1to1.dbf'
dbf2.nh <- 'NH_Conservation_Public_Lands_albers_sp_join_POS_pt_1to1.dbf'
dbf3.nh <- 'POS_join_NH_Conservation_Public_Lands_albers_sp_pt_1toM.dbf'
dbf4.nh <- 'NH_Conservation_Public_Lands_albers_sp_join_POS_pt_1toM.dbf'
pct.csv.nh <- 'tab_intersect_POS_v2_29_sp_NH_Conservation_Public_Lands_albers_sp.csv'  # Make sure using correct file
nh <- create_spatial_match_table(dbf1.nh, dbf2.nh, dbf3.nh, dbf4.nh, pct.csv.nh, nepos,
                                    source = 'nh', state = 'NH', save_csv = T)

# MEGIS
dbf1.megis <- 'POS_join_Maine_Conserved_Lands_albers_sp_pt_1to1.dbf'
dbf2.megis <- 'Maine_Conserved_Lands_albers_sp_join_POS_pt_1to1.dbf'
dbf3.megis <- 'POS_join_Maine_Conserved_Lands_albers_sp_pt_1toM.dbf'
dbf4.megis <- 'Maine_Conserved_Lands_albers_sp_join_POS_pt_1toM.dbf'
pct.csv.megis <- 'tab_intersect_POS_v2_29_sp_Maine_Conserved_Lands_albers_sp.csv'  # Make sure using correct file
megis <- create_spatial_match_table(dbf1.megis, dbf2.megis, dbf3.megis, dbf4.megis, pct.csv.megis, nepos,
                                    source = 'megis', state = 'ME', save_csv = T)

# VT
dbf1.vt <- 'POS_join_Cadastral_PROTECTEDLND_poly_albers_sp_pt_1to1.dbf'
dbf2.vt <- 'Cadastral_PROTECTEDLND_poly_albers_sp_join_POS_pt_1to1.dbf'
dbf3.vt <- 'POS_join_Cadastral_PROTECTEDLND_poly_albers_sp_pt_1toM.dbf'
dbf4.vt <- 'Cadastral_PROTECTEDLND_poly_albers_sp_join_POS_pt_1toM.dbf'
pct.csv.vt <- 'tab_intersect_POS_v2_29_sp_Cadastral_PROTECTEDLND_poly_albers_sp.csv'  # Make sure using correct file
vt <- create_spatial_match_table(dbf1.vt, dbf2.vt, dbf3.vt, dbf4.vt, pct.csv.vt, nepos,
                                 source = 'vt', state = 'VT', save_csv = T)


#### COMBINING MATCH TABLES
ri.local <- 'nepos_ri_local_match_table_2025-05-09_full.csv'
ri.state <- 'nepos_ri_state_match_table_2025-05-09_full.csv'
tnc <- 'nepos_tnc_match_table_2025-05-09_full.csv'
nced <- 'nepos_nced_match_table_2025-05-09_full.csv'
padus <- 'nepos_padus_match_table_2025-05-09_full.csv'
combine_match_tables('RI', ri.state, tnc, nced, padus, ri.local)

nh <- 'nepos_nh_match_table_2025-07-18_full.csv'
tnc <- 'nepos_tnc_match_table_2025-07-18_full.csv'
nced <- 'nepos_nced_match_table_2025-07-18_full.csv'
padus <- 'nepos_padus_match_table_2025-07-18_full.csv'
combine_match_tables('NH', nh, tnc, nced, padus)

massgis <- 'nepos_massgis_match_table_2025-07-18_full.csv'
tnc <- 'nepos_tnc_match_table_2025-07-18_full.csv'
nced <- 'nepos_nced_match_table_2025-07-18_full.csv'
padus <- 'nepos_padus_match_table_2025-07-18_full.csv'
combine_match_tables('MA', massgis, tnc, nced, padus)

vt <- 'nepos_vt_match_table_2025-07-18_full.csv'
tnc <- 'nepos_tnc_match_table_2025-07-18_full.csv'
nced <- 'nepos_nced_match_table_2025-07-18_full.csv'
padus <- 'nepos_padus_match_table_2025-07-18_full.csv'
combine_match_tables('VT', vt, tnc, nced, padus)

me <- 'nepos_megis_match_table_2025-07-18_full.csv'
combine_match_tables('ME', me, tnc, nced, padus)

##### MATCH TABLES FOR "SPECIAL" SOURCES #####
# WWF&C Wildlands
dbf1.wild <- 'POS_join_wildlands_albers_sp_pt_1to1.dbf'
dbf2.wild <- 'wildlands_albers_sp_join_POS_pt_1to1.dbf'
dbf3.wild <- 'POS_join_wildlands_albers_sp_pt_1toM.dbf'
dbf4.wild <- 'wildlands_albers_sp_join_POS_pt_1toM.dbf'
pct.csv.wild <- 'tab_intersect_POS_v2_29_sp_wildlands_albers_sp.csv'  # Make sure using correct file
wild <- create_spatial_match_table(dbf1.wild, dbf2.wild, dbf3.wild, dbf4.wild, pct.csv.wild, nepos,
                                   source = 'wildlands', state = 'VT', save_csv = T)

# SRM PhD Research data (used for YearProt only)
dbf1.srm <- 'POS_join_SRM_Cons_120114_sp_pt_1to1.dbf'
dbf2.srm <- 'SRM_Cons_120114_sp_join_POS_pt_1to1.dbf'
dbf3.srm <- 'POS_join_SRM_Cons_120114_sp_pt_1toM.dbf'
dbf4.srm <- 'SRM_Cons_120114_sp_join_POS_pt_1toM.dbf'
pct.csv.srm <- 'tab_intersect_POS_v2_25_sp_SRM_Cons_120114_sp.csv'  # Make sure using correct file
srm <- create_spatial_match_table_srm(dbf1.srm, dbf2.srm, dbf3.srm, dbf4.srm, pct.csv.srm, nepos,
                                      source = 'SRM', state = 'NH', save_csv = T)

# Deleted polygons
dbf1.deleted <- 'POS_join_deleted_polygons_pt_1to1.dbf'
dbf2.deleted <- 'deleted_polygons_join_POS_pt_1to1.dbf'
dbf3.deleted <- 'POS_join_deleted_polygons_pt_1toM.dbf'
dbf4.deleted <- 'deleted_polygons_join_POS_pt_1toM.dbf'
pct.csv.deleted <- 'tab_intersect_POS_v2_29_sp_deleted_polygons.csv'  # Make sure using correct file
deleted <- create_spatial_match_table_deleted(dbf1.deleted, dbf2.deleted, dbf3.deleted, dbf4.deleted, pct.csv.deleted, nepos,
                                      source = 'deleted_polygons', state = 'VT', save_csv = T)
