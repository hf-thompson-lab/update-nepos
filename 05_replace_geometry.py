# Replaces geometries in NEPOS with source polygons based on unique IDs
# POS polygons are processed in groups based on match type code to prevent making mistakes
# Codes 1-4 are the best matches and these are just replaced based on IDs
# Codes 5-8 are less good, and have to match either by area name or fee owner in order for the replacement to happen
# Codes >= 9 should never be updated using this code - these should be checked and done manually in ArcPro
#
# NOTE: Prior to running this script, it is expected that the match type code CSV for the state
# created by find_matching_polygons_all.R has been joined to NEPOS. (And that the matching CSV fields
# from the previous state have been deleted.) 
# It is a good idea to inspect a sample of the different match codes to get a sense of what match codes 
# you feel comfortable updating with this script and what ones you want to update more carefully by hand. 
# The function depends on the match code columns already being joined to NEPOS (for match_code_query)
# argument, to limit which match codes you want to update automatically.
#
# The general workflow of using this function is to go source by source. Within ONE state,
# you start with the most reliable source (this is the state-maintained source for all states except CT).
# Then you can move on to the next most reliable source. So for example if you are updating in MA
# starting with MassGIS, match_code_query would include massgis_match_code as the relevant code field.
# If you then wanted to do TNC polygons in MA, match_code_query would use the field tnc_match_code.
# The match_code_query can also include state and sometimes should - for example with multi-state
# datasets, specifying the state ensures that you don't update polygons outside the state you.
# See comments directly above function for an example.
#
# It is also a good idea to check the results after each source. For example if you updated NH
# with data from NH GIS, check it out before moving on to TNC.
#
# NOTE: The function expects to add a NEW field called geom_updated the first time this function is called
# in an update process. It will populate the field with 0, and change this to 1 when a polygon
# has been updated. This field is checked to avoid updating a polygon that has already been updated
# with geometry from a less-favored source. The geom_updated field from any previous update should
# have been deleted once the data was finalized so this process can start fresh. (Note: it doesn't
# need to be deleted between states, just when data is finally complete for the who dataset.)
#
# NOTE: The function also checked for 'polygon edited' in Comments.lower() and does not update these
# regardless of match code. Instead, these are flagged with 2 in geom_updated to be checked.
# It is important that any manual edits to polygons be flagged with the words 'polygon edited'
# in the Comments field so that these are captured by this code and not changed back to a worse geom!
#
# NOTE: The function also skips over wildlands - including polygons where PolySource is wildlands
# and polygons where Comments includes the word wildlands ('Wildlands used to split source data' is
# a common flag in the data.) These will need to be checked manually for any major changes. You can do
# this by creating a match table for wildlands and joining it to NEPOS and exploring the values in NEPOS.
# Ideally, they will mostly be match code 1 and you can examine any that aren't. Also, you can ask
# Brian if there have been any changes to existing wildlands - he would know!
#
# NOTE: In VT, there were some areas that we identified by AreaName and excluded from VT matching
# because we wanted to use TNC data for them. I don't think this should be necessary anymore,
# because now the data is updated and will match better with TNC. However, I put in down below
# in case it is needed.
#
#
# Lucy Lee, 12/2025

import arcpy
import traceback
import sys

# Workspace should be the space where the development copy of NEPOS
# (i.e., not the GDB where final copies live)
# lives along with the source data that will be used to update geometry
# So that we don't need full paths to any of the datasets
arcpy.env.workspace = "D:\\Lee\\POS\\Update_2023\\Data\\matching.gdb\\"

# Singlepart NEPOS in the workspace GDB
pos = ""

# Function to automatically update geometry in a batch process
# See comments within function for more details
# Arguments:
#  - source_fc (string or object): the preprocessed, singlepart source dataset in the workspace GDB
#  - source_desc (string): the text that will populate PolySource
#    this should match existing formats in NEPOS (summarize PolySource to see those values)
#    and should include the date the source data was last updated at time of download.
#    Current PolySource format:
#        CLCC / Last Green Valley 2021   (This is BH CT/MA data - won't be updated in future)
#        CT DEEP Property M/YYYY
#        CT POS 2011
#        Harvard Forest YYYY  (manual edits not present in any other source data, usually from retaining tracts that are now merged)
#        MassGIS M/YYYY
#        MEGIS M/YYYY
#        NCED M/YYYY
#        NH Conservation Public lands M/YYYY
#        RI Local Conservation Areas M/YYYY
#        RI State Conservation Areas M/YYYY
#        TNC SAYYYY
#        USGS PAD-US v4.0  (PADUS uses version number instead of a date)
#        VT Conserved Lands Inventory M/YYYY
#        VT Protected Lands Database M/YYYY
#        WWF&C Wildlands M/YYYY
#  - match_code_query (string SQL): what range of match codes do you want to update in batch?
#    the right values will depend on inspection of the data & match codes for each source.
#    it is also recommended to include the state, so that for multistate datasets like TNC
#    you only do the rows in the state you are currently working on. Example:
#    "State = 'MA' AND tnc_match_code >= 1 AND tnc_match_code <= 4"
#    NOTE: It may be best to start with the very best matches for each source (i.e., match code 1 only
#    query like "State = 'NH' AND nh_match_code = 1" followed by "State = 'NH' AND tnc_match_code = 1").
#    This will ensure that polygons get updated with their very best match (probably their
#    existing PolySource). This could be especially important in states like VT and NH where
#    there are substantial contributions from both the state-maintained datasets and TNC.
#    We don't want to have to redo all that painstaking work in areas like WMNF and GMNF!
#  - n_parts_src_uid (integer, 1 or 2): a 1 part UID is a string like "849392" while a
#    2 part UID contains a dash like "35-102". as of 2025, only MassGIS and WWF&C Wildlands have UIDs that
#    contain a dash and these are the only sources for which n_parts_src_uid should be set to 2.
#    However, it's not really recommended to update Wildlands automatically using this code - it's a special
#    source and should be checked manually, especially if there's not much that has changed, in which case
#    we risk undoing some changes that took a lot of time to do the first time around. (Some polygons
#    have PolySource of WWF&C Wildlands but were also manually drawn or minorly edited for alignment.)
# Returns:
#  Nothing is returned by this function. This function alters NEPOS and it is a good idea
#  to make a copy of NEPOS before each new use so you can go back to a previous version
#  if you make any mistake using this function.
def replace_geometry(source_fc, source_desc, match_code_query, n_parts_src_uid = 1):
    # Add field to store whether geometry was updated
    # If an exception occurs, it's probably just that the field already exists
    # This only needs to be done once at the beginning of updating NEPOS
    # The field can remain in tact between states, but the old geom_updated
    # from a previous update process should be removed
    field_names = [f.name for f in arcpy.ListFields(pos)]
    if 'geom_updated' not in field_names:
        arcpy.management.AddField(pos, 'geom_updated', 'SHORT')
        arcpy.management.CalculateField(pos, 'geom_updated', 0)
        print('Added and calculated geom_updated field...')
    else:
        print('geom_updated already exists in POS...')

    # Create a dictionary of source geometries by UID2
    src_geom = {key:value for (key, value) in arcpy.da.SearchCursor(source_fc, ['UID2', 'SHAPE@'])}
    print('Created dictionary of source UIDs and geometries...')

    # Use UpdateCursor to update geom of POS rows w/ relevant match code to be source geometry
    # UID2 is the UID2 from source data which is also a key in src_geom dictionary
    # SHAPE@ is the geometry object
    # The last three fields are for internal record keeping
    fields = ['UID2', 'SHAPE@', 'geom_updated', 'PolySource', 'PolySource_FeatID', 'Comments']
    c = 0    # Variable to count the number of rows updated
    f = 0    # Variable to count the number of rows flagged but not updated
    with arcpy.da.UpdateCursor(pos, fields, match_code_query) as cur:
        for row in cur:
            if row[2] == 0:                         # Only process rows that have not had geometry updated
                try:                                # And rows that are not manually edited and/or from WWF&C wildlands
                    if 'polygon edited' not in row[5].lower() and 'wildlands' not in row[5].lower() and 'wildlands' not in row[3].lower():
                        # Replace geometry
                        src_id = row[0]                 # Get the ID of the source polygon matched with the POS row
                        row[1] = src_geom[src_id]       # Replace POS geometry with the geometry in src_geom dictionary

                        # Record the changes
                        row[2] = 1                      # Value of 1 means the geometry was updated
                        row[3] = source_desc            # Update PolySource
                        id_parts = row[0].split('-')    # Split UID2 by dashes

                        # Reconstruct UID based on number of parts expected
                        # If there are ever UIDs not in format "ABC123" or "ABC-123"
                        # the conditional statements below need to be updated/expanded
                        if n_parts_src_uid == 1:
                            orig_id = id_parts[0]
                        elif n_parts_src_uid == 2:
                            orig_id = '-'.join(id_parts[0:2])
                        else:
                            print("Expected UID to contain 1 or 2 parts - please update n_parts_src_uid or code")
                            sys.exit()
                        row[4] = orig_id    # Update PolySource_FeatID with original UID from source data

                        c = c + 1           # Increment counter
                        cur.updateRow(row)  # Update the row
                    else:                   # If 'polygon edited' is in Comments,
                        row[2] = 2          # flag geom_updated as 2 and do not update geometry
                        f = f + 1           # Increment flag counter
                        cur.updateRow(row)  # Update the row
                except Exception:
                    print(traceback.format_exc())
                    sys.exit()
    print('Updated geometries for {} rows!'.format(c))
    print('Flagged {} rows where polygon has been edited'.format(f))

# See 07_replace_geometry_VT.py in the archive of code as it was used for 2023 update
# for more information on this - would need to edit the replce_geometry function to 
# include: if row[5].lower() in pas_use_tnc:
#            continue
pas_use_tnc = ['perry holbrook memorial state park', 'willoughby state forest', 'victory basin wma',
               'mt. mansfield state forest', 'camels hump state park', 'groton state forest', 'dead creek wma',
               'green mountain national forest', 'coolidge state forest', 'ascutney state park',
               'townsend state forest', 'whipstock hill wma', 'roxbury state forest', 'west mountain wma',
               'calendar brook wma']

##### EXAMPLE FUNCTION CALLS - DO NOT RUN #####
### These examples show what the function calls could look like ###
## Updating NH rows using NH data followed by TNC - best match only for each source
# replace_geometry("NH_Conserved_Lands_sp", "NH Conservation and Public Lands 5/2024", "State = 'NH' AND nh_match_code = 1", n_parts_src_uid=1)
# replace_geometry("TNC_Secured_Areas_2022_sp", "TNC SA2022", "State = 'NH' AND tnc_match_code = 1", n_parts_src_uid=1)

## Updating MassGIS rows using multiple match codes
# replace_geometry("MassGIS_OS_sp", "MassGIS 1/2025", "State = 'MA' AND massgis_match_code >= 1 AND massgis_match_code <= 4", n_parts_src_uid=2)