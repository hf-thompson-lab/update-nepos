# Adds unique FinalID2 to newly added rows
# New rows are identified based on the absense of FinalID
# When manually updating NEPOS, for example if you need to split a polygon
# to incorporate wildland boundaries, you can delete FinalID from the
# split polygon parts (which all share the same FinalID and FinalID2 since they came from
# the same polygon) - this will lead them to get a new FinalID2 assigned
# if you run this script and you don't need to edit FinalID2 by hand.
#
# This script rewrites FinalID2 for new rows each time - therefore, it is 
# not recommended to write any new code that uses FinalID2 until NEPOS is
# complete, in case FinalID2 changes due to rows being added/deleted/moved around.
#
# Lucy Lee, 11/2025

import arcpy

# Geodatabase where singlepart NEPOS lives
arcpy.env.workspace = "D:/Lee/POS/Update_2023/Data/new_data2.gdb/"

### 
def add_finalid2(data):
    # Take all the rows that have FinalID populated (i.e., not new rows) and
    # parse their FinalID2 (singlepart ID), comparing that value to the current
    # max value (initialized at 0). If the row's FinalID2 number if higher than
    # the current max_id, then that number becomes max_id
    # For this assessment we only use rows where FinalID (multipart ID) is NOT null
    # (existing rows, and not new rows which don't have FinalID yet)
    max_id = 0
    with arcpy.da.SearchCursor(data, "FinalID2", "FinalID IS NOT NULL") as cur:
        for row in cur:
            pieces = row[0].split(' - ')
            num = int(pieces[1])   # Create integer of numeric part of FinalID2
            if num > max_id:       # If the current number is higher than max_id
                max_id = num       # Set max_id to be num

    # Query rows that do not have FinalID assigned - these are the new rows
    # All existing rows should have FinalID (multipart ID) and FinalID2 (singlepart ID) populated
    # Newly added rows will not have either
    # The reason for using FinalID in this query instead of FinalID2 is that using FinalID
    # allows for the rerunning of this code to reset the FinalID2 of new rows as changes are made
    # during development (e.g., a polygon is split into 2 because of wildland and now there are 2 rows
    # with the same FinalID that need to be made unique - in such a case it is simplest to set
    # FinalID of those rows to null so they get assigned a unique FinalID2 with this script and
    # FinalID can be set again at the end when it is assigned for other new rows)
    query = "FinalID IS NULL"

    # Counter that will be used to populate numbers for FinalID2 - start with max_id + 1
    n = max_id + 1
    with arcpy.da.UpdateCursor(data, "FinalID2", query) as cur:
        for row in cur:
            id = f"FinalID2 - {n:06}"  # Create ID using padded 6-digit number
            row[0] = id
            cur.updateRow(row)
            n = n + 1
