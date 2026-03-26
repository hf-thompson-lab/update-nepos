# Creates the public versions of NEPOS to be uploaded to Zenodo
# and also cleans up the internal version
#
# This includes a singlepart version in which internal fields (like YearNotes_Internal)
# have been removed and a multipart version where the fields are restored to the correct
# order (the order gets messed up in the dissolve process because concatenated fields
# are moved to the end).
#
# All edits and development of NEPOS should happen on the INTERNAL SINGLEPART
# version of the data. This way, any changes will propagate through to the public
# versions and we can avoid having version issues between internal and public versions.
#
# Singlepart version: internal fields are deleted, fields are reordered
# Multipart version: fields are reordered to match singlepart version
#
# After running this script, you want to move any old versions of NEPOS
# in the POS_public GDB to the Archive GDB so that the public GDB has only
# the most current POS files. Also, I like to make a folder in the
# POS_shared_with_others folder to record the versions uploaded to Zenodo
# (just so that we are never uncertain!). So you may want to do some
# archiving in that folder once the public data is ready to upload.
#
# Lucy Lee, 1/2026

import arcpy
import os

# Set workspace to wherever the development (internal) POS singlepart and multipart versions live
# This is probably the same workspace as the previous scripts
arcpy.env.workspace = "D:\\Thompson_Lab_POS\\Data\\Old_GDBs_Data\\Update_2025_v2\\ct_2003_correction\\ct_2003_correction.gdb"
arcpy.env.overwriteOutput = True

# Output GDB for public and clean internal versions - needs to be double backslash
# to be compatible with os.path.join
# This script will overwrite any files sent to the same GDB with the same name! So be careful!
public_gdb = "D:\\Thompson_Lab_POS\\Data\\POS_public.gdb"
internal_gdb = "D:\\Thompson_Lab_POS\\Data\\POS_internal.gdb"

# Function to reorder fields in a feature class
# Thank you to Josh Werts! https://joshwerts.com/blog/2014/04/17/arcpy-reorder-fields/
def reorder_fields(table, out_table, field_order, add_missing=True):
    """
    Reorders fields in input featureclass/table
    :table:         input table (fc, table, layer, etc)
    :out_table:     output table (fc, table, layer, etc)
    :field_order:   order of fields (objectid, shape not necessary)
    :add_missing:   add missing fields to end if True (leave out if False)
    -> path to output table
    """
    existing_fields = arcpy.ListFields(table)
    existing_field_names = [field.name for field in existing_fields]

    existing_mapping = arcpy.FieldMappings()
    existing_mapping.addTable(table)

    new_mapping = arcpy.FieldMappings()

    def add_mapping(field_name):
        mapping_index = existing_mapping.findFieldMapIndex(field_name)

        # required fields (OBJECTID, etc) will not be in existing mappings
        # they are added automatically
        if mapping_index != -1:
            field_map = existing_mapping.fieldMappings[mapping_index]
            new_mapping.addFieldMap(field_map)

    # add user fields from field_order
    for field_name in field_order:
        if field_name not in existing_field_names:
            continue
            #raise Exception("Field: {0} not in {1}".format(field_name, table))

        add_mapping(field_name)

    # add missing fields at end
    if add_missing:
        missing_fields = [f for f in existing_field_names if f not in field_order]
        for field_name in missing_fields:
            add_mapping(field_name)

    # use merge with single input just to use new field_mappings
    arcpy.management.Merge(table, out_table, new_mapping)
    return(out_table)


### Clean up internal singlepart NEPOS
# This mainly entails dropping fields that are no longer needed once the update process is complete
# (e.g., geom_updated, PART_COUNT) and putting the fields in the same order as the public versions
def clean_internal_sp(nepos, new_filename):
    sp_field_order = ["FinalID", "FinalID2", "State", "AreaName",
                    "FeeOwner", "FeeOwnType", "FeeOwnCat", "FeeOwnCatComments",
                    "ProtType", "ProtTypeComments", "IntHolder1", "IntHolder1Type",
                    "IntHolder2", "IntHolder2Type", "YearProt", "YearProtComments",
                    "FeeYear", "EaseYear", "WildYear", "GapStatus", "PubAccess",
                    "ProtDuration", "type", "Area_Owner_Name", "Area_Ha", "Area_Ac", "LPT_Num",
                    "Comments", "PolySource", "PolySource_FeatID", "Source_AreaName",
                    "Source_AreaName_FeatID", "Source_FeeOwner", "Source_FeeOwner_FeatID",
                    "Source_ProtType", "Source_ProtType_FeatID", "Source_IntHolder1",
                    "Source_IntHolder1_FeatID", "Source_IntHolder2", "Source_IntHolder2_FeatID",
                    "Source_YearProt", "Source_YearProt_FeatID", "Source_FeeYear",
                    "Source_FeeYear_FeatID", "Source_EaseYear", "Source_EaseYear_FeatID",
                    "Source_WildYear", "Source_WildYear_FeatID", "Source_GapStatus",
                    "Source_GapStatus_FeatID", "Source_PubAccess", "Source_PubAccess_FeatID",
                    "Source_ProtDuration", "Source_ProtDuration_FeatID", "Edit_Date", "Edit_Name",
                    "YearProt_Final", "FeeYear_Final", "EaseYear_Final", "YearNotes_Internal"]
    
    out_internal_clean_sp = os.path.join(internal_gdb, new_filename)

    reorder_fields(nepos, out_internal_clean_sp, sp_field_order, add_missing=False)

    print("Cleaned up internal NEPOS...")

### Delete internal fields form singlepart NEPOS
# Singlepart NEPOS file - update as needed
def create_public_sp(nepos, new_filename):

    # Make a copy of NEPOS for deleting internal fields
    # Also use this opportunity to move Area_Owner_Name to the location
    # specified in the metadata (after type and before area fields)
    sp_field_order = ["FinalID", "FinalID2", "State", "AreaName",
                    "FeeOwner", "FeeOwnType", "FeeOwnCat", "FeeOwnCatComments",
                    "ProtType", "ProtTypeComments", "IntHolder1", "IntHolder1Type",
                    "IntHolder2", "IntHolder2Type", "YearProt", "YearProtComments",
                    "FeeYear", "EaseYear", "WildYear", "GapStatus", "PubAccess",
                    "ProtDuration", "type", "Area_Owner_Name", "Area_Ha", "Area_Ac", "LPT_Num",
                    "Comments", "PolySource", "PolySource_FeatID", "Source_AreaName",
                    "Source_AreaName_FeatID", "Source_FeeOwner", "Source_FeeOwner_FeatID",
                    "Source_ProtType", "Source_ProtType_FeatID", "Source_IntHolder1",
                    "Source_IntHolder1_FeatID", "Source_IntHolder2", "Source_IntHolder2_FeatID",
                    "Source_YearProt", "Source_YearProt_FeatID", "Source_FeeYear",
                    "Source_FeeYear_FeatID", "Source_EaseYear", "Source_EaseYear_FeatID",
                    "Source_WildYear", "Source_WildYear_FeatID", "Source_GapStatus",
                    "Source_GapStatus_FeatID", "Source_PubAccess", "Source_PubAccess_FeatID",
                    "Source_ProtDuration", "Source_ProtDuration_FeatID", "Edit_Date", "Edit_Name"]

    # Delete internal fields
    # This list should not be needed since we can set add_missing to False in the reorder_fields
    # function to drop these fields. However, retaining this list in case DeleteField tool
    # is needed for some reason (and just to have a visual reminder of what fields get dropped
    # in the public version)
    delete_fields = ["PART_COUNT", "ORIG_FID", "geom_updated", "geom_checked", "YearNotes_Internal",
                    "FeeYear_Final", "EaseYear_Final", "YearProt_Final", "FinalID2_OLD"]

    # Output public singlepart version - update filename for future versions!
    out_public_sp = os.path.join(public_gdb, new_filename)

    # Call the reorder fields function with add_missing set to False
    # this will leave out the fields in delete_fields
    reorder_fields(nepos, out_public_sp, sp_field_order, add_missing=False)

    print("Completed singlepart fc...")

### Reorder fields in multipart
def create_public_mp(nepos, out_filename):

    mp_field_order = ["FinalID", "State", "AreaName",
                    "FeeOwner", "FeeOwnType", "FeeOwnCat", "FeeOwnCatComments",
                    "ProtType", "ProtTypeComments", "IntHolder1", "IntHolder1Type",
                    "IntHolder2", "IntHolder2Type", "YearProt", "YearProtComments",
                    "FeeYear", "EaseYear", "WildYear", "GapStatus", "PubAccess",
                    "ProtDuration", "type", "Area_Owner_Name", "Area_Ha", "Area_Ac", "LPT_Num",
                    "Comments"]

    out_public_mp = os.path.join(public_gdb, out_filename)

    # Multipart NEPOS
    reorder_fields(nepos, out_public_mp, mp_field_order)

    print("Completed multipart fc...")


###### CALL FUNCTIONS ######
nepos_sp = "nepos_v2_0_sp_internal"  # Singlepart NEPOS resulting from all previous scripts
clean_internal_sp(nepos_sp, "nepos_v2_0_sp_internal")
create_public_sp(nepos_sp, "nepos_v2_0_sp")

nepos_mp = "POS_final_erase_roads"
create_public_mp(nepos_mp, "nepos_v2_0_mp")
