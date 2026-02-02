# -*- coding: utf-8 -*-

import arcpy
import traceback
import sys


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "POSToolbox_v2"
        self.alias = "POSToolbox v2"

        # List of tool classes associated with this toolbox
        self.tools = [ReplacePolygonGeometry, UpdateAttributes, UpdateFeeEaseYear, AppendDeletedPolygon, AddRow, SpatialCompare]


# Function to replace geometry of a polygon with geometry of another polygon
# The parameter SRC_name below needs to be updated each update process to reflect the latest dates
class ReplacePolygonGeometry(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "ReplacePolygonGeometry"
        self.description = "Replaces geometry of a polygon with shape from a polygon in a different layer"
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        params = []

        POS_poly = arcpy.Parameter(name="POS_poly",
                                   displayName="POS Layer to Update",
                                   direction="Input",
                                   datatype="GPFeatureLayer",
                                   parameterType="Required")
        params.append(POS_poly)

        SRC_poly = arcpy.Parameter(name="SRC_poly",
                                   displayName="Source Layer Providing Update",
                                   direction="Input",
                                   datatype="GPFeatureLayer",
                                   parameterType="Required")
        params.append(SRC_poly)

        SRC_name = arcpy.Parameter(name="SRC_name",
                                   displayName="Name of source to populate PolySource",
                                   direction="Input",
                                   datatype="GPString",
                                   parameterType="Required")
        params.append(SRC_name)

        # UPDATE THIS LIST AS NEEDED!
        SRC_name.filter.list = ['CT DEEP Property 1/2025',
                                'CLCC / Last Green Valley 2021',
                                'VT Protected Lands Database 6/2021',
                                'VT Conserved Lands Inventory 10/2024',
                                'NH Conservation Public Lands 3/2025',
                                'RI Local Conservation Areas 4/2025', 'RI State Conservation Areas 2/2025',
                                'MEGIS 3/2025', 'MassGIS 1/2025',
                                'TNC SA2022', 'NCED 7/2023', 'USGS PAD-US v4.0']
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        self.replace_geometry(parameters[0].value, parameters[1].value, parameters[2].value, messages)
        return

    def replace_geometry(self, arg1, arg2, arg3, messages):
        # Rename arguments
        # Not sure this is necessary if parameter type is GPFeatureLayer?
        pos = arg1
        src = arg2
        src_name = arg3

        # Check to make sure that one record is selected in each input
        pos_desc = arcpy.da.Describe(pos)
        src_desc = arcpy.da.Describe(src)

        n_select_pos = len(pos_desc['FIDSet'])   # Length of list holding IDs of selected records
        n_select_src = len(src_desc['FIDSet'])

        # There should be exactly one feature selected in each layer
        if n_select_pos != 1 or n_select_src != 1:
            arcpy.AddError("One or both layers do not have exactly 1 feature selected. Please select one feature in each input layer.")
            sys.exit()
        else:
            arcpy.AddMessage("Both layers have one feature selected.")

        # Create a dictionary of the source - it will be a dictionary of one key value pair
        # (the polygon that is selected)
        src_geom = {key: value for (key, value) in arcpy.da.SearchCursor(src, ['UID2', 'SHAPE@'])}
        arcpy.AddMessage("Created dictionary of {} ID-geometry pairs...".format(len(src_geom)))

        # Retrieve the UID of the selected polygon in case it doesn't match the UID2 contained in POS
        selected_poly_id = list(src_geom.keys())[0]
        arcpy.AddMessage('Selected polygon ID is {}'.format(selected_poly_id))

        fields = ['SHAPE@', 'geom_updated', 'PolySource', 'PolySource_FeatID', 'Area_Ac', 'Area_Ha']
        with arcpy.da.UpdateCursor(pos, fields) as cur:
            for row in cur:
                try:
                    arcpy.AddMessage('Override ID check is True...')
                    # Update geometry so it is from selected_poly_id, not src_id
                    row[0] = src_geom[selected_poly_id]

                    # Record the changes
                    row[1] = 1  # Value of 1 means the geometry was updated
                    row[2] = src_name
                    id_parts = selected_poly_id.split('-')  # Get the original ID from selected_poly_id
                    if src_name[:7] == 'MassGIS':
                        orig_id = '-'.join(id_parts[0:2])  # MassGIS IDs have a dash, so we reconstruct by joining first 2 ID parts with a dash
                    elif (src_name[:3] == 'TNC' or src_name[:4] == 'NCED' or src_name[:5] == 'MEGIS' 
                    or src_name[:4] == 'USGS' or src_name[:2] == 'RI' or src_name[:2] == 'NH' 
                    or src_name[:2] == 'VT' or src_name[:2] == 'CT' or src_name[:4] == 'CLCC'):
                        orig_id = id_parts[0]  # These sources don't have a dash in original UID, so just take first part
                    row[3] = orig_id
                    cur.updateRow(row)    # Update row so SHAPE@ is updated
                    arcpy.AddMessage("Updated geometry!")
                    
                    # Update area fields with new SHAPE@
                    geom = row[0]
                    acres = geom.getArea("PLANAR", "AcresUS")
                    row[4] = acres
                    hectares = geom.getArea("PLANAR", "Hectares")
                    row[5] = hectares
                    cur.updateRow(row)
                    arcpy.AddMessage("Updated area fields!")
                except Exception:
                    print(traceback.format_exc())


    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return

# Function to update an attribute for one NEPOS polygon with attribute
# from a source polygon. This function works the same way as ReplaceGeometry
# but with attributes instead of SHAPE@. 
class UpdateAttributes(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "UpdateAttributes"
        self.description = "Updates NEPOS attributes with source attributes"
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        params = []

        POS_poly = arcpy.Parameter(name="POS_poly",
                                   displayName="POS Layer to Update",
                                   direction="Input",
                                   datatype="GPFeatureLayer",
                                   parameterType="Required")
        params.append(POS_poly)

        SRC_poly = arcpy.Parameter(name="SRC_poly",
                                   displayName="Source Layer Providing Update",
                                   direction="Input",
                                   datatype="GPFeatureLayer",
                                   parameterType="Required")
        params.append(SRC_poly)

        SRC_name = arcpy.Parameter(name="SRC_name",
                                   displayName="Name of source",
                                   direction="Input",
                                   datatype="GPString",
                                   parameterType="Required")
        params.append(SRC_name)

        # UPDATE AS NEEDED!
        SRC_name.filter.list = ["HF Wildlands 4/2022",
                                'CT DEEP Property 1/2025',
                                'VT Protected Lands Database 6/2021',
                                'VT Conserved Lands Inventory 10/2024',
                                'NH Conservation Public Lands 3/2025',
                                'RI Local Conservation Areas 4/2025', 'RI State Conservation Areas 2/2025',
                                'MassGIS 1/2025', 'TNC SA2022', 'NCED 7/2024',
                                'USGS PAD-US v4.0', 'CBI PAD-US v2.1', 'MEGIS 3/2025']

        attribute = arcpy.Parameter(name="attribute",
                                    displayName="Field to update",
                                    direction="Input",
                                    datatype="GPString",
                                    parameterType="Required")
        params.append(attribute)
        attribute.filter.list = ['AreaName', 'FeeOwner', 'FeeOwnType', 'IntHolder1', 'IntHolder1Type',
                                 'IntHolder2', 'IntHolder2Type', 'ProtType', 'YearProt', 'GapStatus',
                                 'PubAccess', 'ProtDuration', 'WildYear']

        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        self.update_attributes(parameters[0].value, parameters[1].value, parameters[2].value, parameters[3].value, messages)
        return

    def update_attributes(self, arg1, arg2, arg3, arg4, messages):
        # Rename arguments
        # Not sure this is necessary if parameter type is GPFeatureLayer?
        pos = arg1
        src = arg2
        src_name = arg3
        field = arg4

        # Check to make sure that one record is selected in each input
        pos_desc = arcpy.da.Describe(pos)
        src_desc = arcpy.da.Describe(src)

        n_select_pos = len(pos_desc['FIDSet'])   # Length of list holding IDs of selected records
        n_select_src = len(src_desc['FIDSet'])

        # There should be exactly one feature selected in each layer
        if n_select_pos != 1 or n_select_src != 1:
            arcpy.AddError("One or both layers do not have exactly 1 feature selected. Please select one feature in each input layer.")
            sys.exit()
        else:
            arcpy.AddMessage("Both layers have one feature selected.")

        # Create a dictionary of the source - it will be a dictionary of one key value pair
        # (the polygon that is selected). Fields in source data should already match NEPOS field names/values.
        src_att = {key: value for (key, value) in arcpy.da.SearchCursor(src, ['UID2', field])}
        arcpy.AddMessage("Created dictionary of {} ID-attribute pairs...".format(len(src_att)))

        if src_name[:7] == 'MassGIS':
            comm = {key: value for (key, value) in arcpy.da.SearchCursor(src, ['UID2', 'YearProtComments'])}

        # Retrieve the UID of the selected polygon
        selected_src_id = list(src_att.keys())[0]
        arcpy.AddMessage('Retrieved selected source polygon ID...')

        # Fields for update cursor depend on the field being updated
        # Owner / int holder type don't have associated source fields
        source_field = 'Source_' + field
        source_field_feat = source_field + '_FeatID'
        if field not in ['FeeOwnType', 'IntHolder1Type', 'IntHolder2Type'] and field != 'YearProt':
            fields = [field, source_field, source_field_feat]
        elif field == 'YearProt':
            fields = [field, source_field, source_field_feat, 'YearProtComments']
        else:
            fields = [field]
        arcpy.AddMessage('Assembled fields: {}'.format(fields))

        # Cursor updates the field in NEPOS with the field value in the selected source polygon
        # Again, this tool expects NEPOS and source datasets to share the same field names
        with arcpy.da.UpdateCursor(pos, fields) as cur:
            arcpy.AddMessage('Initiated cursor...')
            for row in cur:
                try:
                    # Get the attribute (value) in the dictionary
                    # There is only one entry and therefore one value in the dict
                    row[0] = list(src_att.values())[0]
                    arcpy.AddMessage('Populated {}...'.format(fields[0]))

                    # If more than one field is in the cursor, update the Source and Source_FeatID fields
                    if len(fields) > 1:
                        row[1] = src_name
                        id_parts = str(selected_src_id).split('-')  # Get the original ID from selected_poly_id
                        if src_name[:7] == 'MassGIS':
                            orig_id = '-'.join(id_parts[0:2])  # MassGIS IDs have a dash, so we reconstruct by joining first 2 ID parts with a dash
                        elif (src_name[:3] == 'TNC' or src_name[:4] == 'NCED' or src_name[:4] == 'USGS' or src_name[:3] == 'CBI' 
                        or src_name[:5] == 'MEGIS' or src_name[:2] == 'RI' or src_name[:2] == 'NH' or src_name[:2] == 'VT' or 
                        src_name[:2] == 'CT'):
                            orig_id = id_parts[0]   # These source UIDs don't have a dash, so we just need first part
                        row[2] = orig_id
                        arcpy.AddMessage('Populated {} and {}...'.format(fields[1], fields[2]))
                    # If YearProt is being updated with this tool, we can reset YearProtComments if it doesn't contain
                    # the keywords FeeYear or EaseYear, which indicate a comment we want to keep
                    # We also want to see if the year is from FY_Funding for MassGIS
                    if field == 'YearProt':
                        arcpy.AddMessage('Field is YearProt, checking YearProtComments...')
                        # existing_comment = row[3]
                        # if 'EaseYear' not in existing_comment and 'FeeYear' not in existing_comment:
                        #     if src_name[:7] == 'MassGIS':
                        #         if comm[selected_src_id] is not None:
                        #             row[3] = comm[selected_src_id]
                        #         else:
                        #             row[3] = None
                        #     else:
                        #         row[3] = None
                        row[3] = None
                        arcpy.AddMessage('Updated {}...'.format(fields[3]))

                    cur.updateRow(row)
                    arcpy.AddMessage("Updated {}!".format(fields[0]))
                except Exception:
                    print(traceback.format_exc())

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return

# Function to update FeeYear or EaseYear
class UpdateFeeEaseYear(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "UpdateFeeEaseYear"
        self.description = "Updates NEPOS EaseYear and/or FeeYear attributes"
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        params = []

        POS_poly = arcpy.Parameter(name="POS_poly",
                                   displayName="POS Layer to Update",
                                   direction="Input",
                                   datatype="GPFeatureLayer",
                                   parameterType="Required")
        params.append(POS_poly)

        year_prot = arcpy.Parameter(name="year_prot",
                                    displayName="YearProt",
                                    direction="Input",
                                    datatype="GPString",
                                    parameterType="Optional")
        params.append(year_prot)

        ease_year = arcpy.Parameter(name='ease_year',
                                    displayName="EaseYear",
                                    direction="Input",
                                    datatype="GPString",
                                    parameterType="Optional")
        params.append(ease_year)

        fee_year = arcpy.Parameter(name='fee_year',
                                   displayName="FeeYear",
                                   direction="Input",
                                   datatype="GPString",
                                   parameterType="Optional")
        params.append(fee_year)

        year_comments = arcpy.Parameter(name="year_comments",
                                        displayName="YearProt_Comments",
                                        direction="Input",
                                        datatype="GPString",
                                        parameterType="Optional")
        params.append(year_comments)
        year_comments.filter.list = ['Clear YearProtComments',
                                     'EaseYear and FeeYear based on Comments', 'YearProt and EaseYear based on Comments',
                                     'YearProt and FeeYear based on Comments', 'Years of protection based on Comments']

        SRC_poly = arcpy.Parameter(name="SRC_poly",
                                   displayName="Source Layer Providing Update",
                                   direction="Input",
                                   datatype="GPFeatureLayer",
                                   parameterType="Required")
        params.append(SRC_poly)

        SRC_name = arcpy.Parameter(name="SRC_name",
                                   displayName="Name of source",
                                   direction="Input",
                                   datatype="GPString",
                                   parameterType="Required")
        params.append(SRC_name)
        SRC_name.filter.list = ['MassGIS 1/2025', 'TNC SA2022', 'NCED 7/2023', 'USGS PAD-US v3.0', 'CBI PAD-US v2.1']

        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        self.update_fee_ease_year(parameters[0].value, parameters[1].value, parameters[2].value, parameters[3].value,
                                  parameters[4].value, parameters[5].value, parameters[6].value, messages)
        return

    def update_fee_ease_year(self, arg1, arg2, arg3, arg4, arg5, arg6, arg7, messages):
        # Rename arguments
        # Not sure this is necessary if parameter type is GPFeatureLayer?
        pos = arg1
        year = arg2
        ease_year = arg3
        fee_year = arg4
        year_comments = arg5
        src = arg6
        src_name = arg7

        # Check to make sure that one record is selected in each input
        pos_desc = arcpy.da.Describe(pos)
        src_desc = arcpy.da.Describe(src)

        n_select_pos = len(pos_desc['FIDSet'])   # Length of list holding IDs of selected records
        n_select_src = len(src_desc['FIDSet'])

        # There should be exactly one feature selected in each layer
        if n_select_pos != 1 or n_select_src != 1:
            arcpy.AddError("One or both layers do not have exactly 1 feature selected. Please select one feature in each input layer.")
        else:
            arcpy.AddMessage("Both layers have one feature selected.")

        # Create a dictionary of the source - it will be a dictionary of one key value pair
        # (the polygon that is selected). The field does not matter, just need to get the ID
        src_att = {key: value for (key, value) in arcpy.da.SearchCursor(src, ['UID2', 'UID2'])}

        # Retrieve the UID of the selected polygon
        selected_src_id = list(src_att.keys())[0]

        # Cursor updates the field in NEPOS with the field value in the selected source polygon
        # Again, this tool expects NEPOS and source datasets to share the same field names
        with arcpy.da.UpdateCursor(pos, ['YearProt', 'Source_YearProt', 'Source_YearProt_FeatID',
                                         'EaseYear', 'Source_EaseYear', 'Source_EaseYear_FeatID',
                                         'FeeYear', 'Source_FeeYear', 'Source_FeeYear_FeatID', 'YearProtComments']) as cur:
            for row in cur:
                try:
                    # Get selected source polygon ID to populate Source_FeatID fields
                    id_parts = str(selected_src_id).split('-')  # Get the original ID from selected_poly_id
                    if src_name[:7] == 'MassGIS':
                        orig_id = '-'.join(id_parts[0:2])  # MassGIS IDs have a dash, so we reconstruct by joining first 2 ID parts with a dash
                    elif src_name[:3] == 'TNC' or src_name[:4] == 'NCED' or src_name[:4] == 'USGS' or src_name[:3] == 'CBI':
                        orig_id = id_parts[0]  # These source UIDs don't have a dash, so we just need first part

                    # Populate fields that aren't empty
                    if year is not None:
                        row[0] = int(year)
                        row[1] = src_name
                        row[2] = orig_id
                    if ease_year is not None:
                        row[3] = int(ease_year)
                        row[4] = src_name
                        row[5] = orig_id
                    if fee_year is not None:
                        row[6] = int(fee_year)
                        row[7] = src_name
                        row[8] = orig_id
                    if year_comments is not None:
                        if year_comments == 'Clear YearProtComments':
                            row[9] = None
                        else:
                            row[9] = year_comments
                    cur.updateRow(row)
                except Exception:
                    print(traceback.format_exc())

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return

# Function to add selected polygon(s) to the deleted_polygons layer
# Any number of NEPOS polygons can be selected for this.
# This function also recalculates UID2 (the unique ID for deleted polygons)
# so that any time data is added the field is updated and is complete.
class AppendDeletedPolygon(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Append to Deleted Polygons"
        self.description = "Takes selected polygon(s) and appends to the deleted_polygons layer"
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        params = []

        POS_poly = arcpy.Parameter(name="POS_poly",
                                   displayName="POS Layer to Update",
                                   direction="Input",
                                   datatype="GPFeatureLayer",
                                   parameterType="Required")
        params.append(POS_poly)

        deleted_poly = arcpy.Parameter(name="deleted_poly",
                                       displayName="Deleted polygons layer",
                                       direction="Input",
                                       datatype="GPFeatureLayer",
                                       parameterType="Requiired")
        params.append(deleted_poly)

        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        self.append_deleted_polygon(parameters[0].value, parameters[1].value, messages)
        return

    def append_deleted_polygon(self, arg1, arg2, messages):
        # Rename arguments
        # Not sure this is necessary if parameter type is GPFeatureLayer?
        pos = arg1
        deletes = arg2

        # Get number of selected polygons in POS
        pos_desc = arcpy.da.Describe(pos)
        n_select_pos = len(pos_desc['FIDSet']) 
        
        # Run the Append tool with the desired settings
        arcpy.management.Append(pos, deletes, "NO_TEST")
        arcpy.AddMessage("Appended {} polygons to {}".format(n_select_pos, deletes))

        # Calculate UID2 as OBJECTID
        arcpy.management.CalculateField(deletes, "UID2", "str(!OBJECTID!)")
        arcpy.AddMessage("Calculated UID2 as OBJECTID...")


    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return

# Function to add row(s) to NEPOS
# This tool expects preprocessed, singlepart source data. It can be the 
# output of SpatialCompare (see below) or the regular ungrouped source
# data. Any number of rows can be selected to be added.
# The tool adds the information for the Source and Source_FeatID fields
# and calculates Area_Ac and Area_Ha for the new rows.
class AddRow(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "AddRow"
        self.description = "Adds a row or rows to NEPOS. Basically the Append tool with some customization to populate Source fields."
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        params = []

        POS_poly = arcpy.Parameter(name="POS_poly",
                                   displayName="POS Layer to Update",
                                   direction="Input",
                                   datatype="GPFeatureLayer",
                                   parameterType="Required")
        params.append(POS_poly)

        SRC_poly = arcpy.Parameter(name="SRC_poly",
                                   displayName="Source Layer Providing Row(s)",
                                   direction="Input",
                                   datatype="GPFeatureLayer",
                                   parameterType="Required")
        params.append(SRC_poly)

        SRC_name = arcpy.Parameter(name="SRC_name",
                                   displayName="Name of source to populate PolySource",
                                   direction="Input",
                                   datatype="GPString",
                                   parameterType="Required")
        params.append(SRC_name)

        # UPDATE AS NEEDED!
        SRC_name.filter.list = ['CT DEEP Property 1/2025',
                                'CLCC / Last Green Valley 2021',
                                'VT Protected Lands Database 6/2021',
                                'VT Conserved Lands Inventory 10/2024',
                                'NH Conservation Public Lands 3/2025',
                                'RI Local Conservation Areas 4/2025', 'RI State Conservation Areas 2/2025',
                                'MEGIS 3/2025', 'MassGIS 1/2025',
                                'TNC SA2022', 'NCED 7/2024', 'USGS PAD-US v4.0', 'HF Wildlands 4/2022']
        
        GDB_workspace = arcpy.Parameter(name="GDB Workspace",
                                        displayName = "GDB Workspace",
                                        direction="Input",
                                        datatype="DEWorkspace",
                                        parameterType="Required")
        params.append(GDB_workspace)

        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        self.add_rows(parameters[0].value, parameters[1].value, parameters[2].value, parameters[3].value, messages)
        return

    def add_rows(self, arg1, arg2, arg3, arg4, messages):
        # Rename arguments
        # Not sure this is necessary if parameter type is GPFeatureLayer?
        pos = arg1
        src = arg2
        src_name = arg3
        gdb = arg4

        # Set workspace to avoid path issues when copying features
        arcpy.env.workspace = gdb

        # Check how many rows are selected
        src_desc = arcpy.da.Describe(src)

        # Length of list holding IDs of selected records
        n_select_src = len(src_desc['FIDSet'])

        arcpy.AddMessage(f"There are {n_select_src} rows selected to Append...")

        # Copy selected source features to a new table
        # This is important - doesn't seem to work as a temporary layer, we need
        # to make a new layer and then delete it at the end
        arcpy.management.CopyFeatures(src, "new_rows")
        arcpy.AddMessage("Copied selected rows to new FC...")

        # Add source fields to new rows to make new rows match structure of NEPOS
        arcpy.management.AddField("new_rows", "PolySource", "TEXT", field_length = 50)
        arcpy.management.AddField("new_rows", "PolySource_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField("new_rows", "Source_AreaName", "TEXT", field_length = 50)
        arcpy.management.AddField("new_rows", "Source_AreaName_FeatID", "TEXT", field_length = 50)
        arcpy.management.AddField("new_rows", "Source_FeeOwner", "TEXT", field_length = 50)
        arcpy.management.AddField("new_rows", "Source_FeeOwner_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField("new_rows", "Source_ProtType", "TEXT", field_length = 50)
        arcpy.management.AddField("new_rows", "Source_ProtType_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField("new_rows", "Source_IntHolder1", "TEXT", field_length = 50)
        arcpy.management.AddField("new_rows", "Source_IntHolder1_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField("new_rows", "Source_IntHolder2", "TEXT", field_length = 50)
        arcpy.management.AddField("new_rows", "Source_IntHolder2_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField("new_rows", "Source_YearProt", "TEXT", field_length = 50)
        arcpy.management.AddField("new_rows", "Source_YearProt_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField("new_rows", "Source_FeeYear", "TEXT", field_length = 50)
        arcpy.management.AddField("new_rows", "Source_FeeYear_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField("new_rows", "Source_EaseYear", "TEXT", field_length = 50)
        arcpy.management.AddField("new_rows", "Source_EaseYear_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField("new_rows", "Source_GapStatus", "TEXT", field_length = 50)
        arcpy.management.AddField("new_rows", "Source_GapStatus_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField("new_rows", "Source_PubAccess", "TEXT", field_length = 50)
        arcpy.management.AddField("new_rows", "Source_PubAccess_FeatID", "TEXT", field_length = 25)
        arcpy.management.AddField("new_rows", "Source_ProtDuration", "TEXT", field_length = 50)
        arcpy.management.AddField("new_rows", "Source_ProtDuration_FeatID", "TEXT", field_length = 25)
        arcpy.AddMessage("Added Source fields...")

        # Populate Source fields
        fields = ["PolySource", "PolySource_FeatID", "Source_AreaName", "Source_AreaName_FeatID",
                  "Source_FeeOwner", "Source_FeeOwner_FeatID", "Source_ProtType", "Source_ProtType_FeatID",
                  "Source_YearProt", "Source_YearProt_FeatID", "Source_GapStatus", "Source_GapStatus_FeatID",
                  "Source_PubAccess", "Source_PubAccess_FeatID", "Source_ProtDuration", "Source_ProtDuration_FeatID",
                  "UID"]
        with arcpy.da.UpdateCursor("new_rows", fields) as cur:
            for row in cur:
                row[0] = src_name   # PolySource
                row[1] = row[16]
                row[2] = src_name   # AreaName
                row[3] = row[16]
                row[4] = src_name   # FeeOwner
                row[5] = row[16]
                row[6] = src_name   # ProtType
                row[7] = row[16]
                row[8] = src_name   # YearProt
                row[9] = row[16]
                row[10] = src_name  # GapStatus
                row[11] = row[16]
                row[12] = src_name  # PubAccess
                row[13] = row[16]
                row[14] = src_name  # ProtDuration
                row[15] = row[16]
                cur.updateRow(row)
        arcpy.AddMessage("Calculated most source fields...")

        # Use an UpdateCursor for fields that may or may not have a value
        fields = ["IntHolder1", "Source_IntHolder1", "Source_IntHolder1_FeatID",
                  "IntHolder2", "Source_IntHolder2", "Source_IntHolder2_FeatID",
                  "FeeYear", "Source_FeeYear", "Source_FeeYear_FeatID",
                  "EaseYear", "Source_EaseYear", "Source_EaseYear_FeatID", 
                  "UID"]
        with arcpy.da.UpdateCursor("new_rows", fields) as cur:
            for row in cur:
                if row[0] is not None and row[0] != '' and row[0] != ' ':
                    row[1] = src_name
                    row[2] = row[12]
                if row[3] is not None and row[3] != '' and row[3] != ' ':
                    row[4] = src_name
                    row[5] = row[12]
                if row[6] is not None:
                    row[7] = src_name
                    row[8] = row[12]
                if row[9] is not None:
                    row[10] = src_name
                    row[11] = row[12]
                cur.updateRow(row)
        arcpy.AddMessage("Calculated remaining source fields...")

        # Drop fields that aren't in NEPOS -- this technically shouldn't be necessary
        # but in standalone script this code didn't work unless this was done
        nepos_field_names = [f.name for f in arcpy.ListFields(pos)]
        src_fields = arcpy.ListFields("new_rows")

        # Dropping ORIG_FID because these data did not have a ORIG_FID in NEPOS
        # we can tell if new rows are from the same polygon by the PolySource_FeatID field
        drop_fields = ["ORIG_FID"]
        for s in src_fields:
            if s.name.lower()[:5] == 'shape':
                continue
            elif s.name not in nepos_field_names:
                drop_fields.append(s.name)
                print(f"Added {s.name} to drop_fields")
            else:
                continue

        arcpy.management.DeleteField("new_rows", drop_fields)
        arcpy.AddMessage("Deleted source fields not in NEPOS...")

        # Append the new rows now that the structure matches!
        arcpy.management.Append("new_rows", pos, "NO_TEST")
        arcpy.AddMessage("Appended new rows to NEPOS...")        

        # Calculate area for new rows
        query = "Area_Ac IS NULL OR Area_Ha IS NULL"
        with arcpy.da.UpdateCursor(pos, ["SHAPE@", "Area_Ac", "Area_Ha"], query) as cur:
            for row in cur:
                geom = row[0]
                acres = geom.getArea("PLANAR", "AcresUS")
                row[1] = acres
                hectares = geom.getArea("PLANAR", "Hectares")
                row[2] = hectares
                cur.updateRow(row)
        arcpy.AddMessage("Calculated area...")

        # Delete new_rows
        arcpy.management.Delete("new_rows")
        arcpy.AddMessage("Deleted new_rows...")

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return

# This tool is a combination of the Landvest toolbox tools 02. Spatial Compare
# and 03. Group Parcels. The tools are combined and edited down to only produce
# the data relevant for new data (FC2, which contains groups 2a, 2b, and 6).
# This tool also corrects an issue with the Landvest Spatial Compare tool that
# resulted in polygons not being identifid as new data if their centroid (representative center)
# was in NEPOS but their actual "center" (inside the polygon) was not. This was resolved
# by changing the changing the way the spatial selection is done and making it done
# using points that are actually inside the polygons rather than HAVE_THEIR_CENTER_IN
class SpatialCompare(object):
	def __init__(self):
		# canned required attributes of the tool required by ESRI
		self.label = "Identify New Source Data"
		self.description = "Compare current singlepart POS layer with preprocessed singlepart OS data set to identify new polygons in source data. This tool produces a copy of the source data with groups 2a, 2b, and 6 identified for use in append_rows.py"
		# running in background creates problems, so we turn this off
		self.canRunInBackground = False
	def getParameterInfo(self):
		# create list to store parameters
		params = []

		# Singlepart NEPOS
		inputNEPOS = arcpy.Parameter(name="inputNEPOS",
			displayName="Current POS Layer",
			direction="Input",
			parameterType="Required")
		inputNEPOS.datatype = "GPFeatureLayer"
		params.append(inputNEPOS)

		# Singlepart, preprocessed source data layer
		inputFC2 = arcpy.Parameter(name="inputFC2",
			displayName="OS Layer to Compare",
			direction="Input",
			parameterType="Required")
		inputFC2.datatype = "GPFeatureLayer"
		params.append(inputFC2)

		# GDB where NEPOS and sources are found
		# Outputs will also be sent here
		outputGDB = arcpy.Parameter(name="outputGDB",
			displayName="GDB Export Workspace",
			direction="Input",
			parameterType="Required",
			datatype="DEWorkspace")
		params.append(outputGDB)

		return params

	def execute(self, parameters, messages):
		self.OSCompare(parameters[0].value, parameters[1].value, parameters[2].value, messages)
		return

	def OSCompare(self, arg1, arg2, arg3, messages):
		# Set workspace to be GDB provided in argument 3
		outputGDB = arg3
		arcpy.env.workspace = outputGDB

		# Copy the NEPOS and the source layer to FC1 and FC2, respectively
		# to avoid making changes directly in the data
		messages.addMessage("Copying NEPOS to FC1...")
		arcpy.CopyFeatures_management(arg1, "FC1")
		FC1 = "FC1"

		messages.addMessage("Copying new OS to FC2...")
		arcpy.management.CopyFeatures(arg2, "FC2")
		FC2 = "FC2"


		"""
		ADDING FIELDS
		"""
		# Add needed fields into FC1
		messages.addMessage("Adding new fields to FC1...")
		arcpy.management.AddField(FC1, "LV_ID", "TEXT", "", "", "25", "", "NULLABLE", "NON_REQUIRED", "")
		text_var = '"LV_"& [OBJECTID]'
		arcpy.CalculateField_management(FC1, "LV_ID", text_var, "VB")

		# Add needed fields into FC2
		messages.addMessage("Adding new fields to FC2")
		arcpy.management.AddField(FC2, "FC2_ID", "TEXT", "", "", "25", "", "NULLABLE", "NON_REQUIRED", "")
		arcpy.management.AddField(FC2, "FC2_Acres", "DOUBLE")
		arcpy.management.AddField(FC2, "NEPOSCentInFC2", "TEXT", "", "", "2", "", "NULLABLE", "NON_REQUIRED", "")
		arcpy.management.AddField(FC2, "FC2CentInNEPOS", "TEXT", field_length=1)
		arcpy.management.AddField(FC2, "RFactFC2", "DOUBLE")
		arcpy.management.AddField(FC2, "PctOvFC2NEPOS", "FLOAT")
		arcpy.management.AddField(FC2, "OvFC2NEPOS_Count", "DOUBLE")
		arcpy.management.AddField(FC2, "FC3_Acres", "DOUBLE")


		"""
		CALCULATING FIELDS
		"""
		# Populating the fields with needed information
		text_var = '"FC2_"& [OBJECTID]'
		arcpy.management.CalculateField(FC2, "FC2_ID", text_var, "VB")
		arcpy.management.CalculateField(FC2, "FC2_Acres", "\"!shape.area@ACRES!\"", "PYTHON_9.3", "")
		arcpy.management.CalculateField(FC2, "RFactFC2", """16*!{}!/(!{}!*!{}!)""".format("Shape_Area","Shape_Length","Shape_Length"), "PYTHON_9.3", "")

		# Make point layer of FC1 (NEPOS) and FC2 (source data)
		# It's very important that the points be made with INSIDE and not CENTROID
		# Previously, this code did not work properly because the select by location
		# below was done using two polygon layers with the HAVE_THEIR_CENTER_IN
		# match type - this did not work well for linear or oddly shaped features,
        # where the representative center would be inside NEPOS but in reality the polygon
        # did not overlap NEPOS.
		# By making point layers where we know the point is INSIDE the polygon
		# it is derived from, this should make the select by location work better.
		arcpy.management.FeatureToPoint(FC1, "FC1_pt", "INSIDE")
		arcpy.management.FeatureToPoint(FC2, "FC2_pt", "INSIDE")


		"""
		SPATIAL SELECTION OF FC2
		"""
		# Make feature layer of FC2 dataset
		messages.addMessage("Making feature layer...")
		arcpy.management.MakeFeatureLayer(FC2, 'FC2_lyr')

		# Select features based on location
		messages.addMessage("Trying to select based on location...")
		arcpy.management.SelectLayerByLocation('FC2_lyr', 'CONTAINS', 'FC1_pt')
		yFeatureCount = int(arcpy.management.GetCount('FC2_lyr').getOutput(0))
		messages.addMessage("Selected " + str(yFeatureCount) + " features")
		# Calculate "y" values for selected features
		messages.addMessage("Adding Y values...")
		arcpy.management.CalculateField('FC2_lyr',"NEPOSCentInFC2", "'Y'", "PYTHON")

		# Select features based on location
		messages.addMessage("Switching selection...")
		arcpy.management.SelectLayerByLocation('FC2_lyr', None, None, "", "SWITCH_SELECTION")
		nFeatureCount = int(arcpy.management.GetCount('FC2_lyr').getOutput(0))
		messages.addMessage("Selected " + str(nFeatureCount) + " features")

		# Calculate "y" values for selected features
		messages.addMessage("Adding N values...")
		arcpy.management.CalculateField('FC2_lyr',"NEPOSCentInFC2", "'N'", "PYTHON")

		# Move values back into original dataset
		messages.addMessage("Updating selected values.....")

		# Example of what we call a "joinless join" i.e. using a dictionary to hold key values and use them to join to another dataset
		joinFields = ['FC2_ID', 'NEPOSCentInFC2']

		self.JoinlessJoin('FC2_lyr', FC2, joinFields)

		messages.addMessage("Finished adding values back into FC2...")

		# Because we changed the selection above to not use the HAVE_THEIR_CENTER_IN option, which
		# doesn't work well for linear features, we now don't have the FC2CentInNEPOS field in FC2,
		# which is necessary for the grouping function to work properly. A solution to this
		# is to do another spatial selection and use JoinlessJoin to join the results of that to FC2.
		# We can do a selection on the FC2_pt layer because that has the FC2_ID field which we can use
		# to join back to the FC2 polygon layer that is saved in this tool
		messages.addMessage("Beginning selection and join for FC2...")
		arcpy.management.MakeFeatureLayer("FC2_pt", "FC2_pt_lyr")  # Need feature layers for selection
		arcpy.management.SelectLayerByLocation("FC2_pt_lyr", "WITHIN", FC1)
		arcpy.management.CalculateField("FC2_pt_lyr", "FC2CentInNEPOS", "'Y'")
		arcpy.management.SelectLayerByLocation("FC2_pt_lyr", selection_type="SWITCH_SELECTION")
		arcpy.management.CalculateField("FC2_pt_lyr", "FC2CentInNEPOS", "'N'")
		arcpy.management.SelectLayerByAttribute("FC2_pt_lyr", selection_type="CLEAR_SELECTION")

		# Join the FC2CentInNEPOS field from the pt lyr to main FC2 by FC2_ID
		join_fields = ['FC2_ID', 'FC2CentInNEPOS']
		self.JoinlessJoin('FC2_pt_lyr', FC2, join_fields)
		print("Joined FC2CentInNEPOS to FC2...")

		"""
		INTERSECTING AND CREATING FC3
		"""
		# At this point, the fields have been created and populated to the point that we are ready to do the intersect
		messages.addMessage("Intersecting FC1 and FC2, creating FC3...")
		inFeatures = ["FC1", "FC2"]
		arcpy.analysis.Intersect(inFeatures, "FC3")

		# messages.addMessage("Creating new field FC3_Acres in FC3...")
		arcpy.management.AddField("FC3", "FC3_Acres", "DOUBLE")

		# messages.addMessage("Calculating acreages in FC3...")
		arcpy.management.CalculateField("FC3", "FC3_Acres", "\"!shape.area@ACRES!\"", "PYTHON_9.3", "")


		"""
		SUMMARIZING FC3
		"""
		# Ready to do our summary within FC3 - summarizing on FC2_ID makes it so
        # that we have 
		messages.addMessage("Summarizing on FC2_ID in FC3...")
		stats = [["FC3_Acres", "SUM"]]
		caseField2 = "FC2_ID"
		arcpy.analysis.Statistics("FC3", "FC3_FC2_ID", stats, caseField2)
		messages.addMessage("Finished Summarizing on FC2_ID in FC3...")


		"""
		JOINING DATA FROM SUMMARY TABLES BACK INTO FC1 AND FC2
		"""
		messages.addMessage("Making table views for join...")
		arcpy.management.MakeTableView("FC3_FC2_ID", "FC3_FC2_ID_tableview")

		messages.addMessage("Making feature layers for join...")
		arcpy.management.Delete('FC2_lyr')
		arcpy.management.MakeFeatureLayer(FC2, 'FC2_lyr')

		#ready to join data from FC3 into FC2
		messages.addMessage("Joining info from FC3 back into FC2 feature layer...")
		arcpy.management.AddJoin('FC2_lyr',"FC2_ID", "FC3_FC2_ID_tableview", "FC2_ID")
		messages.addMessage("Calculating fields...")
		arcpy.management.CalculateField('FC2_lyr',"OvFC2NEPOS_Count", "!FC3_FC2_ID.FREQUENCY!", "PYTHON")
		arcpy.management.CalculateField('FC2_lyr',"FC3_Acres", "!FC3_FC2_ID.SUM_FC3_Acres!", "PYTHON")
		arcpy.management.RemoveJoin('FC2_lyr')


		"""
		CALCULATING TOTAL OVERLAP IN PCTOVERNEPOSFC2
		"""
		arcpy.management.CalculateField('FC2_lyr', "PctOvFC2NEPOS", """!{}!/!{}!*100""".format("FC3_Acres", "FC2_Acres"), "PYTHON_9.3", "")

		messages.addMessage("Saving feature layers as permanent data...")
		arcpy.management.CopyFeatures('FC2_lyr', 'FC2_JOIN')

		messages.addMessage("Moving on to grouping...")

		# Queries to identify groups within FC2_JOIN
		QUERY_GRP_2a = "(PctOvFC2NEPOS IS NULL and FC2CentInNEPOS = 'N') or (PctOvFC2NEPOS <0.25 and FC2CentInNEPOS = 'N')"
		QUERY_GRP_2b = "(PctOvFC2NEPOS >=0.25 and FC2CentInNEPOS = 'N') and (PctOvFC2NEPOS <10.0 and FC2CentInNEPOS = 'N')"
		QUERY_GRP_6 = "(PctOvFC2NEPOS>10) and (PctOvFC2NEPOS<60) and (FC2CentInNEPOS = 'N')"

		# CALCULATING FOR GROUP 2
		messages.addMessage("Starting work on FC2...")
		messages.addMessage("Creating feature layer of FC2 for the grouping...")
		arcpy.management.MakeFeatureLayer("FC2_JOIN", "FC2_GROUPED_lyr")
		messages.addMessage("Adding field to store Group 2 info to FC2...")
		arcpy.management.AddField("FC2_GROUPED_lyr", "GROUP_2a", "TEXT", "", "", "8", "", "NULLABLE", "NON_REQUIRED", "")
		arcpy.management.AddField("FC2_GROUPED_lyr", "GROUP_2b", "TEXT", "", "", "8", "", "NULLABLE", "NON_REQUIRED", "")
		messages.addMessage("Selecting features in Group 2...")
		arcpy.management.SelectLayerByAttribute("FC2_GROUPED_lyr", "NEW_SELECTION", QUERY_GRP_2a)
		messages.addMessage("Calculating field for selected records...")
		arcpy.CalculateField_management("FC2_GROUPED_lyr", "GROUP_2a", "'Y'", "PYTHON")
		arcpy.management.SelectLayerByAttribute("FC2_GROUPED_lyr", "NEW_SELECTION", QUERY_GRP_2b)
		arcpy.management.CalculateField("FC2_GROUPED_lyr", "GROUP_2b", "'Y'", "PYTHON")

		# CALCULATING FOR GROUP 6
		messages.addMessage("Adding field to store Group 6 info to FC2...")
		arcpy.management.AddField("FC2_GROUPED_lyr", "GROUP_6", "TEXT", "", "", "8", "", "NULLABLE", "NON_REQUIRED", "")
		messages.addMessage("Selecting features in Group 6...")
		arcpy.management.SelectLayerByAttribute("FC2_GROUPED_lyr", "NEW_SELECTION", QUERY_GRP_6)
		messages.addMessage("Calculating field for selected records...")
		arcpy.management.CalculateField("FC2_GROUPED_lyr", "GROUP_6", "'Y'", "PYTHON")

		messages.addMessage("Finished work on feature layer, saving as permanent data...")
		arcpy.management.SelectLayerByAttribute('FC2_GROUPED_lyr', "CLEAR_SELECTION")
		arcpy.management.CopyFeatures('FC2_GROUPED_lyr', 'FC2_GROUPED')

		messages.addMessage("Cleaning up intermediate layers...")
		arcpy.Delete_management('FC2_JOIN')
		arcpy.management.Delete('FC1')
		arcpy.management.Delete('FC2')
		arcpy.management.Delete('FC3')
		arcpy.management.Delete('FC3_FC2_ID')
		arcpy.management.Delete("FC1_pt")
		arcpy.management.Delete("FC2_pt")
		messages.addMessage("Done! Saved FC2_GROUPED to workspace GDB")

	# Note that the field you are joining must already exist in the data you are joining to!
	def JoinlessJoin (self, FC1, FC2, fields):
		#STEP 1: create a dictionary of values from the table to be joined
		joinDict = {}
		with arcpy.da.UpdateCursor(FC1, fields) as rows:
			for row in rows:
				joinVal = row[0]
				val = row[1]
				joinDict[joinVal] = [val]

		#STEP 2: join the values based on the key in the dictionary back into the other table
		with arcpy.da.UpdateCursor(FC2, fields) as rows:
			totalFeatureCount = int(arcpy.GetCount_management(FC2).getOutput(0))
			counter = 0
			for row in rows:
				keyval = row[0]
				if keyval in joinDict:
					row[1] = joinDict[keyval][0]
				else:
					pass
				rows.updateRow(row)
				counter += 1
				arcpy.SetProgressorLabel(str(counter) + " out of :" + str(totalFeatureCount))
