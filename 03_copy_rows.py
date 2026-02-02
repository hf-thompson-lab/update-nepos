# Copies rows to DBF
# This script needs to be run before running 04_find_matching_polygons.R, which
# relies on a DBF of NEPOS to function properly.
#
# Set the workspace to be the GDB where all the preprocessed, singlepart data is,
# which should be the same place where the 02_spatial_matching.py outputs were saved.
#
# Update the out_file path to be the folder were non-Esri files are saved.
#
# Lucy Lee, 12/2025

import arcpy

arcpy.env.workspace = "D:/Lee/POS/Update_2023/Data/new_data2.gdb/"
arcpy.env.overwriteOutput = True

pos = "POS_v2_29_sp"

#arcpy.management.CalculateGeometryAttributes(pos, [["Area_Ac", "AREA"]], area_unit="ACRES_US")
#arcpy.management.CalculateGeometryAttributes(pos, [["Area_Ha", "AREA"]], area_unit="HECTARES")

out_file = f"D:/Lee/POS/Update_2023/Data/matching/{pos}.dbf"

arcpy.management.CopyRows(pos, out_file)
print("Done")