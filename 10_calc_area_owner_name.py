# Populates the Area_Owner_Name field
# which is meant to serve as way that summarize data at the coarsest level
# e.g., ignoring differences in GapStatus, PubAccess, ProtType, etc.
#
# This code does a basic calculation and then uses an UpdateCursor
# to correct any areas that are still too specific. You can
# summarize the Area_Owner_Name field after it is calculated
# and see if there are additional areas to add to the corrections section.
#
# Lucy Lee, 12/2025

import time
start_time = time.time()
import arcpy

# GDB where NEPOS lives
arcpy.env.workspace = "D:/Lee/POS/Update_2023/Data/new_data2.gdb/"

# Latest singlepart NEPOS
pos = "POS_v2_29_sp"

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

##### CALCULATE BASE FIELD #####
# Calculate the field first as AreaName -- FeeOwner
# The double dash is because this doesn't exist in any of the two fields alone
# So it is a unique set of characters separating area name from owner name
arcpy.management.CalculateField(pos, "Area_Owner_Name", "!AreaName!' -- '!FeeOwner!")

##### CORRECTION LISTS #####
# There are many cases where the default calculation is more specific than
# we want this field to be. In these cases, we make a list for each
# area/owner name and populate it with any existing AreaName -- FeeOwner
# text that is already in the Area_Owner_Name field. It is useful to
# summarize the Area_Owner_Name field and sort it alphabetically
# so you can scan through and see if there are any areas that should
# be incorporated into this code. The lists below only need to contain
# the values that need to be corrected to a more general value (if any that
# are already correct are in NEPOS you don't need to add them here).

# Acadia NP - NPS
acadia_nps = ["Acadia National Park easement -- US DOI - National Park Service"]

# Airline SP - CT DEEP
airline_sp_trail = ["Airline State Park Trail (North Trail) -- CT Department of Energy and Environmental Protection",
                    "Airline State Park Trail (Northern Section) -- CT Department of Energy and Environmental Protection", 
                    "Airline State Park Trail (Southern Section) -- CT Department of Energy and Environmental Protection"]

# Algonquin SF - CT DEEP
algonquin_deep = ["Algonquin State Forest (Old Forest Management Area) -- CT Department of Energy and Environmental Protection"]

# Appalachian Trail - NPS
at_nps = [" AT - NPS Lands in CT -- US DOI - National Park Service",
          "Appalachian National Scenic Trail -- US DOI - National Park Service",
          "Appalachian Trail Corridor -- US DOI - National Park Service",
          "Appalachian Trail Tract 161-01 -- US DOI - National Park Service",
          "Appalachian Trail Tract 164-03 -- US DOI - National Park Service",
          "Appalachian Trail Tract 164-05 -- US DOI - National Park Service",
          "Appalachian Trail Tract 191-08 -- US DOI - National Park Service",
          "Appalachian Trail Tract 191-11 -- US DOI - National Park Service",
          "Appalachian Trail Tract 194-02 -- US DOI - National Park Service",
          "Appalachian Trail Tract 194-07 -- US DOI - National Park Service",
          "Appalachian Trail Tract 194-12 -- US DOI - National Park Service",
          "Appalachian Trail Tract 194-13 -- US DOI - National Park Service",
          "Appalachian Trail Tract 194-26 -- US DOI - National Park Service",
          "Appalachian Trail Tract 195-21 -- US DOI - National Park Service",
          "Appalachian Trail Tract 196-01 -- US DOI - National Park Service",
          "Appalachian Trail Tract 196-10 -- US DOI - National Park Service",
          "Appalachian Trail Tracts -- US DOI - National Park Service",
          "Appalachian Trail Tracts 193-(2,5,14,15) -- US DOI - National Park Service",
          "Appalachian Trail Tracts 196-07 + 196-09 -- US DOI - National Park Service"]

# Appalachian Trail - Private
at_private = ["Appalachian National Scenic Trail -- Private",
              "Appalachian Trail Corridor Easement -- Private",
              "Appalachian Trail Easement -- Private",
              "Appalachian Trail Tract 192-19 -- Private",
              "Appalachian Trail Tract 193-03 -- Private",
              "Appalachian Trail Tract 193-07 -- Private",
              "Appalachian Trail Tract 194-06 -- Private",
              "Appalachian Trail Tract 194-11 -- Private",
              "Appalachian Trail Tract 195-14 -- Private",
              "Appalachian Trail Tract 195-19 -- Private",
              "Appalachian Trail Tract 196-02 -- Private",
              "Appalachian Trail Tract 196-03 -- Private",
              "Appalachian Trail Tract 196-08 -- Private",
              "Appalachian Trail Tract 196-12 -- Private",
              "Appalachian Trail Tracts -- Private"]

# Appalachian Trail - USFS
at_usfs = ["Appalachian Trail Corridor -- USDA - Forest Service",
           "Appalachian Trail Tract 194-08 -- USDA - Forest Service",
           "Appalachian Trail Tract 194-09 -- USDA - Forest Service",
           "Appalachian Trail Tract 195-08 -- USDA - Forest Service"]

# Appalachian Trail - ME DACF
at_medacf = ["Appalachian Trail Corridor -- ME DACF - Bureau of Parks and Lands"]

# Bellamy River WMA - NH F&G
bellamy_nhfg = ["Bellamy River WMA - West - Lot 1 -- NH Fish and Game Department",
                "Bellamy River WMA - West - Lot 2 -- NH Fish and Game Department",
                "Bellamy River WMA - West - Lot 3 -- NH Fish and Game Department",
                "Bellamy River WMA - West - Lot 4 -- NH Fish and Game Department",
                "Bellamy River WMA - West - Lot 5 -- NH Fish and Game Department",
                "Bellamy River WMA - West - Lot 6 -- NH Fish and Game Department"]

# Binney Wilderness Preserve - NEWT
binney_newt = ["Binney Hill Wilderness Preserve - Sawtelle Addition -- Northeast Wilderness Trust",
               "Binney Hill Wilderness Preserve - Steel Addition -- Northeast Wilderness Trust"]

# Biscuit City - SKLT
biscuit_city_sklt = ["Biscuit City / Benjamin -- South Kingstown Land Trust",
                     "Biscuit City / Hoffman -- South Kingstown Land Trust",
                     "Biscuit City / KIA -- South Kingstown Land Trust",
                     "Biscuit City / Pratt -- South Kingstown Land Trust",
                     "Biscuit City / Wiener -- South Kingstown Land Trust"]

# Bow Town Forest - Town of Bow
bow_town_forest = ["Bow Town Forest - Lot 2-122 -- Town of Bow",
                   "Bow Town Forest - Lot 2-83 -- Town of Bow",
                   "Bow Town Forest - Lot 2-97 -- Town of Bow",
                   "Bow Town Forest - Lot 3-138 -- Town of Bow",
                   "Bow Town Forest - Lot 3-63 -- Town of Bow",
                   "Bow Town Forest - Lot 4-26+27 -- Town of Bow",
                   "Bow Town Forest - Lot 4-56 -- Town of Bow",
                   "Bow Town Forest - Lot 5-64 -- Town of Bow",
                   "Bow Town Forest - Turnpike Lots -- Town of Bow"]

# Orbeton Stream - Wagner Timber Partners LLC
orbeton_stream = ["Orbeton Stream (Gravel Extraction Zone) -- Wagner Timber Partners LLC"]

# WMNF - USFS
wmnf_usfs = ["White Mountain National Forest - Wilderness Area -- USDA - Forest Service",
             "White Mountain National Forest ; Caribou - Speckled Ext Roadless Area -- USDA - Forest Service",
             "White Mountain National Forest ; Carr Mountain Roadless Area -- USDA - Forest Service",
             "White Mountain National Forest ; Cherry Mountain Roadless Area -- USDA - Forest Service",
             "White Mountain National Forest ; Dartmouth Range Roadless Area -- USDA - Forest Service",
             "White Mountain National Forest ; Great Gulf Ext. Roadless Area -- USDA - Forest Service",
             "White Mountain National Forest ; Jobildunk Roadless Area -- USDA - Forest Service",
             "White Mountain National Forest ; Kearsarge Roadless Area -- USDA - Forest Service",
             "White Mountain National Forest ; Kilkenny Roadless Area -- USDA - Forest Service",
             "White Mountain National Forest ; Kinsman Mountain Roadless Area -- USDA - Forest Service",
             "White Mountain National Forest ; Mt. Wolf - Gordon Pond Roadless Area -- USDA - Forest Service",
             "White Mountain National Forest ; Pemigewasset Ext Roadless Area -- USDA - Forest Service",
             "White Mountain National Forest ; Pemigewasset Roadless Area -- USDA - Forest Service",
             "White Mountain National Forest ; Presidential - Dry River Ext Roadless Area -- USDA - Forest Service",
             "White Mountain National Forest ; Sandwich Range Roadless Area -- USDA - Forest Service",
             "White Mountain National Forest ; Waterville Roadless Area -- USDA - Forest Service",
             "White Mountain National Forest ; Wild River Roadless Area -- USDA - Forest Service"]

##### UPDATE ROWS USING CORRECTION LISTS #####
with arcpy.da.UpdateCursor(pos, "Area_Owner_Name") as cur:
    for row in cur:
        if row[0] in acadia_nps:
            row[0] = "Acadia National Park -- US DOI - National Park Service"
        elif row[0] in airline_sp_trail:
            row[0] = "Airline State Park Trail -- CT Department of Energy and Environmental Protection"
        elif row[0] in algonquin_deep:
            row[0] = "Algonquin State Forest -- CT Department of Energy and Environmental Protection"
        elif row[0] in at_nps:
            row[0] = "Appalachian Trail -- US DOI - National Park Service"
        elif row[0] in at_private:
            row[0] = "Appalachian Trail -- Private"
        elif row[0] in at_usfs:
            row[0] = "Appalachian Trail -- USDA - Forest Service"
        elif row[0] in at_medacf:
            row[0] = "Appalachian Trail -- ME DACF - Bureau of Parks and Lands"
        elif row[0] in bellamy_nhfg:
            row[0] = "Bellamy River WMA -- NH Fish and Game Department"
        elif row[0] in binney_newt:
            row[0] = "Binney Hill Preserve -- Northeast Wilderness Trust"
        elif row[0] in biscuit_city_sklt:
            row[0] = "Biscuit City -- South Kingstown Land Trust"
        elif row[0] in bow_town_forest:
            row[0] = "Bow Town Forest -- Town of Bow"
        elif row[0] in orbeton_stream:
            row[0] = "Orbeton Stream -- Wagner Timber Partners LLC"
        elif row[0] in wmnf_usfs:
            row[0] = "White Mountain National Forest -- USDA - Forest Service"
        cur.updateRow(row)

print_elapsed_time()