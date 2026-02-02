# Takes exported VT domain table for PAGENCY and
# preprocesses some of the values to conform with NEPOS formatting of names
#
# Prior to running this script you should have exported the domain
# for PAGENCY using the Domain to Table tool in ArcPro. There is also
# a function called export_domain() in recode_source_data.py that
# you can use.
#
# Lucy Lee, 9/2024

library(dplyr)
library(stringr)

# Folder containing CSV of exported domain
setwd('D:/Lee/POS/Update_2023/Data/CadastralConserved_PROTECTEDLND/')

##### PTYPE attributes codes #####
# These don't need any serious cleaning -- we'll do reclassification in Python along with other attributes
ptype <- read.csv('vt_pld_ptype.csv', stringsAsFactors = F)
ptype <- ptype[, -1]  # Remove the first column (junk added by ArcGIS)
colnames(ptype) <- c('PTYPE_CODE', 'PTYPE_DESC')

##### PAGENCY attribute codes #####
pagency <- read.csv('vt_pld_pagency.csv', stringsAsFactors = F)
pagency <- pagency[, -1]   # Remove the first column (junk added by ArcGIS)
colnames(pagency) <- c('PAGENCY_CODE', 'PAGENCY_DESC')
pagency$PAGENCY_DESC <- trimws(pagency$PAGENCY_DESC) # Remove leading white space in PAGENCY_DESC

# We want to do some formatting of public names
# Local names - as of 2024 there are only 10 cities in VT - we can use the first
# digits of codes to isolate local owners and names to append City of or Town of
# First, add padding so all codes are 5 digits (as they are in the domain)
pagency$PAGENCY_CODE <- str_pad(pagency$PAGENCY_CODE, 5, pad = 0)

# Create a column with the first digit to indicate group
# Municipalities begin with 0, 1, or 2
pagency$PAGENCY_GRP <- substr(pagency$PAGENCY_CODE, 1, 1)

# The 10 cities are: Burlington~, South Burlington~, Rutland~, Essex Junction, Barre~,
# Montpelier~, Winooski~, St, Albans~, Newport~, Vergennes~
# Source: https://sos.vermont.gov/vsara/learn/municipalities/vermont-cities/
# Newport City, St. Albans City, Rutland City, Barre City all have corresponding towns
# These cities and towns don't need to be changed.
# NOTE: City of Essex Junction was created in 2022. As of 2024, the most current VT PLD data
# is from 2021. Therefore City of Essex Junction is not yet included as a municipality.
# That will have to be checked and accounted for in future updates.
city_codes <- c('01100', '07015', '07070', '07090', '23055')    # Cities to add 'City of ' to
# UPDATE - Changing these to match the format of other cities/towns and other sources
no_changes <- c('Newport City', 'Newport Town', 'St. Albans City', 'St. Albans Town', 
                'Rutland City', 'Rutland Town', 'Barre City', 'Barre Town')  # Cities/towns needing no change
`%!in%` <- Negate(`%in%`)  # 'not in' operator

# Make edits to agency names, starting with specific cases then more general cases:
# - cities to add 'City of ' in front of name
# - remaining municipalities, excluding the no_changes cities & towns
# - federal agencies
# - state agencies
pagency$PAGENCY_DESC <- case_when(
  pagency$PAGENCY_CODE %in% city_codes ~ paste('City of', pagency$PAGENCY_DESC),
  pagency$PAGENCY_GRP %in% c('0', '1', '2') & pagency$PAGENCY_DESC %!in% no_changes ~ paste('Town of', pagency$PAGENCY_DESC),
  pagency$PAGENCY_DESC == 'Newport City' ~ 'City of Newport',
  pagency$PAGENCY_DESC == 'Newport Town' ~ 'Town of Newport',
  pagency$PAGENCY_DESC == 'St. Albans City' ~ 'City of St. Albans',
  pagency$PAGENCY_DESC == 'St. Albans Town' ~ 'Town of St. Albans',
  pagency$PAGENCY_DESC == 'Rutland City' ~ 'City of Rutland',
  pagency$PAGENCY_DESC == 'Rutland Town' ~ 'Town of Rutland',
  pagency$PAGENCY_DESC == 'Barre City' ~ 'City of Barre',
  pagency$PAGENCY_DESC == 'Barre Town' ~ 'Town of Barre',
  pagency$PAGENCY_DESC == 'US GOVERNMENT (General)' ~ 'United States of America',
  pagency$PAGENCY_DESC == 'US Dept. of Interior - Fish & Wildlife Service' ~ 'US DOI - Fish and Wildlife Service',
  pagency$PAGENCY_DESC == 'US Dept. of Interior - National Park Service' ~ 'US DOI - National Park Service',
  pagency$PAGENCY_DESC == 'US Dept. of Agriculture - Forest Service' ~ 'USDA - Forest Service',
  pagency$PAGENCY_DESC == 'Forest Legacy Program' ~ 'USDA - Forest Legacy Program',
  pagency$PAGENCY_DESC == 'US Dept. of Agriculture - Natural Resource Conservation Service (NRCS)' ~ 'USDA - Natural Resources Conservation Service',
  pagency$PAGENCY_DESC == 'US Dept. of Agriculture - Farm Services Agency (FSA)' ~ 'USDA - Farm Services Agency',
  pagency$PAGENCY_DESC == 'US Dept. of Defense - Army Corps of Engineers' ~ 'US DOD - Army Corps of Engineers',
  pagency$PAGENCY_DESC == 'US Dept. of Defense - Army National Guard' ~ 'US DOD - Army National Guard',
  pagency$PAGENCY_DESC == 'STATE OF VERMONT (General)' ~ 'State of Vermont',
  pagency$PAGENCY_DESC == 'VT  Division for Historical Preservation' ~ 'VT ACCD - Division for Historic Preservation',
  pagency$PAGENCY_DESC == 'VT Dept. Buildings and General Services' ~ 'VT AOA - Dept. of Buildings and General Services',
  .default = pagency$PAGENCY_DESC
)

# Save results
# IMPORTANT NOTE: These CSVs do not work properly in ArcPro - the code field gets interpreted
# as a number instead of string and padded zeroes are lost that are necessary in order to join
# to spatial data. There are function in the prep_vt() function in recode_source_data.py 
# to fix this!
todays_date <- Sys.Date()
write.csv(ptype, paste0('vt_pld_ptype_', todays_date, '.csv'), row.names = F)
write.csv(pagency[, -3], 'vt_pld_pagency_', todays_date, '.csv', row.names = F)  # Has updated towns/cities with same name
