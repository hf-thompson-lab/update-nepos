# Takes some of the sheets in NH AttributesCodes.xls and saves as CSV for use in ArcGIS Pro
# For some reason, the Excel file could not be used directly.
#
# This code takes the codes for PPAGENCY and SPAGENCY in the excel file
# and combines them into a single set of codes in a CSV with 2 columns
# that can be used for both PPAGENCY and SPAGENCY.
#
# This code also rewrites some of the PAGENCY descriptions to match the format used
# in NEPOS.
#
# Lucy Lee, 7/2024

library(readxl)
library(dplyr)

# Folder where NH data including the AttributeCodes.xls spreadsheet live
setwd('D:/Lee/POS/Update_2023/Data/New_Hampshire_Conservation_Public_Lands/')

# Read in the sheets for PPAGENCY and SPAGENCY
ppagency <-readxl:: read_xls('AttributeCodes.xls', sheet='PPAGENCY')
spagency <- readxl::read_xls('AttributeCodes.xls', sheet='SPAGENCY')

# Set column names
colnames(ppagency) <- c('CODE', "PPAGENCY_DESC")
colnames(spagency) <- c('CODE', "SPAGENCY_DESC")

# Join the PPAGENCY and SPAGENCY data together by CODE
z <- dplyr::full_join(ppagency, spagency, by = 'CODE')

# There are a few codes in each that are not in the other
# Going to combine them into one by moving SPAGENCY_DESC into NA cells of PPAGENCY_DESC
z[is.na(z$PPAGENCY_DESC), 'PPAGENCY_DESC'] <- z[is.na(z$PPAGENCY_DESC), 'SPAGENCY_DESC']

# Then we can drop SPAGENCY_DESC, rename columns, and save
z <- z[, c('CODE', 'PPAGENCY_DESC')]
colnames(z) <- c('CODE', 'DESC')

# Format some names to better match the NEPOS schema
z$DESC <- dplyr::case_match(z$DESC,
                     "US Dept. of Interior, Fish & Wildlife Service" ~ "US DOI - Fish and Wildlife Service",
                     "US Dept. of Interior, National Park Service" ~ "US DOI - National Park Service",
                     "US Dept. of Interior, National Park Service A/T" ~ "US DOI - National Park Service",
                     "US Dept. of Agriculture, Forest Service" ~ "USDA - Forest Service",
                     "US Dept. of Agriculture, Natural Resources Conservation Service" ~ "USDA - Natural Resources Conservation Service",
                     "US Dept. of Defense, Army Corps of Engineers" ~ "US DOD - Army Cops of Engineers",
                     "US Dept. of Defense, US Air Force" ~ "US DOD - Air Force",
                     "NH Office of State Planning" ~ "NH DBEA - Office of State Planning and Development",
                     "NH Dept. of Resources & Economic Dev.  (DRED)" ~ "NH Department of Resources and Economic Development",
                     "NH Fish & Game" ~ "NH Fish and Game Department",
                     "NH Dept. of Agriculture" ~ "NH Department of Agriculture",
                     "NH University of New Hampshire (Durham)" ~ "University of New Hampshire (Durham)",
                     "NH Plymouth State College" ~ "Plymouth State College",
                     "NH Keene State College" ~ "Keene State College",
                     "NH Dept. of Environmental Services (DES)" ~ "NH Department of Environmental Services",
                     "NH DES, Water Resources Division" ~ "NH DES - Water Resources Division",
                     "NH Dept. of Transportation" ~ "NH Department of Transportation",
                     "NH Division of Historic Resources" ~ "NH DNCR - Division of Historical Resources",
                     "NH Dept. of Corrections" ~ "NH Department of Corrections",
                     "NH Health and Human Services" ~ "NH Department of Health and Human Services",
                     "US Government" ~ "United States of America",
                     .default = z$DESC)

write.csv(z, 'ppagency_spagency_names.csv', row.names=F)
