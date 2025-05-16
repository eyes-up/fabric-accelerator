# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2987f99a-68e4-43af-9366-01eaebfbe67b",
# META       "default_lakehouse_name": "RogueHire_LH",
# META       "default_lakehouse_workspace_id": "9b76398b-86b2-4e3f-af4b-84c47768a465"
# META     }
# META   }
# META }

# CELL ********************

%run /commonTransforms

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run /DeltaLakeFunctions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Notebook Parameters

# PARAMETERS CELL ********************

L1TransformInstanceID = None
L1TransformID = None
IngestID = None
CustomParameters = None
InputRawFileSystem = None
InputRawFileFolder = None
InputRawFile = None
InputRawFileDelimiter = None
InputFileHeaderFlag = None
OutputL1CurateFileSystem = None
OutputL1CuratedFolder = None
OutputL1CuratedFile = None
OutputL1CuratedFileDelimiter = None
OutputL1CuratedFileFormat = None
OutputL1CuratedFileWriteMode = None
OutputDWStagingTable = None
LookupColumns = None
OutputDWTable = None
OutputDWTableWriteMode = None
ReRunL1TransformFlag = None
WatermarkColName = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Parameters for Testing only, should be commented off
# L1TransformInstanceID = 2
# L1TransformID = 21
# IngestID = 21
# CustomParameters = None
# InputRawFileSystem = 'Files'
# InputRawFileFolder = 'bronze/se38/dbo/vw_Organizations/2025-05'
# InputRawFile = 'dbo_vw_Organizations_2025-05-14_000000.parquet'
# InputRawFileDelimiter = None
# InputFileHeaderFlag = None
# OutputL1CurateFileSystem = None
# OutputL1CuratedFolder = None
# OutputL1CuratedFile = None
# OutputL1CuratedFileDelimiter = None
# OutputL1CuratedFileFormat = None
# OutputL1CuratedFileWriteMode = None
# OutputDWStagingTable = None
# LookupColumns = 'company_id'
# OutputDWTable = 'silver.dbo_vw_Organizations'
# OutputDWTableWriteMode = 'append'
# ReRunL1TransformFlag = None
# WatermarkColName = 'LastEditedWhen'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read, cleanse and enrich raw/bronze data

# CELL ********************

df = readFile('bronze',InputRawFileSystem,InputRawFileFolder,InputRawFile)
ingestCount = df.count()

ct=CommonTransforms(df)

# Remove duplicates
if LookupColumns is not None:
    df=ct.deDuplicate(LookupColumns.split('|'))
else:
    df=ct.deDuplicate()

# Remove leading and trailing spaces from all string columns
df=ct.trim()

# # Replace Null Value with generic values
df = ct.replaceNull(0)
df = ct.replaceNull("NA")
df = ct.replaceNull("2020-01-01")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Load standardized/silver data

# CELL ********************

if OutputDWTableWriteMode == 'append' and LookupColumns is not None and ingestCount>0:
    output = upsertDelta(df,OutputDWTable,LookupColumns,WatermarkColName)
    numSourceRows = output["numSourceRows"]
    numTargetRowsInserted = output["numTargetRowsInserted"]
    numTargetRowsUpdated = output["numTargetRowsUpdated"]
    numTargetRowsDeleted = output["numTargetRowsDeleted"]
elif OutputDWTableWriteMode == 'overwrite' and ingestCount>0:
    output = insertDelta (df, OutputDWTable, OutputDWTableWriteMode)
    numSourceRows = ingestCount
    numTargetRowsInserted = output["numOutputRows"]
    numTargetRowsUpdated ="0"
    numTargetRowsDeleted ="0"
else:
    numSourceRows = ingestCount
    numTargetRowsInserted = "0"
    numTargetRowsUpdated ="0"
    numTargetRowsDeleted ="0"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Return Values (to pipeline)

# CELL ********************

import json
notebookutils.notebook.exit(json.dumps({
  "numSourceRows": numSourceRows,
  "numTargetRowsInserted": numTargetRowsInserted,
  "numTargetRowsUpdated": numTargetRowsUpdated,
  "numTargetRowsDeleted": numTargetRowsDeleted
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
