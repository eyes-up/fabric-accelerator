# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Environment Variables
# Use this notebook to set environment specific variables that can be referenced in other spark notebooks. These variables need to set once in each environment

# CELL ********************

# Fabric Workspace ID for the Bronze medallion layer
bronzeWorkspaceId = "6db55d91-3d30-4353-bbeb-0dac1badad8b"

# Bronze Lakehouse name. Set to None if not applicable.
bronzeLakehouseName = "RogueHire_LH"

# Bronze data warehouse name. Set to None if not applicable.
bronzeDatawarehouseName = None

# Fabric Workspace ID for the Silver medallion layer. Use the same ID if all medallion layers are in the same workspace.
silverWorkspaceId = "6db55d91-3d30-4353-bbeb-0dac1badad8b"

# Silver Lakehouse name. Set to None if not applicable.
silverLakehouseName = "RogueHire_LH"

# Silver data warehouse name. Set to None if not applicable.
silverDatawarehouseName = None

# Fabric Workspace ID for the Gold medallion layer. Use the same ID if all medallion layers are in the same workspace.
goldWorkspaceId = "6db55d91-3d30-4353-bbeb-0dac1badad8b"

# Gold Lakehouse name. Set to None if not applicable.
goldLakehouseName = None

# Gold data warehouse name. Set to None if not applicable.
goldDatawarehouseName = "RogueHireDW"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
