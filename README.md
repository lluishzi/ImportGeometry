# ImportGeometry
Script to import geometry from any QGIS project to postgres layers from the toolbox.

## Installation

Simply run the tool from 

## Usage Overview

This tool is designed to automate the process of:
- Creating tables in a PostGIS database based on QGIS vector layers.
- Ingesting features from selected layers into those tables.
- Optionally remapping project layer sources to the new database locations.
- Managing permissions and metadata for the exported data.

---

## Workflow & Actions Performed

### 1. **Parameter Collection**
   - The tool collects user input for database connection, schema, permissions, layers to export, and other options.   
   
### 2. **Phase 1: Table Creation**
   - If enabled, the tool generates SQL scripts to create tables in the specified schema, matching the structure of the selected layers.
   - Optionally, it can execute the generated SQL directly in the database.

### 3. **Phase 2: Data Ingestion**
   - If enabled, the tool exports the data from the selected layers into the corresponding PostGIS tables.
   - It can optionally delete existing data in the target tables before importing.

### 4. **Phase 3: Project Remapping**
   - If enabled, the tool updates the QGIS project so that layers now point to the new PostGIS sources instead of their original sources.

### 5. **Logging and Output**
   - The tool logs all actions, generates SQL and structure files, and can store metadata about the operation in a database table.

---

## Parameters and Their Utility

| Parameter Name         | Description & Utility                                                                                 |
|----------------------- |------------------------------------------------------------------------------------------------------|
| **SCHEMA**             | Destination schema in the PostGIS database for the new tables. Attention , the user connecting to postgres specified in **QGIS database connection name** must have the necessary permissions to create tables in the specified schema.                                       |
| **PERMISSIONS**        | Comma-separated list of permissions to grant on the new tables in the format of [[a|s]:<username>,*] , the **a** permission is for all, the **s** permission is for select (e.g., `a:username` for ALL).         |
| **SRID**               | Spatial Reference System Identifier for the output tables (e.g., 25831, 4326).                       |
| **LAYER_LIST**         | List of QGIS vector layers to export to PostGIS.                                                     |
| **DEST_FOLDER**        | Folder where output files (SQL scripts, structure info) will be saved, it can be a temporary folder and must be writable.                               |
| **CNX_DEST**           | The QGIS database connection name for the target PostGIS database where the new tables will be created and features ingested.                                   |
| **TABLE_INFO**         | (Optional) Name of a table in the database to store metadata about the export operation. This name should include the schema (e.g., `public.export_metadata`). If this doesn't exist, it will be created.             |
| **PROC_1**             | Boolean: If true, generate SQL for table creation (Phase 1).                                         |
| **RUNDB_PROC_1**       | Boolean: If true, execute the generated SQL for table creation in the database.                      |
| **PROC_2**             | Boolean: If true, ingest data from layers into the new tables (Phase 2).                             |
| **IDINFO**             | (Optional) Identifier for a record in the metadata table, used for retrieving the information of the export operation.  |
| **DELEXISTINGDATA**    | Boolean: If true, delete existing data in the target tables before importing new data.               |
| **PROC_3**             | Boolean: If true, remap the QGIS project layers to point to the new PostGIS sources (Phase 3).       |

---

## Typical Steps for a User

1. **Select the layers** you want to export.
2. **Choose the target schema** and database connection.
3. **Set permissions** if needed.
4. **Enable Phase 1** to generate and/or run table creation SQL.
5. **Enable Phase 2** to ingest data.
6. **Enable Phase 3** to update the project to use the new database sources.
7. **Run the tool**. Output files and logs will be saved in the specified folder.
