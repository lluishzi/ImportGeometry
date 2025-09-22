import os
import json
import  datetime
from qgis.core import (
    QgsProcessingAlgorithm,
    QgsProcessingParameterString,
    QgsProcessingParameterEnum,
    QgsProcessingParameterFeatureSource,
    QgsProcessingParameterFeatureSink,
    QgsProcessingParameterDatabaseSchema,
    QgsProcessingParameterFolderDestination,
    QgsProcessingParameterMultipleLayers,
    QgsProcessingParameterProviderConnection,
    QgsProcessingContext,
    QgsProcessingParameterDatabaseSchema,
    QgsProcessingOutputLayerDefinition,
    QgsFeatureSink,
    QgsFeature,
    QgsProviderRegistry,
    QgsProcessingFeedback,
    QgsProcessingParameterBoolean,
    QgsProcessingParameterNumber,
    QgsAuthMethodConfig,
    QgsApplication

)
import re
import subprocess
import unicodedata


mapeigCamps={
    "string":"text",
    "varchar":"text",
    "char":"text",
    "integer":"int",
    "int4":"int",    
    "date":"timestamp",
    "real":"float",
    "integer64":"int",
    "boolean":"boolean",
    "int":"int",
    "text":"text",
    "double":"float",
    "datetime":"timestamp",
    "integer64":"int",
    "bool":"boolean",
    "json":"jsonb",
}
mapeigTipusGeom={
    "linestring":"LINESTRING",
    "linestring25d":"LINESTRING",
    "linestringm":"LINESTRING",
    "linestringz":"LINESTRING",
    "linestringzm":"LINESTRING",
    "multilinestring":"MULTILINESTRING",
    "multilinestring25d":"MULTILINESTRING",
    "multilinestringm":"MULTILINESTRING",
    "multilinestringz":"MULTILINESTRING",
    "multilinestringzm":"MULTILINESTRING",
    "multipoint":"MULTIPOINT",
    "multipoint25d":"MULTIPOINT",
    "multipointm":"MULTIPOINT",
    "multipointz":"MULTIPOINT",
    "multipointzm":"MULTIPOINT",
    "multipolygon":"MULTIPOLYGON",
    "multipolygon25d":"MULTIPOLYGON",
    "multipolygonm":"MULTIPOLYGON",
    "multipolygonz":"MULTIPOLYGON",
    "multipolygonzm":"MULTIPOLYGON",
    "polygon":"POLYGON",
    "polygon25d":"POLYGON",
    "polygongeometry":"POLYGON",
    "polygonm":"POLYGON",
    "polygonz":"POLYGON",
    "polygonzm":"POLYGON",
    "point":"POINT",
    "point25d":"POINT",
    "pointgeometry":"POINT",
    "pointm":"POINT",
    "pointz":"POINT",
    "pointzm":"POINT",
    "nogeometry":"NOGEOM",
}
mapaCamps={
    "begin":"inici",
    "end":"final",
    "name":"nom",
    "timestamp":"datav",
    "between":"entre",
}


def strip_accents(s):
   return ''.join(c for c in unicodedata.normalize('NFD', s)
                  if unicodedata.category(c) != 'Mn')
def espmin(c):
    #c=re.sub(r'[^\x00-\x7F]+','_', c)
    c=c.lower().strip().replace(" ","_")
    c=strip_accents(c)
    c=re.sub(r'([^a-z\_0-9])','_', c)
    if c[0] in ['0','1','2','3','4','5','6','7','8','9']:
        c='c'+c
    c=re.sub(r'_+','_',c)
    c=re.sub(r'_+$','',c)
    if c[0]=='_':
        c=c[1:]
    return c
#treu corxets i noms de taula amb punt dels camps
def agafaNomCamp(c):
    c=c.strip()
    p1=re.compile(r'\[?[^\]]+\]?\.\[?(.*)\]?').match(c)
    if p1:
        pa=p1[1]
        if pa[:-1]==']':
            return pa[:-1]
        return pa    
    return c.replace('[','').replace(']','')

def warxiu(arx,cont,tob="w"):
    with open(arx,tob, encoding="utf-8") as f:
        f.write(cont)

def rarxiu(arx,tob="r"):
    with open(arx,tob, encoding="utf-8") as f:
        return f.read()        

def execcomOld(coma):
    try:
        data = subprocess.check_output(coma, shell=False,startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=3), text=True, stderr=subprocess.STDOUT)
        exitcode = 0
    except subprocess.CalledProcessError as ex:
        data = ex.output
        exitcode = ex.returncode
    return exitcode, data
def execcom(coma,print):
    try:
        data=""
        popen = subprocess.Popen(coma, stdout=subprocess.PIPE, shell=True,text=True)
        for line in popen.stdout: 
            print(line)            
            data=data+line+os.linesep
        popen.stdout.close()
        return_code = popen.wait()
        return return_code, data
    except subprocess.CalledProcessError as ex:
        data = ex.output
        exitcode = ex.returncode
        return exitcode, data

def sanitize_connection_string(conn_string):
    """Sanitize a connection string by removing sensitive information"""
    if not conn_string:
        return conn_string
    # Replace password parameter in connection strings
    sanitized = re.sub(r'password\s*=\s*[^\s]+', 'password=*****', conn_string)
    # Replace other potentially sensitive information
    sanitized = re.sub(r'pwd\s*=\s*[^\s]+', 'pwd=*****', sanitized)
    return sanitized


def conversioCaracters(origen:str,desti:str,codorigen:str='cp1252',coddesti:str='utf-8'):
    print(f'Converting [{origen}] from encoding [{codorigen}] to [{desti}] with encoding {coddesti}')
    with open(origen, 'r', encoding=codorigen) as inp,\
        open(desti, 'w', encoding=coddesti) as outp:
        for line in inp:
            outp.write(line)

def ecad(cadena):
    if cadena is None:
        return "''"
    cad=cadena.replace("'", "''")
    return f"'{cad}'"            

class DictObj:
    def __init__(self, in_dict:dict):
        assert isinstance(in_dict, dict)
        for key, val in in_dict.items():
            if isinstance(val, (list, tuple)):
               setattr(self, key, [DictObj(x) if isinstance(x, dict) else x for x in val])
            else:
               setattr(self, key, DictObj(val) if isinstance(val, dict) else val)
    def __getattr__(self, name):
        return None
               

class IteradorCapesProjecte:
    def __init__(self, lcapes,elmaplayer,partnom=None):
        self.lcapes= lcapes
        self.n=-1        
        self.maplay=elmaplayer
        self.capes={ele.name():ele for ele in lcapes} if lcapes else {}
        self.partnom=partnom
    def cercaNoms2(self,base,partn,dc,lnum,n,lp):
        from qgis.core import (QgsLayerTreeLayer,QgsMapLayer)
        lloc=partn.copy()
        lnut=lnum.copy()
        i=0
        if len(base.name())>0 :
            lloc.append(base.name())
            lnut.append('{:0>3}'.format(n))
        for f in base.children():        
            #abans hi havia f.layer().isValid() and 
            if isinstance(f,QgsLayerTreeLayer) and f.layer().type() == QgsMapLayer.VectorLayer :
                if  f.layer().name() in self.capes:
                    i+=1
                    lp.append(f.layer())
                    source=f.layer().dataProvider().dataSourceUri()
                    if source in dc:
                        scom=dc[source]["coment"]
                        print(f"Tree layer {f.layerId()} has the same source as {scom}, it will not be imported ","warning") 
                    else:
                        dc[source]={"treeid":f.layerId(), "coment":' > '.join(lloc) + f": {f.name()}","num":'_'.join(lnut) +"_"+ '{:0>2}'.format(i),"vis":f.isVisible()}
                        dc[source]["dadorigen"]=f.layer().dataProvider().dataSourceUri() 
                else:
                    print(f"Layer {f.layer().name()} will not be imported because it is not selected","warning")
            else:            
                n+=100
                self.cercaNoms2(f,lloc,dc,lnut,n,lp)    

    def __iter__(self):
        from qgis.core import (QgsProject)
        self.dcdesc={}
        root = QgsProject.instance().layerTreeRoot()        
        self.lf=list()
        self.cercaNoms2(root,list(),self.dcdesc,list(),0,self.lf)
        def pel(e):
            a=e[1]
            a["idel"]=e[0]
            return a        
        if self.maplay:
            self.lf=self.lcapes
        return self

    def __next__(self): # Python 2: def next(self)
        from qgis.core import (QgsMapLayer)
        self.n+=1
        if self.n>=len(self.lf):
            raise StopIteration    
        l=self.lf[self.n]        
        if not l.isValid():
            print(f"Invalid source layer: {l.name()}","error")
            return self.__next__()
        if not l.type() == QgsMapLayer.VectorLayer:
            print(f"Layer {l.name()} is not a vector type and will not be imported","warning")
            return self.__next__()
        r=dict(pth=l.source(),capa=l,ncapa=l.name(),coment='')
        source=l.dataProvider().dataSourceUri()
        if source in self.dcdesc:
            r["coment"]=self.dcdesc[source]["coment"]
            r["dadorigen"]=self.dcdesc[source]["dadorigen"]
            r["vis"]=self.dcdesc[source]["vis"]
        if self.partnom!=None:
            r["ncapa"]=f"{self.partnom}{self.dcdesc[source]['num']}"        
        return r

def GetConectionString(conectstring:str)->str:
    # Split the connection string into key=value pairs
    params = {}
    # Use regex to handle quoted values with spaces
    matches = re.findall(r"(\w+)=('.*?'|\".*?\"|\S+)", conectstring)
    for key, value in matches:
        # Remove quotes if present
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        elif value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        key = key.strip()
        value = value.strip()
        params[key] = value
    if "authcfg" in params:
        auth_mgr = QgsApplication.authManager()
        auth_cfg = QgsAuthMethodConfig()
        auth_mgr.loadAuthenticationConfig(params["authcfg"], auth_cfg, True)
        params.update(auth_cfg.configMap())
        if "username" in params:
            params["user"] = params["username"]
            del params["username"]
        del params["authcfg"]
    # Generate connection string from params dictionary
    conn_str = ' '.join([f"{k}={v}" for k, v in params.items() if v])
    return conn_str

if False:
#    debugpy.listen(("localhost", 5678))
    print("Waiting for VS Code debugger on port 5678...")
#    debugpy.wait_for_client()   # L'script queda aturat fins que t'hi connectes
    

class ChConPg(QgsProcessingAlgorithm):
    # Define parameter names
    SCHEMA = 'SCHEMA'
    PERMISSIONS = 'PERMISSIONS'
    SRID = 'SRID'
    LAYER_LIST = 'LAYER_LIST'
    DEST_FOLDER = 'DEST_FOLDER'
    CNX_DEST="CNX_DEST"
    TABLE_INFO="TABLE_INFO"
    PROC_1="PROC_1"
    PROC_2="PROC_2"
    PROC_3="PROC_3"
    RUNDB_PROC_1="RUNDB_PROC_1"
    IDINFO="IDINFO"
    DELEXISTINGDATA="DELEXISTINGDATA"

    def createInstance(self):
        return ChConPg()

    def name(self):
        return 'chConPg'

    def displayName(self):
        return 'Import vector layers to PostGIS with remapping'

    def group(self):
        return 'Utility Tools'

    def groupId(self):
        return 'lluishzi_tools'

    def shortHelpString(self):
        return """
The chConexPg.py script defines a custom QGIS Processing tool for exporting vector layers to a PostGIS database, with additional options for schema, permissions, and project remapping. Here’s a breakdown of its usage, workflow, and parameters:

---

****Usage Overview****

This tool is designed to automate the process of:
Creating tables in a PostGIS database based on QGIS vector layers.
Ingesting features from selected layers into those tables.
Optionally remapping project layer sources to the new database locations.
Managing permissions and metadata for the exported data.

****Workflow & Actions Performed****

1. Parameter Collection
   The tool collects user input for database connection, schema, permissions, layers to export, and other options.

2. Phase 1: Table Creation
   If enabled, the tool generates SQL scripts to create tables in the specified schema, matching the structure of the selected layers.
   Optionally, it can execute the generated SQL directly in the database.

3. Phase 2: Data Ingestion
   If enabled, the tool exports the data from the selected layers into the corresponding PostGIS tables.
   It can optionally delete existing data in the target tables before importing.

4. Phase 3: Project Remapping
   If enabled, the tool updates the QGIS project so that layers now point to the new PostGIS sources instead of their original sources.

5. Logging and Output
   The tool logs all actions, generates SQL and structure files, and can store metadata about the operation in a database table.

****Parameters and their Utility****

| Parameter Name         | Description & Utility                                                                                 |
|----------------------- |------------------------------------------------------------------------------------------------------|
| SCHEMA             | Destination schema in the PostGIS database for the new tables. Attention , the user connecting to postgres specified in QGIS database connection name must have the necessary permissions to create tables in the specified schema.                                       |
| PERMISSIONS        | Comma-separated list of permissions to grant on the new tables in the format of [[a|s]:,] , the *a* permission is for all, the *s** permission is for select (e.g., a:username for ALL).         |
| SRID               | Spatial Reference System Identifier for the output tables (e.g., 25831, 4326).                       |
| LAYER_LIST         | List of QGIS vector layers to export to PostGIS.                                                     |
| DEST_FOLDER        | Folder where output files (SQL scripts, structure info) will be saved, it can be a temporary folder and must be writable.                               |
| CNX_DEST           | The QGIS database connection name for the target PostGIS database where the new tables will be created and features ingested.                                   |
| TABLE_INFO         | (Optional) Name of a table in the database to store metadata about the export operation. This name should include the schema (e.g., public.export_metadata). If this doesn't exist, it will be created.             |
| PROC_1             | Boolean: If true, generate SQL for table creation (Phase 1).                                         |
| RUNDB_PROC_1       | Boolean: If true, execute the generated SQL for table creation in the database.                      |
| PROC_2             | Boolean: If true, ingest data from layers into the new tables (Phase 2).                             |
| IDINFO             | (Optional) Identifier for a record in the metadata table, used for retrieving the information of the export operation.  |
| DELEXISTINGDATA    | Boolean: If true, delete existing data in the target tables before importing new data.               |
| PROC_3             | Boolean: If true, remap the QGIS project layers to point to the new PostGIS sources (Phase 3).       |

****Typical Steps for a User****

Select the layers you want to export.
Choose the target schema and database connection.
Set permissions if needed.
Enable Phase 1 to generate and/or run table creation SQL.
Enable Phase 2 to ingest data.
Enable Phase 3 to update the project to use the new database sources.
Run the tool. Output files and logs will be saved in the specified folder.

          
"""

    def initAlgorithm(self, config=None):
        conn_names = QgsProviderRegistry.instance().providerMetadata("postgres").connections().keys()
        
        if not conn_names:
            raise Exception("⚠ No hi ha cap connexió PostGIS configurada al QGIS")
        import_path = os.path.join(os.getcwd(), "conexpg.json")

        conexpg_data = dict()
        if os.path.exists(import_path):
            with open(import_path, "r", encoding="utf-8") as f:
                conexpg_data = json.load(f)
        conexpg_data = conexpg_data.get("inputs", dict())

        # Add a string parameter for schema
        self.addParameter(
            QgsProcessingParameterString(
                self.SCHEMA,
                'Destination schema',
                defaultValue=conexpg_data.get("SCHEMA","public"),
            )
        )

        # Add a string parameter for permissions
        self.addParameter(
            QgsProcessingParameterString(
                self.PERMISSIONS,
                'Permissions',
                defaultValue=conexpg_data.get("PERMISSIONS",""),
                optional=True
            )
        )

        # Add an SRID selector (enum)
        self.addParameter(
            QgsProcessingParameterEnum(
                self.SRID,
                'Select SRID',
                options=['25831','4326', '3857', '4269'],
                defaultValue=conexpg_data.get("SRID","25831")
            )
        )

        # Add a layer list selection parameter
        self.addParameter(
            QgsProcessingParameterMultipleLayers(
                self.LAYER_LIST,
                'Select Layers'
            )
        )

        # Add a destination folder selection parameter
        self.addParameter(
            QgsProcessingParameterFolderDestination(
                self.DEST_FOLDER,
                'Destination Folder for struct data'
            )
        )


        self.addParameter(
            QgsProcessingParameterProviderConnection(
                self.CNX_DEST,
                'DB Connection',
                "postgres"
            )
        )

         # Add a string parameter for new destination
        self.addParameter(
            QgsProcessingParameterString(
                self.TABLE_INFO,
                'Name of information table',
                defaultValue=conexpg_data.get("TABLE_INFO","public.info_traspassos"),
                optional=True
            )
        )
        self.addParameter(
            QgsProcessingParameterBoolean(
                self.PROC_1,
                'Process Phase 1: Table creation',
                defaultValue=conexpg_data.get("PROC_1",False)
            )
        )
        self.addParameter(
            QgsProcessingParameterBoolean(
                self.RUNDB_PROC_1,
                'Process Phase 1: Table creation - Run generated SQL',
                defaultValue=conexpg_data.get("RUNDB_PROC_1",False),
            )
        )
        self.addParameter(
            QgsProcessingParameterBoolean(
                self.PROC_2,
                'Process Phase 2: Data Ingestion',
                defaultValue=conexpg_data.get("PROC_2",False),
            )
        )
        self.addParameter(
            QgsProcessingParameterNumber(
                self.IDINFO,
                'Process Phase 2: Struct record identifier',
                defaultValue=0,
            )
        )
        self.addParameter(
            QgsProcessingParameterBoolean(
                self.DELEXISTINGDATA,
                'Process Phase 2: Delete existing postgres data',
                defaultValue=conexpg_data.get("DELEXISTINGDATA",True),
            )
        )
        self.addParameter(
            QgsProcessingParameterBoolean(
                self.PROC_3,
                'Process Phase 3: Project reallocation',
                defaultValue=conexpg_data.get("PROC_3",False),
            )
        )

    def processAlgorithm(self, parameters, context, feedback:QgsProcessingFeedback):
        # Retrieve parameter values
        params={
            'esquema': self.parameterAsString(parameters, self.SCHEMA, context),
            'llperms': self.parameterAsString(parameters, self.PERMISSIONS, context),
            'sriddest': self.parameterAsEnum(parameters, self.SRID, context),
            'layer_list': self.parameterAsLayerList(parameters, self.LAYER_LIST, context),
            'sortida': self.parameterAsString(parameters, self.DEST_FOLDER, context),
            'bddesti':  self.parameterAsConnectionName(parameters, self.CNX_DEST, context),
            'taulainfo':  self.parameterAsString(parameters, self.TABLE_INFO, context),
            'rundbproc1':  self.parameterAsBoolean(parameters, self.RUNDB_PROC_1, context),
            'runproc1':  self.parameterAsBoolean(parameters, self.PROC_1, context),
            'runproc2':  self.parameterAsBoolean(parameters, self.PROC_2, context),
            'runproc3':  self.parameterAsBoolean(parameters, self.PROC_3, context),
            'esborradadesAnt':  self.parameterAsBoolean(parameters, self.DELEXISTINGDATA, context),
            'idinfo':  self.parameterAsInt(parameters, self.IDINFO, context),
            'idinfo':  self.parameterAsInt(parameters, self.IDINFO, context),
        }
        
        
        def fesLog(missatge,tip="debug"):
            if not isinstance(missatge, str):
                try:
                    missatge = json.dumps(missatge, default=str, indent=2)
                except Exception:
                    try:
                        missatge = vars(missatge)
                    except Exception:
                        missatge = str(missatge)
            if tip=="debug":
                feedback.pushDebugInfo(f"Debug: {missatge}")
            elif tip=="warning":
                feedback.pushWarning(f"Warning: {missatge}")
            elif tip=="error":
                feedback.reportError(f"Error: {missatge}")
            elif tip=="codi":
                feedback.pushFormattedMessage(f"<pre>{missatge}</pre>",missatge)
                
        for k in params:
            fesLog(f"Param '{k}': {params[k]}")
        params["bddesti"]=QgsProviderRegistry.instance().providerMetadata("postgres").connections().get(params['bddesti'])
        #poder de fer https://qgis.org/pyqgis/3.40/core/QgsAbstractDatabaseProviderConnection.html com execsql
        if (params["taulainfo"] and params["taulainfo"]!='') :
                    params["bddesti"].execSql(f"""
CREATE TABLE if not exists {params["taulainfo"]} (
	id int4 GENERATED ALWAYS AS IDENTITY NOT NULL,
	esquema text,
	capa text,
	execucio text,
	data timestamptz,
	origen text,
	fullsql text,
	estructura json
);"""
                            )
        # Process layers (example logic)
        if params["runproc1"]:
            params["idinfo"]=self.generaCreateTablesIEquivalencies(DictObj(params),fesLog)
        if params["runproc2"]:
            self.traspassaCapesAPostgres(DictObj(params),IteradorCapesProjecte(params["layer_list"],False),context,feedback,fesLog)
        if params["runproc3"]:
            self.remapejaOrigensDades(DictObj(params),fesLog)
        # Return results (example output)
        return {
            self.DEST_FOLDER: params['sortida']
        }
    def createInstance(self):
        return self.__class__()
    
    def remapejaOrigensDades(self,args,print):    
        from qgis.core import (QgsProject,QgsDataProvider,QgsProcessingException)
        dcs=dict()
        print("Remapping sources")
        if args.struct:            
            dtm=json.loads(rarxiu(args.struct))
        else:
            if args.idinfo and args.idinfo>0:
                dtm=json.loads(args.bddesti.execSql(f"""select  estructura from {args.taulainfo} where id={args.idinfo} ;
                                """).rows()[0][0])
            else:
                if args.sortida and os.path.exists(os.path.join(args.sortida,f"estructura.txt")):
                    dtm=json.loads(rarxiu(os.path.join(args.sortida,f"estructura.txt")))
                else:
                    raise QgsProcessingException("No structure or idinfo specified")
        # Dictionary with source equivalences to table definition
        for x in dtm:
            dcs[dtm[x]["source"]] = dtm[x]
        # For each layer, check if the source is one of the searched and change the source
        if args.noudesti is None or args.noudesti=='':
            args.noudesti=f"{args.bddesti.uri()} table=$sh$.$tb$ ($cg$) "
        for v in IteradorCapesProjecte(args.layer_list,True):
            cap=v["capa"]
            nso=cap.dataProvider().dataSourceUri()
            if nso in dcs:           
                desti=args.noudesti.replace("$sh$",f'"{dcs[nso]["esquema"]}"').replace("$tb$",f'"{dcs[nso]["nomtaula"]}"').replace("$cg$",dcs[nso]["cgeomdest"])
                print(f"Remapping {nso} to {sanitize_connection_string(desti)}")
                cap.setDataSource(desti,cap.name(),"postgres",QgsDataProvider.ProviderOptions())
                if cap.isValid():
                    print(f"Layer {cap.name()} with source [{nso}] changed to [{desti}]")
                else:
                    print(f"Layer {cap.name()} with source [{nso}] NOT changed to [{desti}]")
            else:
                print(f"Layer {cap.name()} with source [{nso}] not found ")
        print(f"Saving project with modified layers")
        nfs=QgsProject.instance().absoluteFilePath()+".mod.qgz"
        rg=QgsProject.instance().write(nfs)
        if not rg:
            print(f"Error saving file, {QgsProject.instance().error()}")
        else:
            print(f"File successfully saved to {nfs}")
        #QgsProject.instance().close()    

    def traspassaCapesAPostgres(self,args,iterador,context,feedback,print):
        from qgis.core import (QgsApplication,QgsProviderRegistry,
                            QgsProcessingFeedback,QgsVectorLayerExporter,QgsProcessingException,QgsProviderConnectionException,
                            QgsProcessingRegistry,QgsVectorLayer,QgsWkbTypes,QgsCoordinateReferenceSystem,QgsProject,QgsDataSourceUri)
        from qgis.analysis import QgsNativeAlgorithms
        import processing
        if args.struct:            
            defin=json.loads(rarxiu(args.struct))
        else:
            if args.idinfo and args.idinfo>0:
                defin=json.loads(args.bddesti.execSql(f"""select  estructura from {args.taulainfo} where id={args.idinfo} ;
                                """).rows()[0][0])
            else:
                if args.sortida and os.path.exists(os.path.join(args.sortida,f"estructura.txt")):
                    defin=json.loads(rarxiu(os.path.join(args.sortida,f"estructura.txt")))
                else:
                    raise QgsProcessingException("No s'ha indicat ni estructura ni idinfo")
        dcap=dict()
        ctmp=''
        if iterador:
            dcap={c.get("ncapa"):c.get("capa") for c in iterador}  

        # get the configuration information (including username and password)
        for capa,dcapa in defin.items(): #per cada capa
            try:
                #generem una capa de sql amb els alias
                print(f"Processing layer {dcapa.get('arxiu')} to {dcapa.get('esquema','public')}.{dcapa.get('nomtaula','dad')}")
                tegeom=dcapa.get("tgeom","NOGEOM")!="NOGEOM"
                sql=dcapa.get("sqltrasp","---")
                #creem la capa temporal amb els noms de camp canviats
                #print(f"I:SQL de conversio de noms de camps:{sql}")
                sridori=dcapa.get("srid", 'EPSG:25831')
                if capa in dcap:
                    sridori=dcap.get(capa).crs().authid()
                dparams={'INPUT_DATASOURCES':[dcap.get(capa,dcapa.get("arxiu"))],'INPUT_QUERY':sql,'INPUT_GEOMETRY_FIELD':dcapa.get('cgeom','geometry') if tegeom else '','INPUT_GEOMETRY_TYPE':0 if tegeom else 1,'INPUT_GEOMETRY_CRS':QgsCoordinateReferenceSystem(sridori) if tegeom else None,'OUTPUT':'TEMPORARY_OUTPUT'}
                print("Running: qgis:executesql")
                print(dparams)
                resul=processing.run("qgis:executesql", dparams,context=context,feedback=feedback)
                if not "OUTPUT" in resul:
                    print(f"E:No s'ha pogut carregar el alias per la capa:{capa} - {sql}")
                    continue
                print("Importing data to temporary geopackage:")
                print(resul)
                vlayer=resul.get("OUTPUT")
                # de https://github.com/qgis/QGIS/blob/b6a7a1070329a1f167341e3187781954d28f2f39/python/plugins/processing/algs/qgis/ImportIntoPostGIS.py
                #la exportem a geopackage pel ogr
                if os.path.exists(os.path.join(args.sortida,f'temporal.gpkg')):
                    os.remove(os.path.join(args.sortida,f'temporal.gpkg'))
                if os.path.exists(args.sortida)==False:
                    os.makedirs(args.sortida)
                QgsProject.instance().addMapLayer(vlayer)
                print("Exporting to geopackage:")
                dparun2={'LAYERS':[vlayer.source()],'OUTPUT':os.path.join(args.sortida,f'temporal.gpkg'),'OVERWRITE':False,'SAVE_STYLES':True,'SAVE_METADATA':True,'SELECTED_FEATURES_ONLY':False,'EXPORT_RELATED_LAYERS':False}
                print(dparun2)
                processing.run("native:package", dparun2,
                               context=context,feedback=feedback)
                QgsProject.instance().removeMapLayer(vlayer)
                #borrem dades previes si cal
                if args.esborradadesAnt:
                    print(f"Truncating table {dcapa.get('esquema','public')}.{dcapa.get('nomtaula','dad')}","warning")
                    args.bddesti.execSql(f"truncate table {dcapa.get('esquema','public')}.{dcapa.get('nomtaula','dad')}")
                    #processing.run("native:postgisexecutesql", {'DATABASE':"gisvendrell SDOMINI2",'SQL':f"truncate table {dcapa.get('esquema','public')}.{dcapa.get('nomtaula','dad')}"})
                #disparem el ogr que es l'unic que ho fa be
                if tegeom:
                    srs=f"\"{ f'EPSG:{args.sriddest}' if args.sriddest else 'EPSG:25831'}\" " 
                    srs=args.sriddestogr or srs
                    if srs:
                        srs=f" -t_srs  {srs}"
                else:
                    srs=""
                # Create command string with database connection using environment variables
                processed_conn_string = GetConectionString(args.bddesti.uri())                
                ctmp=f"ogr2ogr.exe -progress --config PG_USE_COPY=YES   -doo \"PRELUDE_STATEMENTS=BEGIN;\" -doo \"CLOSING_STATEMENTS=COMMIT;\"   -f PostgreSQL \"PG:{processed_conn_string}  active_schema={dcapa.get('esquema','public')} \" -lco DIM={args.dimout or '2'} \"{os.path.join(args.sortida,f'temporal.gpkg')}\" -lco SPATIAL_INDEX=OFF -update -append -nlt {dcapa.get('tgeom','POINT') if tegeom else 'NONE'} -lco GEOMETRY_NAME=geometry -lco FID={dcapa.get('pkorig','objectid')}  -nln {dcapa.get('esquema','public')}.{dcapa.get('nomtaula','dad')} {srs} -skipfailures "                
                sanitized_cmd = sanitize_connection_string(ctmp)
                print(f"Importing data with {sanitized_cmd}")
                res=execcom(f"{ctmp}",print)
                if res[0]!=0:
                    print(f" The execution of {sanitized_cmd} did not go well","error")
                    #raise  f"E:La execucio de {f} no ha anat be {res[0]}"
                # Sanitize command before logging or displaying
                
                rs=f'''
    ******************************************************************************************************************************        
    Comanda:{sanitized_cmd}
    Resultat ({res[0]}):
    {res[1]}
    ******************************************************************************************************************************
    '''
                print(rs)
                warxiu(os.path.join(args.sortida,f'fesinserts.txt'),rs,"a")
                if (args.taulainfo and args.taulainfo!='') :
                    tinf=args.taulainfo 
                    args.bddesti.execSql(f"""INSERT INTO {tinf} (esquema, capa, execucio, data, origen, fullsql, estructura) 
                                         VALUES({ecad(args.esquema)}, {ecad(dcapa.get('nomtaula','dad'))}, {ecad(rs)}, now(), {ecad(capa)}, '', {ecad(json.dumps(dcapa, indent=4))});"""
                            )
            except Exception as ex:
                # Sanitize command before logging or displaying in error messages
                sanitized_cmd = sanitize_connection_string(ctmp)
                rs=f'''
    ******************************************************************************************************************************        
    Comanda:{sanitized_cmd}
    Processant capa {dcapa.get('arxiu')} cap a {dcapa.get('esquema','public')}.{dcapa.get('nomtaula','dad')}
    Exepció - 
    {ex}
    ******************************************************************************************************************************
    '''
                warxiu(os.path.join(args.sortida,f'fesinserts.txt'),rs,"a")
                print(rs)

    def generaCreateTablesIEquivalencies(self,arg,print):    
        from qgis.core import (QgsVectorLayer,QgsWkbTypes)
        comandes=""
        gensql=""
        gensqlfin=""
        gensqlcoments=""
        n=0 #comptador de camps 
        nc=0 #comptador de capes
        dcapes=dict()
        #de moment sense us per si cal traduir noms de taules
        trtau=dict()    
        print(f"Generating SQL structure, received:{arg}")
        for v in IteradorCapesProjecte(arg.layer_list,False):        
            print(f"Processing {v}")
            f=v.get("pth")
            llgrseq=set()
            nct=os.path.splitext(os.path.basename(f))[0]        
            nfcapa=v.get("ncapa",espmin(nct))
            if nfcapa.lower() in trtau:
                print(f"Table translation found from {nfcapa.lower()} to {trtau[nfcapa.lower()]}")
                nfcapa=trtau[nfcapa.lower()]            
            nfcapa=espmin(nfcapa)
            if nfcapa in dcapes:
                nfcapa=nfcapa+f"_{n}"
                print(f"Layer {v['pth']} has been renamed to {nfcapa} due to a name conflict","warning")
                n=n+1
            #        dcapes[nfcapa]=1
            print(f"Using layer name {nfcapa} to process [{nct}]:{f}")
            scam=""
            eqhicamp=""
            dcamps={}
            if "capa" in v:
                vlayer = v.get("capa")
            else:
                vlayer = QgsVectorLayer(f, f"Capa{n}")
            if not vlayer.isValid():
                print(f"{f} Layer failed to load!","error")
                continue        
            tipgeom=QgsWkbTypes.displayString(vlayer.wkbType()).lower()
            geometria=tipgeom != "nogeometry"
            if not tipgeom in mapeigTipusGeom:
                print(f"{f} geometry type {vlayer.wkbType()} is unknown, not generating table","error")
                continue
            tipgeom=mapeigTipusGeom.get(tipgeom)
            #comprovem sistema de coordenades origen        
            sridorig=vlayer.crs().postgisSrid() if geometria else -1
            if sridorig==0:
                print(f"{f} SRID {vlayer.crs().authid()} is unknown to Postgres, not generating table","error")
                continue
            nc=0
            infocapa={"arxiu":f,"source":vlayer.source(),"srid":vlayer.crs().authid() if geometria else "","sridpgis":sridorig,"tgeom":tipgeom,"nomtaula":nfcapa,"esquema":arg.esquema,"cgeom":v.get("cgeom","geometry"),"cgeomdest":"geom","pkpg":"id","pkorig":"objectid","coment":v.get("coment",""),"dadorigen":v.get("dadorigen","desconegut")} #informacio de processat de capes
            print(f"Processant taula {f} com a capa {nfcapa}")
            for cam in vlayer.fields():
#                if cam.name().lower() in campsEliminar:
#                    print(f"W:El camp {cam.name()} ha estat marcat per no fer-lo")
#                    continue
                ncam=espmin(cam.name())
                if ncam in mapaCamps:
                    ncam=mapaCamps[ncam]
                ncam=ncam[0:60]
                if ncam in dcamps or ncam=="id":
                    ncam=f"ncamp_{nc}"
                    print(f"Field {espmin(cam.name())} in the layer has been renamed to {ncam} due to a name conflict","warning")
                    nc+=1
                dcamps[ncam]=cam.name()
                if cam.typeName().lower() in mapeigCamps:                
                    scam=scam + f"\n  {ncam} {mapeigCamps[cam.typeName().lower()]},"
                    if ncam!=cam.name():
                        gensqlcoments+=f"""COMMENT ON COLUMN  {arg.esquema}.{nfcapa}.{ncam} IS 'Origen:{cam.name().replace("'","''")}';
    """
                else:
                    print(f"Field {cam.name()} is of unknown type: {cam.typeName()}","warning")
                    continue
            sqlalias=f"select "
            for cdest,corig in dcamps.items():
                sqlalias+=f" \"{corig}\" as {cdest}, "
            if geometria:
                sqlalias+=f" geometry from input1 "
            else:
                sqlalias= f"{sqlalias[:-2]} from input1"
            gensqlloc=f"""
            CREATE TABLE {arg.esquema}.{nfcapa}
            (
                id integer primary key generated always as identity,  {scam[:-1]}             
            );            
    """
            #ALTER TABLE  {arg.esquema}.{nfcapa} OWNER to postgres;          
            if  arg.llperms and len(arg.llperms)>0:
                for np in arg.llperms.split(","):
                    p=np.lower().split(":",2)
                    if p[0]=="a":
                        prm="ALL"
                        llgrseq.add(p[1])
                    else:
                        prm="SELECT, REFERENCES"
                    gensqlloc+=f"GRANT { prm}   ON TABLE  {arg.esquema}.{nfcapa} TO {p[1]};\n"                
                    # faltaria grant all on all sequences in schema aigues to udisseny
            if geometria:
                gensqlloc+=f"select AddGeometryColumn('{arg.esquema}','{nfcapa}','geom',{arg.sriddest or '25831'},'{tipgeom}', '2');\nCREATE INDEX idx_{nfcapa}_geom   ON {arg.esquema}.{nfcapa}   USING gist   (geom );\n\n\n"
            gensqlfin+=f"truncate table {arg.esquema}.{nfcapa};\n"
            cmcapl=""
            if len(infocapa.get("coment",""))>0:
                cmcapl+=f"""{infocapa.get("coment","")}
    """
            cmcapl+=f"""Nom original de la capa: {vlayer.name()}
    Origen de la capa:{infocapa['source']}"""
            gensqlcoments+=f"""COMMENT ON TABLE {arg.esquema}.{nfcapa} IS '{cmcapl.replace("'","''")}';
    """        
            infocapa["camps"]=dcamps
            infocapa["sqlcrea"]=gensqlloc
            infocapa["sqltrasp"]=sqlalias
            dcapes[nfcapa]=infocapa
            gensql+=gensqlloc
        for n in llgrseq:
            gensql+=f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {arg.esquema}  TO {n};\n"
        gensql+=gensqlcoments
        gensql+=gensqlfin

        print(f"""
/************************************************************************/
      SQL SCRIPT TO GENERATE TABLES AND PERMISSIONS
/************************************************************************/              
{gensql}
/************************************************************************/              
      ""","codi")
        if (not os.path.isdir(arg.sortida)):
            os.makedirs(arg.sortida)
        warxiu(os.path.join(arg.sortida,f"genfullsql.sql"), gensql)    
        warxiu(os.path.join(arg.sortida,f"estructura.txt"), json.dumps(dcapes, indent=4))
        print(f"""
    *****************************************************************
    Generation result:
    *****************************************************************
    SQL construction output: {os.path.join(arg.sortida,f"genfullsql.sql")}
    Structure equivalence output: {os.path.join(arg.sortida,f"estructura.txt")}
        """)
        codiretorn = None
        if (arg.taulainfo and arg.taulainfo!='') :
            tinf=arg.taulainfo 
            rsexec=arg.bddesti.execSql(f"""INSERT INTO {tinf} (esquema, capa, execucio, data, origen, fullsql, estructura)
                                 VALUES({ecad(arg.esquema)}, '', '',now(), '', {ecad(gensql)}, {ecad(json.dumps(dcapes, indent=4))}) RETURNING id  ;
                                """)
            print(f"Execution info inserted into table {tinf} of database {arg.bddesti.uri()}")
            codiretorn=rsexec.rows()[0][0]
            print(f"Code: {rsexec.rows()}")
        if (arg.rundbproc1):
            arg.bddesti.execSql(gensql)
        return codiretorn