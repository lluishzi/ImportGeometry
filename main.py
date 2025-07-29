import os
import re
import sys
import json
import argparse
import psycopg2
import  datetime

"""
Creació de la taula d'informacio (tard)

CREATE TABLE public.info_traspassos (
	id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	esquema text,
	capa text,
	execucio text,
	data timestamptz,
	origen text,
	fullsql text,
	estructura json
);


Permisos generics:
grant all on schema espais_verds to gis_disseny;


alter default privileges in schema espais_verds grant all on tables to gis_disseny;
alter default privileges in schema espais_verds grant all on sequences to gis_disseny;


"""

sys.path.append(os.path.realpath(os.getcwd()+"\\moduls"))
print(f"Afegit:{os.path.realpath(os.getcwd()+"\\moduls")}")
from mwutils import normNomCamp as espmin,warxiu,rarxiu,execcom
qgs=None
# Variable global per l'arxiu d'entorn
arxiuenv=None

def sanitize_connection_string(conn_string):
    """Sanitize a connection string by removing sensitive information"""
    if not conn_string:
        return conn_string
    # Replace password parameter in connection strings
    sanitized = re.sub(r'password\s*=\s*[^\s]+', 'password=*****', conn_string)
    # Replace other potentially sensitive information
    sanitized = re.sub(r'pwd\s*=\s*[^\s]+', 'pwd=*****', sanitized)
    return sanitized

def initEntornQGis(pt,args):
    global qgs
    if args.pthpwdbd :
        os.environ["QGIS_AUTH_DB_DIR_PATH"]=args.pthpwdbd
    if args.pwdpwbd :        
        os.environ["QGIS_AUTH_PASSWORD_FILE"]=args.pwdpwbd
    if pt:
        # set up system paths
        qspath = os.path.join(pt,'qgis_sys_paths.csv' )
        # provide the path where you saved this file.
        with open(qspath) as file:
            els = [line.rstrip() for line in file]
        els=els[1:]
        sys.path += els
        # set up environment variables
        qepath = os.path.join(pt,'qgis_env.json')
        js = json.loads(open(qepath, 'r').read())
        for k, v in js.items():
            os.environ[k] = v
    else:
        #cal fer set PYTHONPATH=c:\PROGRA~1\QGIS32~1.11\apps\qgis-ltr\python\ sbent que OSGEO4W_ROOT=C:\PROGRA~1\QGIS32~1.11        
        cadpt=f'{os.environ["OSGEO4W_ROOT"]}\\apps\\qgis-ltr\\python\\'
        if not os.path.exists(cadpt):
            cadpt=f'{os.environ["OSGEO4W_ROOT"]}\\apps\\qgis\\python\\'
            if not os.path.exists(cadpt):
                raise Exception(f"No s'ha pogut trobar la llibreria de qgis en base a {os.environ['OSGEO4W_ROOT']}\\apps\\qgis\\python\\")
        sys.path.insert(0,cadpt+"\\plugins\\")
        sys.path.insert(0,cadpt)
    #import gdal
    from qgis.core import (QgsApplication,
                        QgsProcessingFeedback,
                        QgsProcessingRegistry,QgsVectorLayer,QgsWkbTypes)
    from qgis.analysis import QgsNativeAlgorithms
    feedback = QgsProcessingFeedback()
    # initializing processing module
    if pt:
        QgsApplication.setPrefixPath(js['HOME'], True)
    qgs = QgsApplication([], False)
    qgs.initQgis() # use qgs.exitQgis() to exit the processing module at the end of the script.
    # initialize processing algorithms
    from processing.core.Processing import Processing
    Processing.initialize()
    import processing
    QgsApplication.processingRegistry().addProvider(QgsNativeAlgorithms())

campsEliminar=() #"objectid"
mapeigCamps={
    "string":"text",
    "integer":"int",
    "date":"timestamp",
    "real":"float",
    "integer64":"int",
    "boolean":"boolean",
    "integer":"int",
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
def get_connection_string_with_env_vars(connection_string):
    """
    Process a connection string to replace environment variable references
    Format: password=environ['PASSWORD_VAR'] will be replaced with the value from environment variable
    
    The function will:
    1. First check if the variable exists in the system environment variables
    2. If not found, it will look in the INI file specified by the global variable 'arxiuenv'
    
    The INI file should have the format:
    [env]
    VAR1=value1
    VAR2=value2
    """
    global arxiuenv
    
    if not connection_string:
        return connection_string
    
    # Create a dictionary with all available environment variables
    env_vars = os.environ.copy()
    
    # If an INI file path is specified in the global variable, read variables from it
    if arxiuenv and os.path.exists(arxiuenv):
        try:
            with open(arxiuenv, 'r') as f:
                lines = f.readlines()
                
            section = None
            for line in lines:
                line = line.strip()
                
                # Skip empty lines and comments
                if not line or line.startswith('#') or line.startswith(';'):
                    continue
                
                # Check for section headers [section]
                if line.startswith('[') and line.endswith(']'):
                    section = line[1:-1]
                    continue
                
                # Process key=value pairs
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    # If we're in the [env] section or no section is defined, add the variable
                    if section is None or section.lower() == 'env':
                        env_vars[key] = value
        except Exception as ex:
            print(f"Warning: Error reading environment file {arxiuenv}: {ex}")
    
    # Find environment variable references: environ['VAR_NAME']
    pattern = r"environ\['([^']+)'\]"
    matches = re.findall(pattern, connection_string)
    
    result = connection_string
    for var_name in matches:
        if var_name in env_vars:
            # Replace the reference with the actual value
            result = result.replace(f"environ['{var_name}']", env_vars[var_name])
        else:
            print(f"Warning: Environment variable {var_name} not found in system or INI file")
    
    return result

def execPGSQL(cnx,sql,params=set()):  
    # Process connection string for environment variables
    processed_cnx = get_connection_string_with_env_vars(cnx)
    
    with psycopg2.connect(processed_cnx) as cn:
        with cn.cursor() as cr:
            cr.execute(sql,params)

def ObteCapesGeoPackage(path):
    lret=[]
    lfits=[os.path.join(path,n) for n in  os.listdir(path)]
    for f in lfits:
        ObteCapesGeoPackageSimple(f,lret)    
    return lret
def ObteCapesGeoPackageSimple(args,lret=[]):
    from qgis.core import (QgsApplication,
                        QgsProcessingFeedback,
                        QgsProcessingRegistry,QgsVectorLayer,QgsWkbTypes)
    fileName = args
    layer = QgsVectorLayer(fileName,"test","ogr")
    subLayers =layer.dataProvider().subLayers()    
    #per cada una nomes cal posar NOMGEOPACKAGE|layername=XXXXXXXX
    for subLayer in subLayers:
        name = subLayer.split('!!::!!')[1]
        uri = "%s|layername=%s" % (fileName, name,)
        lret.append({"pth":uri,"ncapa":name,"cgeom":subLayer.split('!!::!!')[4]} )
    return lret

def cercaNoms2(base,partn,dc,lnum,n,lp):
    from qgis.core import (QgsLayerTreeLayer,QgsMapLayer)
    lloc=partn.copy()
    lnut=lnum.copy()
    i=0
    if len(base.name())>0 :
        lloc.append(base.name())
        lnut.append('{:0>3}'.format(n))
    for f in base.children():        
        #abans hi havia f.layer().isValid() and 
        if isinstance(f,QgsLayerTreeLayer) and f.layer().type() == QgsMapLayer.VectorLayer:
            i+=1
            lp.append(f.layer())
            source=f.layer().dataProvider().dataSourceUri()
            if source in dc:
                scom=dc[source]["coment"]
                print(f"Atenció capa del arbre {f.layerId()} amb el mateix origen que {scom} , no la importarem ") 
            else:
                dc[source]={"treeid":f.layerId(), "coment":' > '.join(lloc) + f": {f.name()}","num":'_'.join(lnut) +"_"+ '{:0>2}'.format(i),"vis":f.isVisible()}
                dc[source]["dadorigen"]=f.layer().dataProvider().dataSourceUri()                
        else:            
            n+=100
            cercaNoms2(f,lloc,dc,lnut,n,lp)            

class IteradorCapesLLista:
    def __init__(self, lfits):
        self.lf = lfits
        self.n=-1

    def __iter__(self):
        return self

    def __next__(self): # Python 2: def next(self)
        self.n+=1
        if self.n>=len(self.lf):
            raise StopIteration    
        return self.lf[self.n]

class IteradorCapesProjecte:
    def __init__(self, arxiu,args,elmaplayer):
        self.a= arxiu
        self.n=-1        
        self.maplay=elmaplayer

    def __iter__(self):
        from qgis.core import (QgsApplication,QgsProviderRegistry,
                        QgsProcessingFeedback,QgsVectorLayerExporter,QgsProcessingException,QgsProviderConnectionException,
                        QgsProcessingRegistry,QgsVectorLayer,QgsWkbTypes,QgsCoordinateReferenceSystem,QgsProject,QgsDataSourceUri)
        readflags = QgsProject.ReadFlags()
        readflags|= QgsProject.FlagTrustLayerMetadata
        if not QgsProject.instance().read(self.a,readflags):
            raise Exception(f"No s'ha pogut carregar el projecte {self.a}")
        ll=[l for l in QgsProject.instance().mapLayers().values()]
        #self.lf=ll
        self.dcdesc={}
        root = QgsProject.instance().layerTreeRoot()        
        self.lf=list()
        cercaNoms2(root,list(),self.dcdesc,list(),0,self.lf)
        self.partnom=args.autonom
        def pel(e):
            a=e[1]
            a["idel"]=e[0]
            return a        
        if self.maplay:
            self.lf=ll
        return self

    def __next__(self): # Python 2: def next(self)
        from qgis.core import (QgsMapLayer)
        self.n+=1
        if self.n>=len(self.lf):
            raise StopIteration    
        l=self.lf[self.n]        
        if not l.isValid():
            print(f"Capa erronia d'origen :{l.name()}")
            return self.__next__()
        if not l.type() == QgsMapLayer.VectorLayer:
            print(f"Capa {l.name()} no és de tipus vector i no la importarem")
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
    

def generaCreateTablesIEquivalencies(lfits,arg):    
    from qgis.core import (QgsApplication,
                        QgsProcessingFeedback,
                        QgsProcessingRegistry,QgsVectorLayer,QgsWkbTypes)
    from qgis.analysis import QgsNativeAlgorithms
    comandes=""
    gensql=""
    gensqlfin=""
    gensqlcoments=""
    n=0 #comptador de camps 
    nc=0 #comptador de capes
    dcapes=dict()
    trtau=dict()    
    if arg.tradtau:
        vt=arg.tradtau.split('|')
        for e in vt:
            vt2=e.split(':')
            trtau[vt2[0].lower()]=vt2[1]   
    print(json.dumps(trtau)) 
    for v in lfits:        
        f=v.get("pth")
        llgrseq=set()
        nct=os.path.splitext(os.path.basename(f))[0]        
        nfcapa=v.get("ncapa",espmin(nct))
        print(f"Provant traduccio de taula per {nfcapa.lower()}")
        if nfcapa.lower() in trtau:
            print(f"Traduccio de taula trobada de {nfcapa.lower()} a {trtau[nfcapa.lower()]}")
            nfcapa=trtau[nfcapa.lower()]            
        nfcapa=espmin(nfcapa)
        if nfcapa in dcapes:
            nfcapa=nfcapa+f"_{n}"
            print(f"W:La capa {v['pth']} ha estat renombrada a {nfcapa} per coincidencia amb una altre")
            n=n+1
#        dcapes[nfcapa]=1
        print(f"I: Utilitzarem el nom de capa  {nfcapa} per tractar [{nct}]:{f}")
        scam=""
        eqhicamp=""
        dcamps={}
        if "capa" in v:
            vlayer = v.get("capa")
        else:
            vlayer = QgsVectorLayer(f, f"Capa{n}")
        if not vlayer.isValid():
            print(f"E:{f} Layer failed to load!")
            continue        
        tipgeom=QgsWkbTypes.displayString(vlayer.wkbType()).lower()
        geometria=tipgeom != "nogeometry"
        if not tipgeom in mapeigTipusGeom:
            print(f"E:{f} el tipus de geometria  {vlayer.wkbType()} no és conegut, no generem")
            continue
        tipgeom=mapeigTipusGeom.get(tipgeom)
        #comprovem sistema de coordenades origen        
        sridorig=vlayer.crs().postgisSrid() if geometria else -1
        if sridorig==0:
            print(f"E:{f} el srid {vlayer.crs().authid()} no és conegut pel postgres, no generem")
            continue
        nc=0
        
        infocapa={"arxiu":f,"source":vlayer.source(),"srid":vlayer.crs().authid() if geometria else "","sridpgis":sridorig,"tgeom":tipgeom,"nomtaula":nfcapa,"esquema":arg.esquema,"cgeom":v.get("cgeom","geometry"),"cgeomdest":"geom","pkpg":"id","pkorig":v.get("pkorigen",arg.pkorigen or "objectid"),"coment":v.get("coment",""),"dadorigen":v.get("dadorigen","desconegut")} #informacio de processat de capes
        print(f"I:Processant taula {f} com a capa {nfcapa}")
        for cam in vlayer.fields():
            if cam.name().lower() in campsEliminar:
                print(f"W:El camp {cam.name()} ha estat marcat per no fer-lo")
                continue
            ncam=espmin(cam.name())
            if ncam in mapaCamps:
                ncam=mapaCamps[ncam]
            ncam=ncam[0:60]
            if ncam in dcamps or ncam=="id":
                ncam=f"ncamp_{nc}"
                print(f"W:El camp {espmin(cam.name())} de la capa  ha estat renombrat a {ncam} per coincidencia amb una altre")
                nc+=1
            dcamps[ncam]=cam.name()
            if cam.typeName().lower() in mapeigCamps:                
                scam=scam + f"\n  {ncam} {mapeigCamps[cam.typeName().lower()]},"
                if ncam!=cam.name():
                    gensqlcoments+=f"""COMMENT ON COLUMN  {arg.esquema}.{nfcapa}.{ncam} IS 'Origen:{cam.name().replace("'","''")}';
"""
            else:
                print(f"E:El camp {cam.name()} es de tipus desconegut:{cam.typeName()}")
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
         ALTER TABLE  {arg.esquema}.{nfcapa} OWNER to postgres;          
"""
        if "llperms" in arg:
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
            gensqlloc+=f"select AddGeometryColumn('{arg.esquema}','{nfcapa}','geom',{arg.sriddest or '25831'},'{tipgeom}',{arg.dimout or '2'});\nCREATE INDEX idx_{nfcapa}_geom   ON {arg.esquema}.{nfcapa}   USING gist   (geom );\n\n\n"
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
    if (not os.path.isdir(arg.sortida)):
        os.makedirs(arg.sortida)
    warxiu(os.path.join(arg.sortida,f"genfullsql.sql"), gensql)    
    warxiu(os.path.join(arg.sortida,f"estructura.txt"), json.dumps(dcapes, indent=4))
    print(f"""
*****************************************************************
Resultat de la generació:
*****************************************************************
Sortida del la construccio del SQL:{os.path.join(arg.sortida,f"genfullsql.sql")}
Sortida amb la estructura d'equivalencia:{os.path.join(arg.sortida,f"estructura.txt")}
    """)
    if (arg.taulainfo and arg.taulainfo!='NULL') or not arg.taulainfo:
        tinf=arg.taulainfo if arg.taulainfo else "public.info_traspassos"
        execPGSQL(args.bdades,f"INSERT INTO {tinf} (esquema, capa, execucio, data, origen, fullsql, estructura) VALUES(%(esquema)s, %(capa)s, %(execucio)s, %(data)s, %(origen)s, %(fullsql)s, %(estructura)s);",
                  {'esquema':arg.esquema, 'capa':"", 'execucio':"", 'data':datetime.datetime.now(), 'origen':"", 'fullsql':gensql, 'estructura':json.dumps(dcapes, indent=4)}
                  )

def traspassaCapesAPotgres(args,iterador=None):
    from qgis.core import (QgsApplication,QgsProviderRegistry,
                        QgsProcessingFeedback,QgsVectorLayerExporter,QgsProcessingException,QgsProviderConnectionException,
                        QgsProcessingRegistry,QgsVectorLayer,QgsWkbTypes,QgsCoordinateReferenceSystem,QgsProject,QgsDataSourceUri)
    from qgis.analysis import QgsNativeAlgorithms
    import processing
    if not args.bdades:
        print("E: No s'ha proporcionat el parametre de connexio a la BD - bdades")
        return
    defin=json.loads(rarxiu(args.struct))
    dcap=dict()
    ctmp=''
    if iterador:
        dcap={c.get("ncapa"):c.get("capa") for c in iterador}
    for capa,dcapa in defin.items(): #per cada capa
        try:
            #generem una capa de sql amb els alias
            print(f"I:Processant capa {dcapa.get('arxiu')} cap a {dcapa.get('esquema','public')}.{dcapa.get('nomtaula','dad')}")
            tegeom=dcapa.get("tgeom","NOGEOM")!="NOGEOM"
            sql=dcapa.get("sqltrasp","---")
            #creem la capa temporal amb els noms de camp canviats
            #print(f"I:SQL de conversio de noms de camps:{sql}")
            sridori=dcapa.get("srid", 'EPSG:25831')
            if capa in dcap:
                sridori=dcap.get(capa).crs().authid()
            dparams={'INPUT_DATASOURCES':[dcap.get(capa,dcapa.get("arxiu"))],'INPUT_QUERY':sql,'INPUT_GEOMETRY_FIELD':dcapa.get('cgeom','geometry') if tegeom else '','INPUT_GEOMETRY_TYPE':0 if tegeom else 1,'INPUT_GEOMETRY_CRS':QgsCoordinateReferenceSystem(sridori) if tegeom else None,'OUTPUT':'TEMPORARY_OUTPUT'}
            resul=processing.run("qgis:executesql", dparams)
            if not "OUTPUT" in resul:
                print(f"E:No s'ha pogut carregar el alias per la capa:{capa} - {sql}")
                continue
            vlayer=resul.get("OUTPUT")
            # de https://github.com/qgis/QGIS/blob/b6a7a1070329a1f167341e3187781954d28f2f39/python/plugins/processing/algs/qgis/ImportIntoPostGIS.py
            #la exportem a geopackage pel ogr
            if os.path.exists(os.path.join(args.sortida,f'temporal.gpkg')):
                os.remove(os.path.join(args.sortida,f'temporal.gpkg'))
            QgsProject.instance().addMapLayer(vlayer)
            processing.run("native:package", {'LAYERS':[vlayer.source()],'OUTPUT':os.path.join(args.sortida,f'temporal.gpkg'),'OVERWRITE':False,'SAVE_STYLES':True,'SAVE_METADATA':True,'SELECTED_FEATURES_ONLY':False,'EXPORT_RELATED_LAYERS':False})
            QgsProject.instance().removeMapLayer(vlayer)
            #borrem dades previes si cal
            if args.esborradadesAnt:
                execPGSQL(args.bdades,f"truncate table {dcapa.get('esquema','public')}.{dcapa.get('nomtaula','dad')}")
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
            processed_conn_string = get_connection_string_with_env_vars(args.bdades)
            ctmp=f"ogr2ogr.exe -progress --config PG_USE_COPY YES -f PostgreSQL \"PG:{processed_conn_string}  active_schema={dcapa.get('esquema','public')} \" -lco DIM={args.dimout or '2'} \"{os.path.join(args.sortida,f'temporal.gpkg')}\" -lco SPATIAL_INDEX=OFF -append -nlt {dcapa.get('tgeom','POINT') if tegeom else 'NONE'} -lco GEOMETRY_NAME=geometry -lco FID={dcapa.get('pkorig','objectid')}  -nln {dcapa.get('esquema','public')}.{dcapa.get('nomtaula','dad')} {srs} -skipfailures"
            res=execcom(f"{ctmp}")
            if res[0]!=0:
                print(f"E:La execucio de {ctmp} no ha anat be")
                #raise  f"E:La execucio de {f} no ha anat be {res[0]}"
            # Sanitize command before logging or displaying
            sanitized_cmd = sanitize_connection_string(ctmp)
            rs=f'''
******************************************************************************************************************************        
Comanda:{sanitized_cmd}
Resultat ({res[0]}):
{res[1]}
******************************************************************************************************************************
'''
            print(rs)
            warxiu(os.path.join(args.sortida,f'fesinserts.txt'),rs,"a")
            if (args.taulainfo and args.taulainfo!='NULL') or not args.taulainfo:
                tinf=args.taulainfo if args.taulainfo else "public.info_traspassos"
                execPGSQL(args.bdades,f"INSERT INTO {tinf} (esquema, capa, execucio, data, origen, fullsql, estructura) VALUES(%(esquema)s, %(capa)s, %(execucio)s, %(data)s, %(origen)s, %(fullsql)s, %(estructura)s);",
                        {'esquema':dcapa.get('esquema','public'), 'capa':dcapa.get('nomtaula','dad'), 'execucio':rs, 'data':datetime.datetime.now(), 'origen':capa, 'fullsql':"", 'estructura':json.dumps(dcapa, indent=4)}
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
        
def remapejaOrigensDades(lfits,args):    
    from qgis.core import (QgsApplication,
                        QgsProcessingFeedback,
                        QgsProcessingRegistry,QgsVectorLayer,QgsWkbTypes,QgsProject,QgsDataProvider)
    from qgis.analysis import QgsNativeAlgorithms
    dcs=dict()
    dtm=json.loads(rarxiu(args.remap))    
    #diccionari amb les equivalencies de source amb la definicio de taula
    for x in dtm:
        dcs[dtm[x]["source"]] = dtm[x]
    # per cada capa mirem si el source es un dels cercats i li canvia el source
    for v in lfits:
       cap=v["capa"]
       nso=cap.dataProvider().dataSourceUri()
       if nso in dcs:           
           desti=args.noudesti.replace("$sh$",f'"{dcs[nso]["esquema"]}"').replace("$tb$",f'"{dcs[nso]["nomtaula"]}"').replace("$cg$",dcs[nso]["cgeomdest"])
           cap.setDataSource(desti,cap.name(),"postgres",QgsDataProvider.ProviderOptions())
           if cap.isValid():
               print(f"Capa {cap.name()} amb origen [{nso}] canviat a [{desti}]")
           else:
               print(f"Capa {cap.name()} amb origen [{nso}] NO canviat a [{desti}]")
       else:
           print(f"Capa {cap.name()} amb origen [{nso}] no trobada ")
    nfs=args.arxiuqgis+".mod.qgz"
    rg=QgsProject.instance().write(nfs)
    if not rg:
        print(f"Error al grabar l'arxiu, {QgsProject.instance().error()}")
    else:
        print(f"Arxiu  desat correctament a {nfs}")
    #QgsProject.instance().close()    

def remapejaOrigensDadesDirecte(lfits,args):    
    from qgis.core import (QgsApplication,
                        QgsProcessingFeedback,
                        QgsProcessingRegistry,QgsVectorLayer,QgsWkbTypes,QgsProject,QgsDataProvider)
    from qgis.analysis import QgsNativeAlgorithms
    dcs=dict()
    dtm=json.loads(rarxiu(args.remaparxiu))    
    # per cada capa mirem si el source es un dels cercats i li canvia el source
    for v in lfits:
       cap=v["capa"]
       nso=cap.dataProvider().dataSourceUri()
       for k in dtm.keys():
           if nso.endswith(f"|layername={k}"):
                parts = dtm[k]["taula"].split('.')
                if len(parts) == 2:
                    esquema = parts[0]
                    taula = parts[1]
                    desti = args.noudesti.replace("$sh$", f'"{esquema}"').replace("$tb$", f'"{taula}"').replace("$cg$",dtm[k]["cg"])
                    comentari=f"{v["coment"]}. Origen: {nso} "
                    cap.setDataSource(desti, cap.name(), "postgres", QgsDataProvider.ProviderOptions())
                    if cap.isValid():
                        print(f"Capa {cap.name()} amb origen [{nso}] canviat a [{desti}]")
                    else:
                        print(f"Capa {cap.name()} amb origen [{nso}] NO canviat a [{desti}]")                                            
                    telem="TABLE"
                    if "tip" in dtm[k] :
                        telem=dtm[k]["tip"]
                    ensqlcoments=f"""COMMENT ON {telem} {esquema}.{taula} IS '{comentari.replace("'","''")}';"""        
                    if args.bdades:
                        execPGSQL(args.bdades,ensqlcoments)
                    else:
                        print(ensqlcoments)
                else:
                    print(f"Format de clau incorrecte: {k} - s'esperava format 'esquema.taula'")
    nfs=args.arxiuqgis+".mod.qgz"
    rg=QgsProject.instance().write(nfs)
    if not rg:
        print(f"Error al grabar l'arxiu, {QgsProject.instance().error()}")
    else:
        print(f"Arxiu  desat correctament a {nfs}")
    #QgsProject.instance().close()    

def remapejaAGeopackage(lfits,args):    
    from qgis.core import (QgsApplication,
                        QgsProcessingFeedback,
                        QgsProcessingRegistry,QgsVectorLayer,QgsWkbTypes,QgsProject,QgsDataProvider)
    from qgis.analysis import QgsNativeAlgorithms
    import processing
    lcapes=list()
    
    
    if not os.path.isabs(args.remapgpkg):
        os.chdir(os.path.dirname(os.path.abspath(args.arxiuqgis)))        
    if os.path.exists(args.remapgpkg):
        raise f"L'arxiu geopackage {os.path.abspath(args.remapgpkg)} ja existeix, no sobreescrobim"
    # per cada capa mirem si el source es un dels cercats i li canvia el source
    for v in lfits:
       cap=v["capa"]
       if True:  
            if v.get("vis",True) :
               lcapes.append(cap)
               print(f"Capa {cap.name()} canviat")
            else:
               print(f"Capa {cap.name()}  NO canviat ")
       else:
           print(f"Capa {cap.name()} amb origen [{nso}] no trobada ")
    llcapm=processing.run("native:package", {'LAYERS':lcapes,'OUTPUT':args.remapgpkg,'OVERWRITE':False,'SAVE_STYLES':True,'SAVE_METADATA':True,'SELECTED_FEATURES_ONLY':False,'EXPORT_RELATED_LAYERS':False})
    for i,ca in enumerate(lcapes):
        ca.setDataSource(llcapm["OUTPUT_LAYERS"][i],ca.name(),'ogr')
    nfs=args.arxiuqgis+".mod.qgz"
    rg=QgsProject.instance().write(nfs)
    if not rg:
        print(f"Error al grabar l'arxiu, {QgsProject.instance().error()}")
    else:
        print(f"Arxiu  desat correctament a {nfs}")
   



#https://github.com/qgis/QGIS/tree/b6a7a1070329a1f167341e3187781954d28f2f39/python/plugins/processing/algs/qgis
    
#pyqgis 
#e:\TNetAjt\python\ImportaSHP\postgresql-12.13-1-windows-x64-binaries\pgsql\bin\psql.exe  -W -f e:\TNetAjt\python\ImportaSHP\sortida\fesinserts.txt  -h 192.40.165.204  gisvendrell udisseny

#https://www.geomapik.com/desarrollo-programacion-gis/instalacion-de-paquetes-de-python-para-qgis/
print("Atenció, cal executar en el python del qgis, exemple des de osgeo shell!!!")
print("Atenció, cal haver instalat psycopg2 des del OSGeo4W Shell : pip install psycopg2")
parser = argparse.ArgumentParser(description="Genera estructura de shapes")

for t in (
("carpetashp","Carpeta que ha de contenir els shapefiles a analitzar"),
("carpetakmz","Carpeta que ha de contenir els kmz a analitzar"),
("configqgis","carpeta amb la configuració extreta del qgis"),
("arxiu_env","Arxiu INI amb variables d'entorn per substituir referències environ['VAR'] en cadenes de connexió. Si no s'especifica, es farà servir el fitxer .env del directori actual si existeix. Format: [env]\\nVAR1=valor1\\nVAR2=valor2"),
("geopackage","Geopackage amb les capes a analitzar"),
("carppkg","Carpeta amb varis Geopackages o kmz  amb les capes a analitzar"),
("llistaurl","Arxiu amb la llista d'URL de capa a processar"),
("arxiuqgis","Arxiu de qgis que ens processara totes les capes"),
("sortida","carpeta on es generarà tot el scripting"),
("esquema","esquema de destí de la base de dades"),
("pkorigen","nom del camp primary key de les dades orige, defecte: objectid"),
("tradtau","si existeix, contindrà noms de fitxers amb la seva equivalencia al nom de taula: (nomfitxer_sense_path_ni_extensio:nomtaula|)+"),
("llperms","llista de permisos que cal afegir a les taules generades , amb forma de [s|a]:usuari[,] per asignar select o all"),
("sriddest","srid de les taules generades al postgres, defecte 25831, codi enter de la pk de la taula spatial_ref_sys que coincideix amb el EPSG:XXXXXX"),
("sriddestogr","nom de la codificacio final del ogr2ogr , per defecte EPSG:{sriddest}"),
("dimout","[2|3] dimensions de sortida, defecte 2"),
("enc","Codificacio de les cadenes resultants, defecte 'UTF-8' però si alguna taula dona errors de conversió es pot posar concretament aquella com a 'LATIN1'"),
("struct","Si existeix fara la importacio de l'estructura generada estructura.txt "),
("remap","Si existeix fara el remapeig dels origens de dades especificats a  estructura.txt donat a aquest parametre al nou url especificat $sh$ i $tb$ al parametre noudesti de l'arxiu especificat a arxiuqgis"),
("remaparxiu","Arxiu d'equivalencies, que si es dona, indicarà el nom de la capa de l'origen GEOPACKAGE i la taula a postgis de desti i en farà el remapeig"),
("remapgpkg","Si existeix fara el remapeig dels origens de dades de l'arxiu qgis especificat capes actives a un sol geopackage amb el path especificat a aquest parametre amb totes les dades, si el path del geopackage QUE NO HA D'EXISTIR es relatiu, el farà relatiu al arxiu qgis perque quedi lligat aixó i es pugui moure"),
("pthpwdbd","Path de la carpeta del authdb per les operacions que han d'accedir a l'autentificació"),
("pwdpwbd","Fitxer amb el password del authdb"),
("noudesti","Plantilla de cadena de connexio nova on es sustitueix $sh$ pel nom del esquema i $tb$ pel nom de la taula i $cg$ pe camp geom"),
("bdades","Part de la conexio uri de postgre a la bd sense table ni camps ni indexos. IMPORTANT: Utilitzar credencials segures o variables d'entorn, no incloure contrasenyes en text pla. Ex: dbname='mydb' host=hostname port=5432 user='username' password=environ['DB_PASSWORD'] sslmode=disable"),
("esborradadesAnt","Esborra les dades abans de fer la inserció via el ogr"),
("autonom","Genera noms de capes numeriques amb la particula especificada, generarà numeric seqüencial i és important que hi hagi comentaris a cada capa per saber el que és"),
("taulainfo","Si està en blanc fara el taulainfo a public.info_traspassos, si hi ha un nom fara el taula info al nom  i si hi ha NULL no el farà")
):
    parser.add_argument(f"--{t[0]}",type=str,help=f"{t[1]}")


args=parser.parse_args()  
if not args.carpetashp and not args.geopackage and not args.carppkg and not args.struct and not args.llistaurl and not args.arxiuqgis:
    print("E:No s'ha informat de carpetashp o geopackage o struct o llistaurl o arxiuqgis a importar, no es processa res")
    exit(1)

# Estableix la variable global arxiuenv
# 1. Si s'ha especificat a la línia de comandes, utilitzar aquest
# 2. Si no, comprovar si existeix .env al directori actual
if args.arxiu_env:
    arxiuenv = args.arxiu_env
    print(f"Utilitzant arxiu d'entorn especificat: {arxiuenv}")
else:
    # Comprova si existeix un arxiu .env al directori actual
    env_path = os.path.join(os.getcwd(), '.env')
    if os.path.isfile(env_path):
        arxiuenv = env_path
        print(f"Utilitzant arxiu d'entorn per defecte: {arxiuenv}")


initEntornQGis(args.configqgis,args)

if args.struct:
    iter=IteradorCapesProjecte(args.arxiuqgis,args,False) if args.arxiuqgis else None
    traspassaCapesAPotgres(args,iter)
elif args.remap:
    lfits=IteradorCapesProjecte(args.arxiuqgis,args,True)
    remapejaOrigensDades(lfits,args)
elif args.remaparxiu:
    lfits=IteradorCapesProjecte(args.arxiuqgis,args,True)
    remapejaOrigensDadesDirecte(lfits,args)    
elif args.remapgpkg:
    lfits=IteradorCapesProjecte(args.arxiuqgis,args,False)
    remapejaAGeopackage(lfits,args)    
else:
    if args.carpetashp:
        lfits=IteradorCapesLLista([{"pth":os.path.join(args.carpetashp,n),"ncapa":espmin(os.path.splitext(os.path.basename(n))[0])} for n in  filter(lambda n:n.lower().find(".shp")>-1, os.listdir(args.carpetashp))])
    if args.carpetakmz:
        lfits=IteradorCapesLLista([{"pth":os.path.join(args.carpetashp,n),"ncapa":espmin(os.path.splitext(os.path.basename(n))[0])} for n in  filter(lambda n:n.lower().find(".kmz")>-1, os.listdir(args.carpetashp))])
    if args.geopackage:
        lfits=ObteCapesGeoPackageSimple(args.geopackage)    
    if args.carppkg:
        lfits=ObteCapesGeoPackage(args.carppkg)    
    if args.llistaurl:
        lfits=[]
        for  l  in rarxiu(args.llistaurl).splitlines():
            pa=l.split(',',1)
            lfits.append({"ncapa":pa[0],"pth":pa[1] if len(pa)>1 else pa[0]})    
    if args.arxiuqgis:
        lfits=IteradorCapesProjecte(args.arxiuqgis,args,False)    
    generaCreateTablesIEquivalencies(lfits,args)

qgs.exitQgis()

#xuleta pyqgis
#https://github.com/All4Gis/QGIS-cheat-sheet/blob/master/QGIS3.md




