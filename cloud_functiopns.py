from google.cloud import bigquery
import textdistance
import asyncio
import json
from fastapi import FastAPI
from datetime import datetime

##Creamos Coneccion a Conjunto de datos
client = bigquery.Client()

def conn_to_bigquery(request):
    content_type = request.headers['content-type']
    print(content_type)
    if content_type == 'application/json':
        request_json = request.get_json(silent=True)
        if request_json and 'Rezagos' in request_json:
            name = request_json['Rezagos']
        else:
            raise ValueError("JSON invalid, or missing a 'Rezagos' property")
    elif content_type == 'application/x-www-form-urlencoded':
        name = request.form.get('Rezagos')

    Rezagos = json.loads(request_json['Rezagos'])
    Clientes = json.loads(request_json['Clientes'])


    # sRezago = request.args.get('sRezago')
    # eRezago = request.args.get('eRezago')
    # sCliente = request.args.get('sCliente')
    # eCliente = request.args.get('eCliente')

    # ##Creamos consultas
    # queryClientes = """
    #     SELECT 
    #     IdPersona, ApPaterno, ApMaterno,Nombre, FecNacimiento, FecAfiliacionSistema
    #     FROM (
    #         SELECT ROW_NUMBER() OVER(ORDER BY IdPersona DESC) AS Fila,
    #         ifnull(IdPersona,'') as IdPersona,  
    #         ifnull(ApPaterno,'') as ApPaterno, 
    #         ifnull(ApMaterno,'') as ApMaterno,
    #         ifnull(Nombre,'') as Nombre, 
    #         ifnull(FecNacimiento,'1900-01-01 00:00:00') as FecNacimiento, 
    #         ifnull(FecAfiliacionSistema,'1900-01-01 00:00:00') as FecAfiliacionSistema

    #         Where Fila BETWEEN {} AND {}
    # """.format(sCliente,eCliente)

    # queryRezagos = """
    #     SELECT NumRezago,IdCliente,IdPersona,ApPaterno,ApMaterno,Nombre,IdPersonaOri,ApPaternoOri,ApMaternoOri,NombreOri,TipoEntidadPagadora,IdEmpleador,PerCotiza,ValMlRentaImponible,FecOperacion
	# 			,CodMvto,ValMlMonto,ValMlAdicional,ValMlPrimaseguro,ValCuoMonto,ValMlExcesoAfi,ValMlExcesoEmp,TipoRezago,EstadoRezago,CodCausalRezago,FecIngReg,TipoProducto,TipoPlanilla
	# 			,ToCharFolioPlanilla,TipoOrigenDigitacion,TipoRemuneracion,NumPlanilla,FecValorCuota,ValMlValorCuota,CodOrigenProceso,IdUsuarioIngReg
    #     FROM 
	# 	(SELECT ROW_NUMBER() OVER(ORDER BY IdPersona DESC) AS Fila,
	# 		 Cast(ifnull(NumRezago,0) as int) as NumRezago
	# 		,Cast(ifnull(IdCliente,0) as int) as IdCliente
	# 		,ifnull(IdPersona,'') as IdPersona
	# 		,ifnull(ApPaterno,'') as ApPaterno
	# 		,ifnull(ApMaterno,'') as ApMaterno
	# 		,ifnull(Nombre,'') as Nombre
	# 		,ifnull(IdPersonaOri,'') as IdPersonaOri
	# 		,ifnull(ApPaternoOri,'') as ApPaternoOri
	# 		,ifnull(ApMaternoOri,'') as ApMaternoOri
	# 		,ifnull(NombreOri,'') as NombreOri
	# 		,ifnull(TipoEntidadPagadora  ,'') as TipoEntidadPagadora
	# 		,ifnull(IdEmpleador ,'') as IdEmpleador
	# 		,date(ifnull(PerCotiza,'1900-01-01 00:00:00')) as PerCotiza
	# 		,Cast(ifnull(ValMlRentaImponible  ,0) as int) as ValMlRentaImponible
	# 		,date(ifnull(FecOperacion,'1900-01-01 00:00:00')) as FecOperacion
	# 		,ifnull(CodMvto  ,'') as CodMvto
	# 		,Cast(ifnull(ValMlMonto  ,0) as int) as ValMlMonto
	# 		,Cast(ifnull(ValMlAdicional ,0) as int) as ValMlAdicional
	# 		,Cast(ifnull(ValMlPrimaseguro  ,0) as int) as ValMlPrimaseguro
	# 		,Cast(ifnull(ValCuoMonto ,0) as int) as ValCuoMonto
	# 		,Cast(ifnull(ValMlExcesoAfi ,0) as int) as ValMlExcesoAfi
	# 		,Cast(ifnull(ValMlExcesoEmp ,0) as int) as ValMlExcesoEmp
	# 		,Cast(ifnull(TipoRezago  ,0) as int) as TipoRezago
	# 		,ifnull(EstadoRezago,'') as EstadoRezago
	# 		,Cast(ifnull(CodCausalRezago,0) as int) as CodCausalRezago
	# 		,date(ifnull(FecIngReg,'1900-01-01 00:00:00')) as FecIngReg
	# 		,ifnull(TipoProducto,'') as TipoProducto
	# 		,Cast(ifnull(TipoPlanilla,0) as int) as TipoPlanilla
	# 		,Cast(ifnull(ToCharFolioPlanilla  ,0) as int) as ToCharFolioPlanilla
	# 		,ifnull(TipoOrigenDigitacion ,'') as TipoOrigenDigitacion
	# 		,ifnull(TipoRemuneracion  ,'') as TipoRemuneracion
	# 		,Cast(ifnull(NumPlanilla ,0) as int) as NumPlanilla
	# 		,date(ifnull(FecValorCuota  ,'1900-01-01 00:00:00')) as FecValorCuota
	# 		,Cast(ifnull(ValMlValorCuota,0) as int) as ValMlValorCuota
	# 		,ifnull(CodOrigenProceso  ,'') as CodOrigenProceso
	# 		,ifnull(IdUsuarioIngReg,'') as IdUsuarioIngReg
	# 	   
    #     Where Fila BETWEEN {} AND {}
    # """.format(sRezago,eRezago)


    # ##Ejecutamos consultas a Base de datos
    # Clientes = client.query(queryClientes)
    # Rezagos = client.query(queryRezagos)

    ## Instancia tabla de resultados

    ##Creamos variable para almacenar en tabla
    Resultados = []
    ##Iniciamos variable de porcentaje minimo
    minimo = 0
    ##Creamos tabla de resultado
    # crea_tabla(table_id)

    ##por cada cliente recorremos registros de rezagos
    for Rezago in Rezagos:
        clienteRut=''
        clienteNom=''
        clienteAPM=''
        clienteAPP=''
        clienteAlg1=0
        clienteAlg2=0
        cliente2Rut=''
        cliente2Nom=''
        cliente2APM=''
        cliente2APP=''
        cliente2Alg1=0
        cliente2Alg2=0
        ##Generamos cadena a evaluar para cada resitro de rezago
        Id_Rezago1 = str(Rezago['IdPersona']).lstrip("0")
        Cadena_Rezago1 = str(Rezago['ApPaterno']+' '+str(Rezago['ApMaterno'])+' '+str(Rezago['Nombre']))
        #Recorremos registros de clientes
        for Cliente in Clientes:
            #Generamos cadena a evaluar para cada resitro de cliente
            Id_Cliente = str(Cliente['IdPersona']).lstrip("0")
            Cadena_Cliente = str(Cliente['ApPaterno']+' '+str(Cliente['ApMaterno'])+' '+str(Cliente['Nombre']))

        
            ##Realizamos ejecucion de los algoritamos para las cadenas
            cosine1 = int(textdistance.cosine.normalized_similarity(Cadena_Cliente, Cadena_Rezago1)*100)
            if cosine1 > minimo:
                cosineId1 = int(textdistance.cosine.normalized_similarity(Id_Cliente, Id_Rezago1)*100)
                if (cosine1+cosineId1) > (clienteAlg1+clienteAlg2):                
                    clienteRut = str(Cliente['IdPersona'])
                    clienteAPP = str(Cliente['ApPaterno'])
                    clienteAPM = str(Cliente['ApMaterno'])
                    clienteNom = str(Cliente['Nombre'])
                    clienteAlg1 =   cosine1
                    clienteAlg2 =   cosineId1 
                elif (cosine1+cosineId1) > (cliente2Alg1+cliente2Alg2):                
                    cliente2Rut = str(Cliente['IdPersona'])
                    cliente2APP = str(Cliente['ApPaterno'])
                    cliente2APM = str(Cliente['ApMaterno'])
                    cliente2Nom = str(Cliente['Nombre'])
                    cliente2Alg1 =   cosine1
                    cliente2Alg2 =   cosineId1 


        if clienteAlg1 > 0:
            ##Almacenamos el rezago en objeto 
            Resultado = {
            "NumRezago":str(Rezago['NumRezago'])
            ,"RutRezagos":str(Id_Rezago1)
            ,"NumRutRezagos":str(Id_Rezago1)[0:-1]
            ,"DVRutRezagos":str(Id_Rezago1)[-1]
            ,"ApPaternoRezagos":str(Rezago['ApPaterno'])
            ,"ApMaternoRezagos":str(Rezago['ApMaterno'])
            ,"NombreRezagos":str(Rezago['Nombre'])
            ,"RutCliente":clienteRut
            ,"ApPaternoCliente":clienteAPP
            ,"ApMaternoCliente":clienteAPM
            ,"NombreCliente":clienteNom
            ,"SimilitudRut":clienteAlg2
            ,"SimilitudNombre":clienteAlg1
            ,"RutCliente2":cliente2Rut
            ,"ApPaternoCliente2":cliente2APP
            ,"ApMaternoCliente2":cliente2APM
            ,"NombreCliente2":cliente2Nom
            ,"SimilitudRut2":cliente2Alg2
            ,"SimilitudNombre2":cliente2Alg1
            ,"FecNacimiento":""
            ,"PerCotiza":str(Rezago['PerCotiza'])
            ,"IdEmpleador":str(Rezago['IdEmpleador'])
            ,"TipoProducto":str(Rezago['TipoProducto'])
            ,"TipoRezago":str(Rezago['TipoRezago'])
            ,"CodCausalRezago":Rezago['CodCausalRezago']
            ,"CodMvto":str(Rezago['CodMvto'])
            ,"ValMlRentaImponible":str(Rezago['ValMlRentaImponible'])
            ,"ValMlMonto":str(Rezago['ValMlMonto'])
            ,"ValCuoMonto":str(Rezago['ValCuoMonto'])
            ,"Algoritmo":"coseno"
            ,"CodAlgoritmo":2
            }
            
            ##agregamos resultado a variable para almacenar en tabla
            Resultados.append(Resultado)
    ##Se imprime resultado en consola
    print( "Resultados:  {}".format(Resultados))
    # ##insertamos resultados en BigQuery
    if len(Resultados) > 0:
        run_insert(Resultados,table_id)
    # print(sRezago,eRezago ,sCliente,eCliente)
    return "OK"
    
def crea_tabla(table_id):    
    # borra_tabla(table_id)
    schema = [
         bigquery.SchemaField("NumRezago","INT64"), 
         bigquery.SchemaField("RutRezagos","STRING"),
         bigquery.SchemaField("NumRutRezagos","INT64"), 
         bigquery.SchemaField("DVRutRezagos","STRING"),
         bigquery.SchemaField("ApPaternoRezagos","STRING"),
         bigquery.SchemaField("ApMaternoRezagos","STRING"),
         bigquery.SchemaField("NombreRezagos","STRING"),
         bigquery.SchemaField("RutCliente","STRING"),
         bigquery.SchemaField("ApPaternoCliente","STRING"),
         bigquery.SchemaField("ApMaternoCliente","STRING"),
         bigquery.SchemaField("NombreCliente","STRING"),
         bigquery.SchemaField("SimilitudRut","STRING"),
         bigquery.SchemaField("SimilitudNombre","STRING"),
         bigquery.SchemaField("RutCliente2","STRING"),
         bigquery.SchemaField("ApPaternoCliente2","STRING"),
         bigquery.SchemaField("ApMaternoCliente2","STRING"),
         bigquery.SchemaField("NombreCliente2","STRING"),
         bigquery.SchemaField("SimilitudRut2","STRING"),
         bigquery.SchemaField("SimilitudNombre2","STRING"),
         bigquery.SchemaField("FecNacimiento","STRING"), 
         bigquery.SchemaField("PerCotiza","STRING"), 
         bigquery.SchemaField("IdEmpleador","STRING"),
         bigquery.SchemaField("TipoProducto","STRING"),
         bigquery.SchemaField("TipoRezago","INT64"), 
         bigquery.SchemaField("CodCausalRezago","INT64"),
         bigquery.SchemaField("CodMvto","STRING"),
         bigquery.SchemaField("ValMlRentaImponible","STRING"),
         bigquery.SchemaField("ValMlMonto","STRING"),
         bigquery.SchemaField("ValCuoMonto","STRING"),
         bigquery.SchemaField("Algoritmo","STRING"),
         bigquery.SchemaField("CodAlgoritmo","INT64"), 
         bigquery.SchemaField("Periodo","INT64"), 
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

def borra_tabla(table_id):    
    client.delete_table(table_id, not_found_ok=True)
    print("Deleted table '{}'.".format(table_id))



def run_insert(data,table_id):
    errors = client.insert_rows_json(table_id, data)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))    
