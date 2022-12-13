import json
from datetime import datetime
import avro
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import sqlite3
import pandas as pd

def db_conection(conn_type="receive",query="None"):

    #Esta funcion nos conecta a la BD
    #db_conexion nos permitira contectarnos a "employees.db",
    #en caso de no existir, crea la BBDD

    db_conexion=sqlite3.connect("employees.db")

    if conn_type=="receive":
        #Retorna un diccionario con la respuesta de la query
        pd_df=pd.read_sql(query,db_conexion)
        dict_df=pd_df.to_dict(orient="records")
        return dict_df

    if conn_type=="send":
        #Envia una query con ordenes a la BD
        cursor=db_conexion.cursor()
        cursor.execute(query)
        db_conexion.commit()
        cursor.close()

def SQL_Table_to_AVRO(table_name:str,schema_name:str,output_name:str):
    """
    Esta funcion nos permite hacer backups en formato AVRO de Tablas de una BBDD;
    table_name:str -> Nombre de la Tabla
    schema_name:str -> Direccion del archivo ".avsc" que contiene la estructura de datos necesaria para armar el AVRO
    output_name:str -> Nombre del archivo ".avro" que contiene el backup, es necesario agragar la extension
    """

    query="SELECT  * from "+table_name#query de busqueda de datos
    response=db_conection("receive",query)#solicitud de los datos a la BBDD
    schema = avro.schema.parse(open(schema_name, "rb").read())#adecuacion del esquema
    writer = DataFileWriter(open(output_name, "wb"), DatumWriter(), schema)#AVRO file writer
    for element in response:
        writer.append(element)#agregamos los elementos respuesta del query al archivo AVRO
    writer.close()

SQL_Table_to_AVRO("departments","departments_schema.avsc","departments.avro")
SQL_Table_to_AVRO("jobs","jobs_schema.avsc","jobs.avro")
SQL_Table_to_AVRO("hired_employees","hired_employees_schema.avsc","hired_employees.avro")

"""
#Departmen table backup:
        
query="SELECT  * from departments"#query de busqueda de datos
response=db_conection("receive",query)#solicitud de los datos a la BBDD

departments_schema="departments_schema.avsc"#esquema de archivo AVRO
schema = avro.schema.parse(open(departments_schema, "rb").read())#adecuacion del esquema
writer = DataFileWriter(open("departments.avro", "wb"), DatumWriter(), schema)#AVRO file writer

for element in response:#agregamos los elementos respuesta del query al archivo AVRO
    writer.append(element)
writer.close()

#Jobs table backup:
        
query="SELECT  * from jobs"#query de busqueda de datos
response=db_conection("receive",query)#solicitud de los datos a la BBDD

jobs_schema="jobs_schema.avsc"#esquema de archivo AVRO
schema = avro.schema.parse(open(jobs_schema, "rb").read())#adecuacion del esquema
writer = DataFileWriter(open("jobs.avro", "wb"), DatumWriter(), schema)#AVRO file writer

for element in response:#agregamos los elementos respuesta del query al archivo AVRO
    writer.append(element)

writer.close()

#Employees table backup:
        
query="SELECT  * from hired_employees"#query de busqueda de datos
response=db_conection("receive",query)#solicitud de los datos a la BBDD

hired_employees="hired_employees_schema.avsc"#esquema de archivo AVRO
schema = avro.schema.parse(open(hired_employees, "rb").read())#adecuacion del esquema
writer = DataFileWriter(open("hired_employees.avro", "wb"), DatumWriter(), schema)#AVRO file writer

for element in response:#agregamos los elementos respuesta del query al archivo AVRO
    writer.append(element)

writer.close()
"""