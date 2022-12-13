import json
from datetime import datetime
import avro
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
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

def AVRO_to_df(path:str):
    aux_list=[]
    reader = DataFileReader(open(path, "rb"), DatumReader())
    for element in reader:
        aux_list.append(element)
    reader.close()
    output_df=pd.DataFrame(aux_list)

    return output_df

def insert_data(table_name:str,data_frame:pd,Original_id:bool):

    fields=data_frame.columns
    
    if Original_id==False:#copia o no el campo id
        fields=fields[1::]#quitamos id pq es autoincremental(id)
   
    #setup of fields:
    
    col_names_to_db=""
    for field in fields:
        col_names_to_db=col_names_to_db+str(field)+","
    col_names_to_db = col_names_to_db[:-1]#quitamos la ultima coma
    
    #print(col_names_to_db)
    for row in range(data_frame.shape[0]):


     
        row_list=list(data_frame.iloc[row])

        if Original_id==False:#copia o no el campo id
            row_list=row_list[1::]#eliminamos el elemento id, la base es autoincremental
        
        row_info_to_db=""
        for element in row_list:
            element=str(element)
            if str.isdigit(element)==True:
                #si el elemento es convertible a entero no se agregan comillas
                row_info_to_db=row_info_to_db+element+","
            else:
                #si el elemento no es convertible a entero se agregan comillas
                element="'"+element+"'"
                row_info_to_db=row_info_to_db+element+","
        row_info_to_db = row_info_to_db[:-1]#eliminamos la ultima coma

        query='INSERT INTO '+str(table_name)+' ('+col_names_to_db+') '
        query+='VALUES ('+str(row_info_to_db)+')'

        print("[INFO]Se agregoron los valores ",row_info_to_db," en los campos ",col_names_to_db," de la tabla", table_name)
        #print("QUERY:",query)
        db_conection("send",query)#enviamos la query

""""
#Departments
departments_backup=[]       
reader = DataFileReader(open("department.avro", "rb"), DatumReader())
for element in reader:
    #print (element)
    departments_backup.append(element)
reader.close()

#Jobs
jobs_backup=[]
reader = DataFileReader(open("jobs.avro", "rb"), DatumReader())
for element in reader:
    #print (element)
    jobs_backup.append(element)
reader.close()

#Hired_employees
hired_employees_backup=[]
reader = DataFileReader(open("hired_employees.avro", "rb"), DatumReader())
for element in reader:
    #print (element)
    hired_employees_backup.append(element)
reader.close()

hired_employees_backup=pd.DataFrame(hired_employees_backup)
#print(hired_employees_backup)
"""

#creamos la tabla departments

query='CREATE TABLE "departments"'
query+='('
query+='"id" INTEGER NOT NULL UNIQUE PRIMARY KEY AUTOINCREMENT, '
query+='"department" TEXT NOT NULL'
query+=')'

db_conection("send",query)#enviamos la query

#creamos la tabla jobs

query='CREATE TABLE "jobs" '
query+='('
query+='"id" INTEGER NOT NULL UNIQUE PRIMARY KEY AUTOINCREMENT, '
query+='"job" TEXT NOT NULL'
query+=')'

db_conection("send",query)#enviamos la query

#creamos la tabla hired_employees

query='CREATE TABLE "hired_employees" ' 
query+='( '
query+='"id" INTEGER NOT NULL UNIQUE PRIMARY KEY AUTOINCREMENT, '
query+='"name"	TEXT NOT NULL, '
query+='"datetime"	TEXT NOT NULL, '
query+='"department_id" INTEGER NOT NULL, '
query+='"job_id"	INTEGER NOT NULL, '
query+='FOREIGN KEY("job_id") REFERENCES "jobs"("id"), '
query+='FOREIGN KEY("department_id") REFERENCES "departments"("id")'
query+=')'
	
db_conection("send",query)#enviamos la query

departments_df=AVRO_to_df("departments.avro")

insert_data("departments",departments_df,False)

jobs=AVRO_to_df("jobs.avro")

insert_data("jobs",jobs,False)

hired_employees_df=AVRO_to_df("hired_employees.avro")

insert_data("hired_employees",hired_employees_df,True)