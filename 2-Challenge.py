from fastapi import FastAPI
import sqlite3
import pandas as pd
from pydantic import BaseModel
import datetime as dt
from typing import Any, Dict, List, Union
from fastapi import Body, FastAPI
from typing import List
import random
from typing import Optional
from datetime import date, timedelta, datetime

class Hired_employees(BaseModel):
 
    name:str
    department_id:int
    job_id:int

class N_Hired_employees(BaseModel):
    
    data:List[Hired_employees]

#Only for Special Endpoint

class Random_employees(BaseModel):

    number_of_employees:int
    """
    class Config:
        schema_extra = {
            "example": { "insert_a_number_of_random_employees": 1000
            }
        }
    """

def Random_date():
    """Crea dias aleatorios para la facura"""
    first_datetime= datetime.fromisoformat('2021-01-01')
    last_datetime=datetime.today()
    delta_datetime=(last_datetime-first_datetime).days
    random_days=random.randint(1,delta_datetime)
    random_date=first_datetime+dt.timedelta(days=random_days)
    random_date=random_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    return random_date

def Random_name():
    """Devuelve un nombre aleatoria de la lista"""
    random_names=["Juan Manuel","Juan Domingo","Carlos Saul","Juan Manuel",
    "Jorge Rafael","Julio Argentino","Maria Estela","Raul Ricardo",
    "Juan Bautista","Domingo Faustino","Arturo Umberto","Fernando"]
    random_name=random.choice(random_names)
    return random_name

def Random_Id(table):
    """Devuelve un Id aleatorio dentro de los margenes de la BD, se actualiza conforme cambian los registros"""
    query='SELECT MIN('+table+'.Id),max('+table+'.Id) FROM '+table
    response=db_conection("receive",query)
    lim_inf=response[0]["MIN("+table+".Id)"]
    lim_sup=response[0]["max("+table+".Id)"]
    Id=random.randint(lim_inf,lim_sup)
    return Id

def Table_column_to_list(table,column):
    """Ejecuta un Query, busca una columna especifica y la devuelve en forma de lista"""
    query='SELECT '+column+' from '+table
    response=db_conection("receive",query)
    lista=[]
    for field in response:
        lista.append(list(field.values())[0])
 
    
    return lista



   

app=FastAPI(title="Hired employees manager",description="API para administracion de empleados",version=0.1)

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

@app.get("/",tags=[""], include_in_schema=False)
def home():
    """Devuelve un mensaje de bienvenida al proyecto"""
    return {"Welcome Hired employees manager, please visit /docs"}

#####################################################################################################################################
#Jobs
#####################################################################################################################################

@app.get("/jobs/",tags=["Jobs"])
def all_jobs():

    query="SELECT  * from jobs"
    response=db_conection("receive",query)
    
    return response

@app.post("/jobs/",tags=["Jobs"])
def Insert_a_job(new_job:str):
   
    query="INSERT INTO jobs (job) VALUES ('"+new_job+"')"
    db_conection("send",query)

    response="The new job called "+new_job+" has been added"

    return {response}

@app.put("/jobs/{id}",tags=["Jobs"])
def Update_a_job(id:int,update_job:str):

    query="UPDATE jobs SET job='"+update_job+"' WHERE id = "+str(id)     
    db_conection("send",query)
    response="The new job with id="+str(id)+" has been updated to "+update_job

    return {response}

#####################################################################################################################################
#Departments
#####################################################################################################################################

@app.get("/departments/",tags=["Departments"])
def all_departments():

    query="SELECT  * from departments"
    response=db_conection("receive",query)
    
    return response

@app.post("/departments/",tags=["Departments"])
def Insert_a_department(new_department:str):
   
    query="INSERT INTO departments (department) VALUES ('"+new_department+"')"
    db_conection("send",query)

    response="The new department called "+new_department+" has been added"

    return {response}

@app.put("/departments/{id}",tags=["Departments"])
def Update_a_department(id:int,update_dep:str):

    query="UPDATE departments SET department='"+update_dep+"' WHERE id = "+str(id)     
    db_conection("send",query)
    response="The new department with id="+str(id)+" has been updated to "+update_dep

    return {response}

#####################################################################################################################################
#Hired_employees
#####################################################################################################################################

@app.get("/hired_employees/",tags=["Hired employees"])
def check_all_hired_employees():

    query="SELECT hired_employees.id, hired_employees.name, hired_employees.datetime, departments.department, jobs.job "
    query+="from hired_employees "
    query+="INNER JOIN departments on hired_employees.department_id=departments.id "
    query+="INNER JOIN jobs on hired_employees.job_id=jobs.id"

    response=db_conection("receive",query)
    
    return response
"""
@app.post("/hired_employees/",tags=["Hired employees"])
def Insert_a_new_employee(new_employee:Hired_employees):

    put_datetime=dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    put_name=new_employee.name
    put_department=new_employee.department_id
    put_job=new_employee.job_id

    query="INSERT INTO hired_employees (name,datetime,department_id,job_id) "
    query+="VALUES ('"+put_name+"','"+str(put_datetime)+"',"+str(put_department)+","+str(put_job)+")"
    db_conection("send",query)
   
    response="The new employee called "+put_name+" has been added"

    return {response}
"""
@app.post("/hired_employees/",tags=["Hired employees"])
def Insert_one_or_more_employees(data: list[Hired_employees]):
#https://fastapi.tiangolo.com/tutorial/body-nested-models/?h=list#bodies-of-pure-lists
   
    for hired_employees in data:

        put_datetime=dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        put_name=hired_employees.name
        put_department=hired_employees.department_id
        put_job=hired_employees.job_id

        query="INSERT INTO hired_employees (name,datetime,department_id,job_id) "
        query+="VALUES ('"+put_name+"','"+str(put_datetime)+"',"+str(put_department)+","+str(put_job)+")"
        db_conection("send",query)

    response=str(len(data))+" employees has been added to DDBB"

    return {response}

#####################################################################################################################################
#Special
#####################################################################################################################################

@app.post("/special/",tags=["Special"])
def Create_a_number_of_employees(incoming:Random_employees):

    
    for i in range(incoming.number_of_employees):
        put_name=Random_name()
        put_datetime=Random_date()
        put_department=Random_Id("departments")
        put_job=Random_Id("jobs")
        query="INSERT INTO hired_employees (name,datetime,department_id,job_id) "
        query+="VALUES ('"+put_name+"','"+str(put_datetime)+"',"+str(put_department)+","+str(put_job)+")"
        db_conection("send",query)

    response=str(incoming.number_of_employees)+" employees has been added to DDBB"

    return {response}

@app.get("/special/Metrics_A",tags=["Special"])
def Metrics_A():
    #Conexion a la base de datos, traemos la tabla entera de hired_employees
    response=db_conection("receive","SELECT * from hired_employees")
    #Convertimos la lista de diccionarios a pandas dataframe
    response=pd.DataFrame(response)
    #Establecemos los dates limites en cada segemento "Q"
    Q1_li,Q1_ls="2021-01-01","2021-03-31"  
    Q2_li,Q2_ls="2021-04-01","2021-06-30"
    Q3_li,Q3_ls="2021-07-01","2021-09-30"
    Q4_li,Q4_ls="2021-10-01","2021-12-31"
    # Armamos una lista "Qresponse"con la siguiente estructura ->  Qresponse=["Qn,department_idn,job_idn",...]
    # donde Qn nos indica el cuatrimestre de ingreso y department_idn,job_idn las caracteristicas del ingreso
    Qresponse=[]
    for row in range(response.shape[0]):#recorremos todo el dataframe, fila por fila
        #ROW structure: -> [id,name,datetime,department_id,job_id] 
        row_list=list(response.iloc[row])#creamos una lista con el contenido de cada fila
        fecha=row_list[2]
    
        fecha=datetime.strptime(fecha[0:10],'%Y-%m-%d')
    
        if fecha>=datetime.strptime(Q1_li,'%Y-%m-%d') and fecha<=datetime.strptime(Q1_ls,'%Y-%m-%d'):
            q1=["Q1",row_list[3],row_list[4]]
            Qresponse.append(q1)

        if fecha>=datetime.strptime(Q2_li,'%Y-%m-%d') and fecha<=datetime.strptime(Q2_ls,'%Y-%m-%d'):
            q2=["Q2",row_list[3],row_list[4]]
            Qresponse.append(q2)

        if fecha>=datetime.strptime(Q3_li,'%Y-%m-%d') and fecha<=datetime.strptime(Q3_ls,'%Y-%m-%d'):
            q3=["Q3",row_list[3],row_list[4]]
            Qresponse.append(q3)

        if fecha>=datetime.strptime(Q4_li,'%Y-%m-%d') and fecha<=datetime.strptime(Q4_ls,'%Y-%m-%d'):
            q4=["Q4",row_list[3],row_list[4]]
            Qresponse.append(q4)

        #Conectamos a la BBDD y ordenamos, para posteriormente reemplazar los FK
        departments_list=Table_column_to_list("departments","department")
        departments_list=sorted(departments_list)
       
        jobs_list=Table_column_to_list("jobs","job")
        jobs_list=sorted(jobs_list)
   
        #Recorremos todos los departaments y jobs, y comparamos con Qresponse, y donde encontramos
        #una coincidencia, sumamos un hired_employee con caracteristicas especificas en cada "Q"
        q_challenge=[]
        for index1,department in enumerate(departments_list,start=1):
            for index2,job in enumerate(jobs_list,start=1):
                q1=0
                q2=0
                q3=0
                q4=0
                for k in range(len(Qresponse)):
                    if Qresponse[k][0]=="Q1" and Qresponse[k][1]==index1 and Qresponse[k][2]==index2:
                        q1=q1+1
                    if Qresponse[k][0]=="Q2" and Qresponse[k][1]==index1 and Qresponse[k][2]==index2:
                        q2=q2+1
                    if Qresponse[k][0]=="Q3" and Qresponse[k][1]==index1 and Qresponse[k][2]==index2:
                        q3=q3+1
                    if Qresponse[k][0]=="Q4" and Qresponse[k][1]==index1 and Qresponse[k][2]==index2:
                        q4=q4+1
    
            q_challenge.append([department,job,q1,q2,q3,q4])


    
    q_challenge=pd.DataFrame(q_challenge)
    print(q_challenge)
    q_challenge.columns=["department","job","Q1","Q2","Q3","Q4"]
    q_challenge=q_challenge.to_json(orient="records")

    return q_challenge

#para terminar el ultimo endpoint:

#agregar schedule variable
#query que busque los limites de job y de department

#backup in AVRO format
#restore from backup

#make the first endpoint
#make the second endpoint

#visual report

#logging failed request



    




