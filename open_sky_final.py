#importing packages
import snowflake.connector
import time
from datetime import datetime
from opensky_api import OpenSkyApi
from pathlib import Path

#Output File Paths
file_path = "E:/ZeroG/opensky_files/"
archived_path="E:/ZeroG/opensky_files/Archived/"


#SnowFlake Credentials and table details
username='knaman'
password='*****'
accountname='ei93613.east-us-2.azure'
warehouse='WH_XS'
database='OPEN_SKY_DB'
schema='opensky'
stage_table='STAGE_OPENSKY_STATE'
target_table = 'OPENSKY_STATE'
audit_table = 'audit_table'

#Create Connection with Snowflake
def connection_create(username,password,accountname,warehouse,database,schema):
    ctx = snowflake.connector.connect(
    user=username,
    password=password,
    account=accountname,
    warehouse=warehouse,
    database=database,
    schema=schema
    )
    cs = ctx.cursor()
    return cs

#Logs audit related entry in a Table
def audit_start(database,schema,audit_table):
    prev_run_upd_query = "update "+database+"."+schema+"."+audit_table+" set run_status = 'Failed' where run_status ='Running' "
    create_insert_query="insert into "+database+"."+schema+"."+audit_table+"(run_status,start_time) values ('Running',current_timestamp())"
    #print(prev_run_upd_query)
    #print(create_insert_query)
    cur=connection_create(username,password,accountname,warehouse,database,schema)
    cur.execute(prev_run_upd_query)
    cur.execute(create_insert_query)
    print("Inserted job running entry in audit table")

def audit_update(database,schema,audit_table,status,run_key,file_name):
    update_job_query="update "+database+"."+schema+"."+audit_table+" set run_status = '"+status+"' , end_time = current_timestamp(), file_name = '"+file_name+"' where run_key = "+run_key
    #print(update_job_query)
    cur=connection_create(username,password,accountname,warehouse,database,schema)
    cur.execute(update_job_query)
    print("Updated job status to Success in audit table")

def generate_file(run_key):
    api = OpenSkyApi()
    states = api.get_states()
    record_time_unix = str(states.time).strip()
    record_time = datetime.utcfromtimestamp(states.time).strftime('%Y%m%d%H%M%S')

    file_name = 'open_sky_'+str(record_time)+'.csv'
    file_path = 'E:/ZeroG/opensky_files/'
    archived_path="E:/ZeroG/opensky_files/Archived/"

#Open file for writing
    file_out = open(file_path+file_name,'w+')

#Adding file header
    file_out.write('record_time,baro_altitude,callsign,geo_altitude,heading,icao24,last_contact,latitude,longitude,on_ground,origin_country,position_source,sensors,spi,squawk,time_position,velocity,vertical_rate,run_key'+'\n')

    for s in states.states:
        baro_altitude = str(s.baro_altitude).strip()
        callsign = str(s.callsign).strip()
        geo_altitude = str(s.geo_altitude).strip()
        heading = str(s.heading).strip()
        icao24 = str(s.icao24).strip()
        last_contact = str(s.last_contact).strip()
        latitude = str(s.latitude).strip()
        longitude = str(s.longitude).strip()
        on_ground = str(s.on_ground).strip()
        origin_country = str(s.origin_country).strip()
        position_source = str(s.position_source).strip()
        sensors = str(s.sensors).strip()
        spi = str(s.spi).strip()
        squawk = str(s.squawk).strip()
        time_position = str(s.time_position).strip()
        velocity = str(s.velocity).strip()
        vertical_rate = str(s.vertical_rate).strip()

        file_out.write(','.join([record_time_unix,baro_altitude,callsign,geo_altitude,heading,icao24,last_contact,latitude,longitude,on_ground,origin_country,position_source,sensors,spi,squawk,time_position,velocity,vertical_rate,str(run_key)])+'\n')
        
    file_out.close()
    print('file generated')
    return file_name


def sf_table_load(file_name,stage_table):
    cur=connection_create(username,password,accountname,warehouse,database,schema)
    #moving file to snowflake stage
    cur.execute("put file://E:\ZeroG\opensky_files\\"+file_name+" @file_stage")
    print("File moved to Snowflake Stage")
    #truncate stage table
    cur.execute("truncate table "+database+"."+schema+"."+stage_table)
    print("Stage table truncated")
    #load file data into snowflake landing table
    cur.execute("copy into "+database+"."+schema+"."+stage_table+" from @file_stage/"+file_name)
    print("File loaded into Snowflake Table")
    
def sf_reporting_table_load(database,schema,stage_table,target_table):
    cur=connection_create(username,password,accountname,warehouse,database,schema)
    sql_stmt = "CALL sp_reporting_table_load('"+database+"','"+schema+"','"+stage_table+"','"+target_table+"')"
    #print(sql_stmt)
    cur.execute(sql_stmt)
    print("Reporting Table Loaded")
      

def file_archive(file_name,file_path,archived_path):
    Path(file_path+file_name).rename(archived_path+file_name)
    print("File moved to archive folder")
    

def run_start():
    cur=connection_create(username,password,accountname,warehouse,database,schema)
    audit_start(database,schema,audit_table)
    cur.execute("select max(run_key) from "+database+"."+schema+"."+audit_table)
    run_key = str(cur.fetchone()[0])
    #print(run_key)
    file_name = generate_file(run_key)
    sf_table_load(file_name,stage_table)
    sf_reporting_table_load(database,schema,stage_table,target_table)
    audit_update(database,schema,audit_table,'Success',run_key,file_name)
    file_archive(file_name,file_path,archived_path)
    
run_start()
print('Job Finished Successfully')
    