from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = "yt_api"

#This is used to get the connection and cursor
def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt",database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn,cur

#This will close the connection and cursor
def close_conn_cur(conn,cur):
    cur.close()
    conn.close()

#This function is used to create schema for the underlying data
def create_schema(schema):
    conn,cur = get_conn_cursor()
    scehma_sql = (f"CREATE SCHEMA IF NOT EXISTS {schema}")
    cur.execute(scehma_sql)
    conn.commit()
    close_conn_cur(conn,cur)

#This is a table creation definition
def create_table(schema):
    conn, cur = get_conn_cursor()
    if schema == 'staging':
        table_sql = f""" 
            CREATE TABLE IF NOT EXISTS {schema}.{table}(
            "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
            "Video_Title" text NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" VARCHAR(20) NOT NULL,
            "Video_Views" INT,
            "Likes_Count" INT,
            "Comments_Count" INT
            );
"""
    else:
        table_sql = f""" 
            CREATE TABLE IF NOT EXISTS {schema}.{table}(
            "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
            "Video_Title" text NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" TIME NOT NULL,
            "Video_Type" VARCHAR(10) NOT NULL,
            "Video_Views" INT,
            "Likes_Count" INT,
            "Comments_Count" INT
            );
"""
    cur.execute(table_sql)
    conn.commit()
    close_conn_cur(conn,cur)    

def get_video_ids(cur,schema):
    cur.execute(f""" SELECT "Video_ID" from {schema}.{table}; """)
    ids = cur.fetchall()#This will return a dictionary in format{['Video_ID':'xvgat2jid'],['Video_ID':'abcdefg23g']}
    video_ids = [row["Video_ID"] for row in ids] #This will convert the dictionary into a list of ids only
    return video_ids    

    

