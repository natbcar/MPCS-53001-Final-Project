import mysql.connector

def reset_sql_database():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="mypassword",
            database="mpcs53001_final_project"
        )
        cursor = connection.cursor()
        
        with open('sql/schema.sql', 'r') as f:
            sql_script = f.read()
        
        print(sql_script)
        
        # In 9.2.0+, execute() handles multiple statements by default
        # without the multi=True argument.
        cursor.execute(sql_script)
        
        # New API: Use fetchsets() to iterate through all statement results.
        # This replaces the old loop over the execute() return value.
        for result_set in cursor.fetchsets():
            # This 'consumes' the results of each statement in your script,
            # clearing the buffer and preventing 'Commands out of sync'.
            pass
            
        connection.commit()
        print("MySQL Database reset successfully.")
        
    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
    except Exception as e:
        print(f"General Error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

reset_sql_database()