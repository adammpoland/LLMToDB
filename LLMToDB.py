from DatabaseSchema import DatabaseSchema
import ollama

modelSelected = "llama3.1"

def LLMToSQL(userQuery, db_info):
    
    messages = [
        {
            'role': 'system',
            'content': f'You are a database expert and generate responses in mysql only. Your database looks like this {db_info}. Please show only sql with no comments'
        },
        {
            'role': 'user',
            'content': f'{userQuery}'
        }
    ]
    response = ollama.chat(model=modelSelected, messages=messages)
    return response['message']['content']

def DescribeDatabase(dbColumnsAndTables):
    messages = [
        {
            'role': 'system',
            'content': f'You are a data expert and must analyze and provide a description of the each column and table in plain english. This is the information you have about the database {dbColumnsAndTables}'
        },
        {
            'role': 'user',
            'content': f'Can you describe the information about the database in plain english {dbColumnsAndTables}?'
        }
    ]
    response = ollama.chat(model=modelSelected, messages=messages)
    return response['message']['content']


def InterpretResults(userQuery, results):
    messages = [
        {
            'role': 'system',
            'content': f'You are a data expert and must answer the users query given the results of the database. The database results are {results}. Give only results based off of what is in the database.'
        },
        {
            'role': 'user',
            'content': f'{userQuery}'
        }
    ]
    response = ollama.chat(model=modelSelected, messages=messages)
    return response['message']['content']



if __name__ == "__main__":
    db_config = {
        'user': 'dbUser',
        'password': 'password',
        'host': 'localhost',
        'database': 'world'
    }

    db_schema = DatabaseSchema(db_config)
    db_schema.connect()
    db_schema.fetch_schema_info()
    dbColumnsAndTables = db_schema.get_formatted_output()
    description=DescribeDatabase(dbColumnsAndTables)
    print(description)
    userQuery = input("Ask a query to the database: ")
    dbColumnsAndTables = db_schema.get_formatted_output()
    
    print(LLMToSQL(userQuery, dbColumnsAndTables))
    results= db_schema.run_sql(LLMToSQL(userQuery, dbColumnsAndTables))
    print(InterpretResults(userQuery, results))

    db_schema.write_to_file(results)
    db_schema.close()