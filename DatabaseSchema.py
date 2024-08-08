import mysql.connector
import csv
class DatabaseSchema:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        self.tables = {}

    def connect(self):
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("Connection to the database established successfully.")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            self.conn = None
            self.cursor = None

    def fetch_schema_info(self):
        if self.cursor is None:
            print("No connection to the database.")
            return
        
        query = """
        SELECT 
            t.table_name,
            c.column_name, 
            c.data_type, 
            c.is_nullable, 
            c.column_default
        FROM 
            information_schema.tables t
        JOIN 
            information_schema.columns c 
        ON 
            t.table_name = c.table_name
        WHERE 
            t.table_schema = %s
            AND c.table_schema = %s
        ORDER BY 
            t.table_name, c.ordinal_position;
        """
        
        try:
            self.cursor.execute(query, (self.db_config['database'], self.db_config['database']))
            results = self.cursor.fetchall()
            self._process_results(results)
        except mysql.connector.Error as err:
            print(f"Error: {err}")

    def _process_results(self, results):
        self.tables.clear()
        for row in results:
            table_name = row[0]
            column_info = {
                'column_name': row[1],
                'data_type': row[2],
                'is_nullable': row[3],
                'column_default': row[4]
            }
            if table_name not in self.tables:
                self.tables[table_name] = {'columns': [], 'rows': []}
            self.tables[table_name]['columns'].append(column_info)
        
        # Fetch top ten rows for each table
        for table_name in self.tables.keys():
            self.tables[table_name]['rows'] = self._fetch_top_ten_rows(table_name)
        print("Schema information processed successfully.")

    def _fetch_top_ten_rows(self, table_name):
        query = f"SELECT * FROM {table_name} LIMIT 10;"
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            return rows
        except mysql.connector.Error as err:
            print(f"Error fetching rows for table {table_name}: {err}")
            return []

    def get_formatted_output(self):
        output = ""
        for table_name, table_info in self.tables.items():
            output += f"Table: {table_name}\n"
            for column in table_info['columns']:
                output += (f"  Column: {column['column_name']}, Type: {column['data_type']}, "
                           f"Nullable: {column['is_nullable']}, Default: {column['column_default']}\n")
            
            # Add the top ten rows to the output
            output += "  Top 10 Rows:\n"
            for row in table_info['rows']:
                output += f"    {row}\n"
            output += "\n"
        return output

    def print_output(self):
        print(self.get_formatted_output())

    def run_sql(self, sql_query):
        self.cursor.execute(sql_query)
        results = self.cursor.fetchall() 
        return results
    
    def write_to_file(self, results):
        # Get column names
        columns = [desc[0] for desc in self.cursor.description]

        # Define the CSV file path
        csv_file_path = 'worldInfo.csv'

        # Write data to CSV file
        with open(csv_file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            
            # Write column headers
            writer.writerow(columns)
            
            # Write rows
            writer.writerows(results)

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Connection to the database closed.")