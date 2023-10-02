import os, json, logging, mysql.connector
from dotenv import load_dotenv
from datetime import datetime

class Config:
    def __init__(self):
        load_dotenv("../.env")
        self.logsdir = os.getenv("logsdir")
        self.schemadir = os.getenv("schemadir")
        os.makedirs(self.logsdir, exist_ok=True)

    def get_schema_data(self, script_basename_without_ext):
        with open(fr"{self.schemadir}\{script_basename_without_ext}.json", 'r') as f:
            schema_data = json.load(f)
        return schema_data

    def get_dataset_name(self, schema_data):
        return list(schema_data.keys())[0]

class Logger:
    def __init__(self, datasetName, logsdir):
        log_file = f"{logsdir}/log_PROD_staging_{datasetName}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", filename=log_file)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(console_formatter)
        logging.getLogger().addHandler(console_handler)

class Database:
    def __init__(self):
        self.mysql_host = os.getenv("mysqlHost")
        self.mysql_port = os.getenv("mysqlPort")
        self.mysql_username = os.getenv("mysqlUsername")
        self.mysql_password = os.getenv("mysqlPassword")
        self.mysql_database = os.getenv("mysqlDatabase")

    def connect(self):
        return mysql.connector.connect(
            host=self.mysql_host,
            port=self.mysql_port,
            user=self.mysql_username,
            password=self.mysql_password,
            database=self.mysql_database,
            allow_local_infile=True
        )

class CSVToMySQL:
    def __init__(self, csv_dir, schema, db):
        self.csv_dir = csv_dir
        self.table_name = datasetName
        self.schema = schema[self.table_name]
        self.db = db

    def create_table(self):
        try:
            conn = self.db.connect()
            cursor = conn.cursor()
            drop_table_query = f"DROP TABLE IF EXISTS {self.table_name};"
            columns = ', '.join(f'{column_name} {data_type} NOT NULL' for column_name, data_type in self.schema.items())
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                {columns}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=COMPRESSED;
            """
            cursor.execute(drop_table_query)
            cursor.execute(create_table_query)
            conn.close()
            print("\n")
            logging.info(f"[[ {self.table_name.upper()} ]]")
            logging.info(f"Table {self.db.mysql_database}.{self.table_name} created successfully.")
        except mysql.connector.Error as error:
            logging.error(f"An error occurred while creating the table: {error}")
            raise

    def extract_from_csv(self):  # sourcery skip: raise-specific-error
        try:
            logging.info(f"Starting import of data from CSV files to {self.table_name}")
            conn = self.db.connect()
            cursor = conn.cursor()
            truncate_query = f"TRUNCATE TABLE {self.table_name}"
            cursor.execute(truncate_query)
            logging.info(f"Table {self.table_name} truncated.")
            
            total_rows_imported = 0

            for filename in os.listdir(self.csv_dir):
                if filename.startswith(self.table_name) and filename.endswith(".csv"):
                    print(f"Does '{filename}' start with '{self.table_name}': {filename.startswith(self.table_name)}")
                    print(f"Does '{filename}' end with '.csv': {filename.endswith('.csv')}")
                    csv_file_path = self.csv_dir.replace('\\', '\\\\')
                    csv_file_path = os.path.join(csv_file_path, filename)
                    columns = ', '.join(self.schema.keys())
                    load_data_query = f"""
                    LOAD DATA LOCAL INFILE '{csv_file_path}'
                    INTO TABLE {self.table_name}
                    FIELDS TERMINATED BY ','
                    OPTIONALLY ENCLOSED BY '"'
                    LINES TERMINATED BY '\\n'
                    IGNORE 1 LINES
                    (
                        {columns}
                    )
                    """
                    logging.info(f"Executing query: {load_data_query}")
                    cursor.execute(load_data_query)
                    row_count = cursor.rowcount
                    total_rows_imported += row_count
                    if row_count == 0:
                        logging.warning(f"No data was imported from {csv_file_path}")
                    else:
                        logging.info(f"Imported {row_count} rows from {csv_file_path}")
            
            if total_rows_imported == 0:
                raise Exception("No data was imported. Exiting program.")
            
            conn.commit()
            logging.info("Changes committed to the database.")
            conn.close()
            logging.info("Database connection closed.")
            
            logging.info("All data imported successfully.")
        except mysql.connector.Error as error:
            logging.error(f"An error occurred while importing data: {error}")
            raise

if __name__ == "__main__":
    script_basename = os.path.basename(__file__)
    script_basename_without_ext = os.path.splitext(script_basename)[0]

    config = Config()
    schema_data = config.get_schema_data(script_basename_without_ext)
    datasetName = config.get_dataset_name(schema_data)

    db = Database()
    logger = Logger(datasetName, config.logsdir)

    csv_dirInit = os.getenv("outdir")
    csv_dir = fr'{csv_dirInit}\\'

    csv_to_mysql = CSVToMySQL(csv_dir, schema_data, db)
    csv_to_mysql.create_table()
    csv_to_mysql.extract_from_csv()
