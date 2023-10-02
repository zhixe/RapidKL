import os
import mysql.connector
from dotenv import load_dotenv
import logging
from datetime import datetime

load_dotenv("../.env")
datasetName="accident"
logsdir = os.getenv("logsdir")
os.makedirs(logsdir, exist_ok=True)

# Set up logging configuration
log_file = f"{logsdir}/log_PROD_staging_{datasetName}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", filename=log_file)

# Add a StreamHandler to print log messages to the console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)
logging.getLogger().addHandler(console_handler)

class CSVToMySQL:
    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.table_name = datasetName
        self.mysql_host = os.getenv("mysqlHost")
        self.mysql_port = os.getenv("mysqlPort")
        self.mysql_username = os.getenv("mysqlUsername")
        self.mysql_password = os.getenv("mysqlPassword")
        self.mysql_database = os.getenv("mysqlDatabase")
        self.bcc_acc_id ="bccAccid"
        self.acc_date ="accDate"
        self.time_report ="timeReport"
        self.event_time ="eventTime"
        self.event_loc ="eventLoc"
        self.location_cat ="locationCat"
        self.bus_no ="busNo"
        self.body_no ="bodyNo"
        self.bus_model_id ="busModelid"
        self.route_id ="routeId"
        self.depot_id ="depotId"
        self.capt_id ="captId"
        self.third_party ="thirdParty"
        self.cat_id ="catId"
        self.injury ="injury"
        self.third_party2 ="thirdParty2"
        self.spad_case ="spadCase"
        self.bus_damage ="busDamage"
        self.immediate_cause ="immediateCause"
        self.catitan ="catitan"
        self.remark ="remark"
        self.pax ="pax"
        self.public ="public"
        self.staff ="staff"
        self.status ="status"
        self.report_date ="reportDate"
        self.guilty ="guilty"
        self.penalty ="penalty"
        self.cost ="cost"
        self.act_taken ="actTaken"
        self.cso_duty ="csoDuty"
        self.category_acc ="categoryAcc"
        self.acc_sn ="accSn"
        self.telegram_desc ="telegramDesc"
        self.fin_impact ="finImpact"
        self.ope_impact ="opeImpact"
        self.ops_find ="opsFind"
        self.eng_find ="engFind"
        self.conclusion_close ="conclusionClose"
        self.accstatus ="accstatus"
        self.dt_created ="dtCreated"
        self.dt_modified ="dtModified"
        self.modified_by ="modifiedBy"

    def create_table(self):
        try:
            # Establish a connection to the MySQL server
            conn = mysql.connector.connect(
                host=self.mysql_host,
                port=self.mysql_port,
                user=self.mysql_username,
                password=self.mysql_password,
                database=self.mysql_database
                # allow_local_infile=True  # Enable loading local data on the client side
            )

            # Create the table if it doesn't exist
            cursor = conn.cursor()

            drop_table_query = f"""
            DROP TABLE IF EXISTS {self.table_name};
            """

            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                {self.bcc_acc_id} INT NOT NULL,
                {self.acc_date} DATE NOT NULL,
                {self.time_report} TEXT NOT NULL,
                {self.event_time} TEXT NOT NULL,
                {self.event_loc} TEXT NOT NULL,
                {self.location_cat} SMALLINT NOT NULL,
                {self.bus_no} TEXT NOT NULL,
                {self.body_no} TEXT NOT NULL,
                {self.bus_model_id} SMALLINT NOT NULL,
                {self.route_id} INT NOT NULL,
                {self.depot_id} INT NOT NULL,
                {self.capt_id} TEXT NOT NULL,
                {self.third_party} TEXT NOT NULL,
                {self.cat_id} SMALLINT NOT NULL,
                {self.injury} TEXT NOT NULL,
                {self.third_party2} TEXT NOT NULL,
                {self.spad_case} BOOL NOT NULL,
                {self.bus_damage} TEXT NOT NULL,
                {self.immediate_cause} SMALLINT NOT NULL,
                {self.catitan} TEXT NOT NULL,
                {self.remark} TEXT NOT NULL,
                {self.pax} INT NOT NULL,
                {self.public} INT NOT NULL,
                {self.staff} INT NOT NULL,
                {self.status} SMALLINT NOT NULL,
                {self.report_date} DATE NOT NULL,
                {self.guilty} SMALLINT NOT NULL,
                {self.penalty} INT NOT NULL,
                {self.cost} TEXT NOT NULL,
                {self.act_taken} SMALLINT NOT NULL,
                {self.cso_duty} TEXT NOT NULL,
                {self.category_acc} TEXT NOT NULL,
                {self.acc_sn} TEXT NOT NULL,
                {self.telegram_desc} TEXT NOT NULL,
                {self.fin_impact} INT NOT NULL,
                {self.ope_impact} INT NOT NULL,
                {self.ops_find} TEXT NOT NULL,
                {self.eng_find} TEXT NOT NULL,
                {self.conclusion_close} SMALLINT NOT NULL,
                {self.accstatus} SMALLINT NOT NULL,
                {self.dt_created} DATETIME NOT NULL,
                {self.dt_modified} DATETIME NOT NULL,
                {self.modified_by} TEXT NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=COMPRESSED
            """

            cursor.execute(drop_table_query)
            cursor.execute(create_table_query)

            # Close the connection
            conn.close()

            print("\n")
            logging.info(f"[[ {self.table_name.upper()} ]]")
            logging.info(f"Table {self.mysql_database}.{self.table_name} created successfully.")
        except mysql.connector.Error as error:
            logging.error(f"An error occurred while creating the table: {error}")
            raise

    def extract_from_csv(self):
        try:
            logging.info(f"Importing data from {self.csv_file} to {self.table_name}")

            # Establish a connection to the MySQL server
            conn = mysql.connector.connect(
                host=self.mysql_host,
                port=self.mysql_port,
                user=self.mysql_username,
                password=self.mysql_password,
                database=self.mysql_database,
                allow_local_infile=True  # Enable loading local data on the client side
            )

            # Truncate the table to avoid duplicates
            cursor = conn.cursor()
            truncate_query = f"TRUNCATE TABLE {self.table_name}"
            cursor.execute(truncate_query)

            # Define the path to the CSV file
            csv_file_path = os.path.abspath(self.csv_file)
            csv_file_path = csv_file_path.replace('\\', '\\\\')

            # Build the LOAD DATA INFILE query
            load_data_query = f"""
            LOAD DATA LOCAL INFILE '{csv_file_path}'
            INTO TABLE {self.table_name}
            FIELDS TERMINATED BY ','
            OPTIONALLY ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (
                {self.bcc_acc_id},
                {self.acc_date},
                {self.time_report},
                {self.event_time},
                {self.event_loc},
                {self.location_cat},
                {self.bus_no},
                {self.body_no},
                {self.bus_model_id},
                {self.route_id},
                {self.depot_id},
                {self.capt_id},
                {self.third_party},
                {self.cat_id},
                {self.injury},
                {self.third_party2},
                {self.spad_case},
                {self.bus_damage},
                {self.immediate_cause},
                {self.catitan},
                {self.remark},
                {self.pax},
                {self.public},
                {self.staff},
                {self.status},
                {self.report_date},
                {self.guilty},
                {self.penalty},
                {self.cost},
                {self.act_taken},
                {self.cso_duty},
                {self.category_acc},
                {self.acc_sn},
                {self.telegram_desc},
                {self.fin_impact},
                {self.ope_impact},
                {self.ops_find},
                {self.eng_find},
                {self.conclusion_close},
                {self.accstatus},
                {self.dt_created},
                {self.dt_modified},
                {self.modified_by}
                )
            """

            # Execute the LOAD DATA INFILE query
            cursor.execute(load_data_query)

            # Commit the changes and close the connection
            conn.commit()
            conn.close()

            logging.info("Data imported successfully.")
        except mysql.connector.Error as error:
            logging.error(f"An error occurred while importing data from {self.csv_file} to {self.table_name}: {error}")
            raise

if __name__ == "__main__":
    # Specify the CSV file
    indir = os.getenv("indirSQL")
    csv_file = fr'{indir}\{datasetName}.csv'

    # Create an instance of CSVToMySQL and pass the necessary parameters
    csv_to_mysql = CSVToMySQL(csv_file)

    # Create the table
    csv_to_mysql.create_table()

    # Extract data from the CSV file and import it into the table
    csv_to_mysql.extract_from_csv()
