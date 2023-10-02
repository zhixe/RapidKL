# Version 1.11
import os
import glob
import time
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from multiprocessing import Pool
from pyspark.sql import SparkSession
import polars as pl

# Define a Configuration class to store environment variables
class Config:
    def __init__(self):
        # Load environment variables from the .env file
        load_dotenv("../.env")
        self.input_directory = os.getenv("indir")
        self.output_directory = os.getenv("outdir")
        self.log_dir = os.getenv("logsdir")

        # Create output and logs directories if they don't exist
        os.makedirs(self.output_directory, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)

        # Get the number of CPU cores available on the system
        self.num_processes = os.cpu_count() or 1  # Use at least 1 process if cpu_count() returns None
        self.chunk_size = 10000 # Default value of chunk size

# Define a Logging class for logging operations
class DataProcessorLogger:
    def __init__(self, log_dir):
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)

    def configure_logging(self):
        # Include the current date in the log filename
        current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(self.log_dir, f"data_processing_{current_date}.log")

        logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    def log_info(self, message):
        logging.info(message)

    def log_error(self, message):
        logging.error(message)

# Define a class for processing CSV output
class CsvOutputProcessor:
    def __init__(self, outdir):
        self.outdir = outdir

    def process_csv_output(self, csv_file_path, chunk, base_filename, chunk_number):
        try:
            chunk.to_csv(csv_file_path, index=False, header=True)
        except Exception as e:
            f"Error writing CSV file for {base_filename}_{chunk_number}: {str(e)}"

# Define a class for handling CSV output
class CsvOutputHandler:
    def __init__(self, outdir):
        self.outdir = outdir

    def write_csv(self, csv_file_path, chunk, base_filename, chunk_number):
        csv_processor = CsvOutputProcessor(self.outdir)
        csv_processor.process_csv_output(csv_file_path, chunk, base_filename, chunk_number)

# Define a class for processing data
class BaseFilenameProcessor:
    def __init__(self, indir, outdir, config, log_dir):
        self.indir = indir
        self.outdir = outdir
        self.config = config
        self.chunk_size = Config().chunk_size
        self.log_dir = log_dir

    def removeExistingFile(self):
        delete_output_file = glob.glob(os.path.join(self.outdir, r"*.csv"))
        for delete_output_path in delete_output_file:
            os.remove(delete_output_path)

        delete_log_file = glob.glob(os.path.join(self.log_dir, r"*.log"))
        for delete_log_path in delete_log_file:
            os.remove(delete_log_path)

    def list_input_files(self):
        return [
            os.path.join(self.indir, file)
            for file in os.listdir(self.indir)
            if file.endswith(".csv")
        ]

    def generate_base_filename(self, input_file):
        return os.path.splitext(os.path.basename(input_file))[0]

    def generate_output_filenames(self, base_filename, chunk_number):
        csv_filename = f"{base_filename}_{chunk_number}.csv"
        csv_file_path = os.path.join(self.outdir, csv_filename)

        # Read the CSV file into a DataFrame
        dfT = pd.read_csv(csv_filename)

        # Write the DataFrame out as a Parquet file
        dfT.to_parquet(f'{base_filename}_{chunk_number}.parquet', index=False)

        return csv_filename, csv_file_path

    def process_chunk(self, input_file):
        # Initialize the logger for each process
        logger = DataProcessorLogger(self.config.log_dir)
        logger.configure_logging()

        try:
            self._extracted_from_process_chunk(input_file, logger)
        except Exception as e:
            logger.log_error(f"Error processing chunks: {str(e)}")

    def _extracted_from_process_chunk(self, input_file, logger):
        base_filename = self.generate_base_filename(input_file)
        logger.log_info(f"Processing input file: {input_file}, Base Filename: {base_filename}")

        total_start_time = time.time()
        csv_row_counts = []

        # Count total rows of a single CSV file
        lz_df = pl.scan_csv(input_file)
        df = lz_df.collect()
        num_rows = df.shape[0]
        # print(f"{num_rows}")

        # Determine chunk size based on total length of input_file
        total_length = num_rows
        if total_length < 10000:
            self.chunk_size = 1000
        elif total_length < 100000:
            self.chunk_size = 10000
        elif total_length < 1000000:
            self.chunk_size = 100000
        elif total_length < 10000000:
            self.chunk_size = 1000000
        elif total_length >= 100000000:
            self.chunk_size = 10000000

        # for chunk_number, chunk in enumerate(pd.read_csv(input_file, chunksize=self.chunk_size, low_memory=False), start=1):
        for chunk_number, chunk in enumerate(pd.read_csv(input_file, chunksize=self.chunk_size, low_memory=False), start=1):
            start_time = time.time()
            csv_filename, csv_file_path = self.generate_output_filenames(base_filename, chunk_number)
            csv_row_count = len(chunk)
            csv_row_counts.append(csv_row_count)

            csv_handler = CsvOutputHandler(self.outdir)
            csv_handler.write_csv(csv_file_path, chunk, base_filename, chunk_number)

            csv_end_time = time.time()
            csv_execution_time = csv_end_time - start_time

            logger.log_info(f"[Batch {chunk_number}]")
            logger.log_info(f"{csv_filename}:")
            logger.log_info(f"  Execution Time: {csv_execution_time:.6f} seconds")
            logger.log_info(f"  Rows: {csv_row_count} rows")

        total_end_time = time.time()
        total_execution_time = total_end_time - total_start_time
        logger.log_info(f"Total Execution Time: {total_execution_time:.6f} seconds\n")

if __name__ == "__main__":
    # Initialize the configuration
    config = Config()

    processor = BaseFilenameProcessor(
        config.input_directory,
        config.output_directory,
        config, config.log_dir)  
    input_files = processor.list_input_files() # Pass the config instance

    # Create a Process Pool
    with Pool(processes=config.num_processes) as pool:
        pool.starmap(processor.process_chunk, [(input_file,) for input_file in input_files])
