import logging
import pathlib

import pyarrow as pa
from pyarrow import csv, fs
import pyarrow.parquet as pq


 # Log to stdout
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s -  %(levelname)s - %(name)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

class ModelOutputHandler:
    def __init__(self, bucket_name: str, s3_key: str, raw_prefix:str='raw'):
        self.bucket_name = bucket_name
        self.s3_key = s3_key
        self.raw_prefix = raw_prefix

        self.aws_region = fs.resolve_s3_region(bucket_name)
        self.s3 = fs.S3FileSystem(
            request_timeout=10,
            connect_timeout=10,
            region=self.aws_region)

        # Parse S3 key into individual parts
        s3_parts = self.parse_s3_key(s3_key, raw_prefix)
        for k, v in s3_parts.items():
            setattr(self, k, v)

        # Parse model-output file name into individual parts
        # (round_id, team, model)
        file_parts = self.parse_file(self.s3_object_name)
        for k, v in file_parts.items():
            setattr(self, k, v)


    def __repr__(self):
        return f'ModelOutputHandler(\'{self.bucket_name}\', \'{self.s3_key}\', \'{self.raw_prefix}\')' 


    def __str__(self):
        return f'Handle model-output data transforms for {self.s3_key} in {self.bucket_name}.'


    @classmethod
    def parse_s3_key(cls, s3_key: str, raw_prefix: str) -> dict:
        """Parse S3 key into individual parts."""

        file_path = pathlib.Path(s3_key)
    
        # If file_path doesn't reflect the hub's designated S3 prefix used for 
        # model-output original uploads, return an error. This check is a hedge
        # against incorrect external triggers (which shouldn't happen).
        if file_path.parts[0] != raw_prefix:
            raise ValueError(f'S3 key {s3_key} does not start with {raw_prefix}.')
        
        if file_path.suffix not in ['.csv', '.parquet']:
            # TODO: validate against hub's list of supported model-output file types?
            raise ValueError(f'Unsupported file type: {file_path.suffix}')

        s3_parts = {}
        s3_parts['s3_prefix'] = str(file_path.parent)
        s3_parts['s3_object_name'] = file_path.stem
        s3_parts['s3_object_type'] = file_path.suffix

        # Assume that output file path (where we're writing files) will
        # be the same as the input path without the preceding raw_prefix
        s3_parts['s3_destination_prefix'] = str(file_path.parent.relative_to(raw_prefix))

        logger.info(f'Parsed S3 key: {s3_parts}')
        return s3_parts


    @classmethod
    def parse_file(cls, s3_object_name: str) -> dict:
        """Parse model-output file name into individual parts."""

        # TODO: verify assumptions about format of model-output filenames
        # Code below assumes [round_id likely yyyy-mm-dd]-[team]-[model]
        object_name_split = s3_object_name.rsplit('-', 2)

        if len(object_name_split) != 3:
            raise ValueError(f'Unexpected model-output file name format: {s3_object_name}')
        
        file_parts = {}
        file_parts['round_id'] = object_name_split[0]
        file_parts['team'] = object_name_split[1]
        file_parts['model'] = object_name_split[2]

        logger.info(f'Parsed model-output filename: {file_parts}')
        return file_parts


    def read_file(self) -> pa.table:
        """Read model-output file into PyArrow table."""

        logger.info('Creating PyArrow S3FileSystem object')

        full_s3_path = f'{self.bucket_name}/{self.s3_key}'
        logger.info(f'Reading file: {full_s3_path}')

        if self.s3_object_type == '.csv':
            model_output_file = self.s3.open_input_stream(full_s3_path)
            model_output_table = csv.read_csv(model_output_file)
        elif self.s3_object_type == '.parquet':
            # parquet requires random access reading (because metadata),
            # so we use open_input_file instead of open_intput_stream
            model_output_file = self.s3.open_input_file(full_s3_path)
            model_output_table = pq.read_table(model_output_file)
        else:
            raise NotImplementedError(f'Unsupported file type: {self.s3_object_type}')

        return model_output_table


    def add_columns(self, model_output_table: pa.table) -> pa.table:
        """Add model-output metadata columns to PyArrow table."""

        num_rows = model_output_table.num_rows
        logger.info(f'Adding columns to table with {num_rows} rows')

        round_id_column = pa.array([self.round_id for i in range(0, num_rows)])
        team_column = pa.array([self.team for i in range(0, num_rows)])
        model_column = pa.array([self.model for i in range(0, num_rows)])

        updated_model_output_table = model_output_table \
            .append_column('round_id', round_id_column) \
            .append_column('team', team_column) \
            .append_column('model', model_column)
        
        return updated_model_output_table


    def write_parquet(self, updated_model_output_table: pa.table) -> tuple[str, str]:
        """Write transformed model-output table to parquet file."""

        transformed_s3_key = f'{self.s3_destination_prefix}/{self.s3_object_name}.parquet'

        # Current assumption is that we're writing back to the same bucket
        parquet_object_location = f'{self.bucket_name}/{transformed_s3_key}'
        
        with self.s3.open_output_stream(parquet_object_location) as parquet_file:
            pq.write_table(updated_model_output_table, parquet_file)
        
        logger.info(f'Finished writing parquet file: {parquet_object_location}')

        return self.bucket_name, transformed_s3_key

