import logging
import pathlib
import re

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv, fs

# Log to stdout
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s -  %(levelname)s - %(name)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class ModelOutputHandler:
    def __init__(
        self,
        file_path: str,
        origin_prefix: str = 'raw',
        storage_location: str | None = None,
        fs_interface: pa.fs.FileSystem | None = None

    ):
        # Might be useful to do a local transform of model-output files at some point,
        # but for now, we'll maintain focus on transforming cloud-based files (specifically, AWS)
        if fs_interface is None or storage_location is None:
            raise NotImplementedError('Only S3FileSystem is supported at this time.')
        else:
            self.fs_interface = fs_interface

        self.storage_location = storage_location
        self.file_path = file_path

        path = pathlib.Path(file_path)
        self.file_name = path.stem
        self.file_type = path.suffix

        if self.file_type not in ['.csv', '.parquet']:
            # TODO: validate against hub's list of supported model-output file types?
            raise ValueError(f'Unsupported file type: {path.suffix}')

        # ModelOutputHandler is designed to operate on original versions of model-output
        # data (i.e., as submitted my modelers). This check ensures that the file being
        # transformed has originated from wherever a hub keeps these "raw" (un-altered)
        # model-outputs.
        if path.parts[0] != origin_prefix:
            raise ValueError(f'Model output path {file_path} does not being with {origin_prefix}.')
        else:
            # Destination path = origin path w/o the origin prefix
            self.destination_path = str(path.relative_to(origin_prefix).parent)

        # Parse model-output file name into individual parts
        # (round_id, team, model)
        file_parts = self.parse_file(self.file_name)
        self.round_id = file_parts['round_id']
        self.team = file_parts['team']
        self.model = file_parts['model']


    def __repr__(self):
        return f"ModelOutputHandler('{self.storage_location}', '{self.file_path}', '{self.destination_path}')"


    def __str__(self):
        return f'Handle model-output data transforms for {self.file_path} in {self.storage_location}.'


    @classmethod
    def from_s3(cls, bucket_name: str, s3_key: str, origin_prefix: str = 'raw'):
        """Instantiate ModelOutputHandler for file on AWS S3."""

        try:
            aws_region = fs.resolve_s3_region(bucket_name)
        except OSError:
            # default to region used by Hubverse
            aws_region = 'us-east-1'

        s3fs = fs.S3FileSystem(request_timeout=10, connect_timeout=10, region=aws_region)

        return cls(s3_key, origin_prefix, bucket_name, s3fs)


    def parse_file(cls, file_name: str) -> dict:
        """Parse model-output file name into individual parts."""

        # TODO: verify assumptions about format of model-output filenames
        # Code below assumes [round_id likely yyyy-mm-dd]-[team]-[model] AND
        # that there are no hyphens in team or model names
        # https://github.com/orgs/Infectious-Disease-Modeling-Hubs/discussions/10
        file_name_split = file_name.rsplit('-', 2)

        if file_name.count('-') > 4 or len(file_name_split) != 3 or not re.match(r'^\d{4}-\d{2}-\d{2}$', file_name_split[0]):
            raise ValueError(f'File name {file_name} not in expected model-output format: yyyy-mm-dd-team-model.')

        file_parts = {}
        file_parts['round_id'] = file_name_split[0]
        file_parts['team'] = file_name_split[1]
        file_parts['model'] = file_name_split[2]

        # TODO: why so many logs?
        logger.info(f'Parsed model-output filename: {file_parts}')
        return file_parts


    def read_file(self, fs: pa.fs.FileSystem) -> pa.table:
        """Read model-output file into PyArrow table."""

        full_path = f'{self.storage_location}/{self.file_path}'
        logger.info(f'Reading file: {full_path}')

        if self.file_type == '.csv':
            model_output_file = fs.open_input_stream(full_path)
            model_output_table = csv.read_csv(model_output_file)
        elif self.file_type == '.parquet':
            # parquet requires random access reading (because metadata),
            # so we use open_input_file instead of open_intput_stream
            model_output_file = fs.open_input_file(full_path)
            model_output_table = pq.read_table(model_output_file)
        else:
            raise NotImplementedError(f'Unsupported file type: {self.file_type}')

        return model_output_table


    def add_columns(self, model_output_table: pa.table) -> pa.table:
        """Add model-output metadata columns to PyArrow table."""

        num_rows = model_output_table.num_rows
        logger.info(f'Adding columns to table with {num_rows} rows')

        # Create a dictionary of the existing columns
        existing_columns = {name: model_output_table[name] for name in model_output_table.column_names}

        # Create arrays that we'll use to append columns to the table
        new_columns = {
            'round_id': pa.array([self.round_id for i in range(0, num_rows)]),
            'team': pa.array([self.team for i in range(0, num_rows)]),
            'model': pa.array([self.model for i in range(0, num_rows)]),
        }

        # Merge the new columns with the existing columns
        all_columns = existing_columns | new_columns
        updated_model_output_table = pa.Table.from_pydict(all_columns)

        return updated_model_output_table


    def write_parquet(self, fs: pa.fs.FileSystem, updated_model_output_table: pa.table) -> tuple[str, str]:
        """Write transformed model-output table to parquet file."""

        transformed_file_path = f'{self.destination_path}/{self.file_name}.parquet'

        # Current assumption is that we're writing back to the same bucket
        parquet_object_location = f'{self.storage_location}/{transformed_file_path}'

        with fs.open_output_stream(parquet_object_location) as parquet_file:
            pq.write_table(updated_model_output_table, parquet_file)

        logger.info(f'Finished writing parquet file: {parquet_object_location}')

        return self.storage_location, transformed_file_path
    

    def transform_model_output(self):
        """Transform model-output data and write to parquet file."""

        model_output_table = self.read_file(self.fs_interface)
        updated_model_output_table = self.add_columns(model_output_table)
        transformed_location, transformed_file_path = self.write_parquet(self.fs_interface, updated_model_output_table)

        return transformed_location, transformed_file_path
