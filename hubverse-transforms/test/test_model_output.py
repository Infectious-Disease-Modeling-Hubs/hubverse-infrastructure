import pyarrow as pa
import pytest
from hubverse_transforms.model_output import ModelOutputHandler


@pytest.fixture()
def model_output_table():
    return pa.table(
        {
            'location': ['earth', 'vulcan', 'seti alpha'],
            'value': [11.11, 22.22, 33.33],
        }
    )


def test_from_s3():
    """Test ModelOutputHandler S3-oriented instantiation."""
    s3_key = 'raw/prefix1/prefix2/2420-01-01-janeways_addiction-voyager1.csv'
    raw_prefix = 'raw'
    mo = ModelOutputHandler.from_s3('delta-quadrant-data', s3_key, raw_prefix)

    assert mo.storage_location == 'delta-quadrant-data'
    assert mo.file_path == 'raw/prefix1/prefix2/2420-01-01-janeways_addiction-voyager1.csv'
    assert mo.file_name == '2420-01-01-janeways_addiction-voyager1'
    assert mo.file_type == '.csv'
    assert mo.round_id == '2420-01-01'
    assert mo.team == 'janeways_addiction'
    assert mo.model == 'voyager1'
    assert type(mo.fs_interface).__name__ == 'S3FileSystem'


@pytest.mark.parametrize(
        'origin_prefix, original_file_path, expected_destination_path',
        [
            ('raw', 'raw/prefix1/prefix2/2420-01-01-team-model.csv', 'prefix1/prefix2'),
            ('raw', 'raw/model-output/prefix1/prefix2/2420-01-01-team-model.csv', 'model-output/prefix1/prefix2'),
            ('raw', 'raw/prefix1/prefix2/prefix3/prefix4/2420-01-01-team-model.csv', 'prefix1/prefix2/prefix3/prefix4'),
            ('raw', 'raw/2420-01-01-team-model.csv', '.'),
            ('different-origin', 'different-origin/prefix1/2420-01-01-team-model.csv', 'prefix1'),
        ]
)
def test_destination_path(origin_prefix, original_file_path, expected_destination_path):
    mo = ModelOutputHandler.from_s3('test-bucket', original_file_path, origin_prefix)
    assert mo.destination_path == expected_destination_path


@pytest.mark.parametrize(
        'file_path, expected_error',
        [
            ('raw/prefix1/prefix2/2420-01-01-janeways-addiction-voyager1.csv', ValueError),
            ('raw/prefix1/prefix2/round_id-janewaysaddiction-voyager1.csv', ValueError),
            ('raw/prefix1/prefix2/2420-01-01-janewaysaddiction-voyager-1.csv', ValueError),
        ]
)
def test_parse_s3_key_invalid_format(file_path, expected_error):
    # ensure ValueError is raised for invalid model-output file name format
    with pytest.raises(ValueError):
        ModelOutputHandler.from_s3('delta-quadrant-data', file_path)


def test_parse_s3_key_invalid_prefix():
    s3_key = 'raw/prefix1/prefix2/2000-01-01-team1-model1.csv'
    raw_prefix = 'custom-raw-prefix'

    with pytest.raises(ValueError):
        ModelOutputHandler.from_s3('delta-quadrant-data', s3_key, raw_prefix)


def test_parse_s3_key_invalid_type():
    s3_key = 'raw/prefix1/prefix2/2000-01-01-team1-model1.jpg'

    with pytest.raises(ValueError):
        ModelOutputHandler.from_s3('delta-quadrant-data', s3_key)


def test_non_s3_instantiation():
    file_path = 'raw/prefix1/prefix2/2420-01-01-janewaysaddiction-voyager1.csv'
    with pytest.raises(NotImplementedError):
        ModelOutputHandler(file_path)


def test_add_columns(model_output_table):
    bucket_name = 'a-fake-bucket'
    s3_key = 'raw/prefix1/prefix2/2420-01-01-janewaysaddiction-voyager1.csv'
    mo = ModelOutputHandler.from_s3(bucket_name, s3_key)

    result = mo.add_columns(model_output_table)

    # transformed data should have 3 new columns: round_id, team, and model
    assert result.num_columns == 5
    assert set(['round_id', 'team', 'model']).issubset(result.column_names)


def test_added_column_values(model_output_table):
    bucket_name = 'a-fake-bucket'
    s3_key = 'raw/prefix1/prefix2/2420-01-01-janewaysaddiction-voyager1.csv'
    mo = ModelOutputHandler.from_s3(bucket_name, s3_key)

    result = mo.add_columns(model_output_table)

    # round_id, team, and model columns should each contain a single value
    # that matches round, team, and model as derived from the file name
    assert len(result.column('round_id').unique()) == 1
    result.column('team').unique()[0].as_py() == '2420-01-01'

    assert len(result.column('team').unique()) == 1
    result.column('team').unique()[0].as_py() == 'janewaysaddiction'

    assert len(result.column('model').unique()) == 1
    result.column('team').unique()[0].as_py() == 'voyager1'
