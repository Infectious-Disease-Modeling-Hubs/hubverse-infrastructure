import pytest

from hubverse_transforms.model_output import ModelOutputHandler


def test_parse_s3_key():
    s3_key = 'raw/prefix1/prefix2/filename.csv'
    raw_prefix = 'raw'
    result = ModelOutputHandler.parse_s3_key(s3_key, raw_prefix)

    assert result['s3_prefix'] == 'raw/prefix1/prefix2'
    assert result['s3_object_name'] == 'filename'
    assert result['s3_object_type'] == '.csv'
    assert result['s3_destination_prefix'] == 'prefix1/prefix2'


def test_parse_s3_key_invalid_prefix():
    s3_key = 'raw/prefix1/prefix2/filename.txt'
    raw_prefix = 'custom-raw-prefix' 

    with pytest.raises(ValueError):
        ModelOutputHandler.parse_s3_key(s3_key, raw_prefix)


def test_parse_s3_key_invalid_type():
    s3_key = 'raw/prefix1/prefix2/filename.txt'
    raw_prefix = 'raw' 

    with pytest.raises(ValueError):
        ModelOutputHandler.parse_s3_key(s3_key, raw_prefix)


# test parse_file
# test add_columns