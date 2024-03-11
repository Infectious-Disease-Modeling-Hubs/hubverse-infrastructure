#!/bin/bash

py_exclude=('*.pyc' '*.ipynb' '*__pycache__*' '*ipynb_checkpoints*')

rm hubverse_transforms.zip

pip install \
--platform manylinux2014_x86_64 \
--target=build \
--implementation cp \
--python-version 3.12 \
--only-binary=:all: --upgrade \
pyarrow

cd build
zip -r ../hubverse_transforms.zip . -x "${py_exclude[@]}"
cd ..
zip -j hubverse_transforms.zip src/hubverse_transforms/__init__.py
zip -j hubverse_transforms.zip src/hubverse_transforms/lambda_function.py
zip -j hubverse_transforms.zip src/hubverse_transforms/model_output.py

aws s3 cp hubverse_transforms.zip s3://hubverse-infrastructure-test/
aws lambda update-function-code \
  --function-name arn:aws:lambda:us-east-1:767397675902:function:hubverse-transform-data \
  --s3-bucket hubverse-infrastructure-test \
  --s3-key hubverse_transforms.zip