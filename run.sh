#!/bin/bash

python pipeline.py GenerateQuandlReport \
  --local-scheduler \
  --workers=1
