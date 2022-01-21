#!/usr/bin/env bash

cargo criterion --bench serialization_bench  --message-format=json  --output-format=quiet | ./serialization-process.py

python3 serialization-bench-parse.py logs/