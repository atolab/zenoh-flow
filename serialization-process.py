#!/usr/bin/env python3

import sys
import json
import csv
from pathlib import Path
import os
from datetime import datetime

log_dir = Path('logs')

if not os.path.exists(log_dir):
    os.makedirs(log_dir)

bench_data = {'bench' : [], 'size': [], 'unit': [], 'iterations': [] ,'total_value' : [], 'avg_value':[]}

ts = f'{datetime.now()}'.replace(' ','-')
file_name=f'zf-serde-data-{ts}.csv'

data_file = open(log_dir.joinpath(file_name), 'w')
csv_writer = csv.writer(data_file)
header = bench_data.keys()
csv_writer.writerow(header)
data_file.flush()

for line in sys.stdin:
    # sys.stdout.write(line)
    try:
        d = json.loads(line)

        if d.get('reason') is not None:
            if d.get('reason') == 'benchmark-complete':
                bid = "-".join(d.get('id').split('-')[:-1])
                size = d.get('id').split('-')[-1]
                unit = d.get('unit')
                avg_values = []
                for i, n_iterations in enumerate(d.get('iteration_count')):
                    value = d.get('measured_values')[i]
                    avg_value = value / n_iterations
                    res = {}
                    res['bench'] = bid
                    res['size'] = size
                    res['unit'] = unit
                    res['iterations'] = n_iterations
                    res['total_value'] = value
                    res['avg_value'] = avg_value
                    csv_writer.writerow(res.values())
                    data_file.flush()

    except Exception:
        pass

data_file.close()