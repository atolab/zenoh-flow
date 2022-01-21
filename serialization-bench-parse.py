from io import StringIO
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import os
import pandas as pd
from pathlib import Path
import seaborn as sns
import sys

palette = 'plasma'
img_dir = Path('img')

if not os.path.exists(img_dir):
    os.makedirs(img_dir)


def time_conversion(l):
    if l == 'ns':
        return pow(10,-9)

    if l == 'us':
        return pow(10,-6)

    if l == "m":
        return pow(10,-3)

    return 1



def bytes_label(n):
    kmod = pow(2, 10)
    kdiv = n / kmod
    if kdiv < 1:
        return "{}".format(n)

    mmod = pow(2, 20)
    mdiv = n / mmod
    if mdiv < 1:
        return "{0:.{c}f} KiB".format(kdiv, c=0 if n % kmod == 0 else 2)

    gmod = pow(2, 30)
    gdiv = n / gmod
    if gdiv < 1:
        return "{0:.{c}f} MiB".format(mdiv, c=0 if n % mmod == 0 else 2)

    tmod = pow(2, 40)
    tdiv = n / tmod
    if tdiv < 1:
        return "{0:.{c}f} GiB".format(gdiv, c=0 if n % gmod == 0 else 2)

    pmod = pow(2, 50)
    pdiv = n / pmod
    if pdiv < 1:
        return "{0:.{c}f} TiB".format(tdiv, c=0 if n % tmod == 0 else 2)

    emod = pow(2, 60)
    ediv = n / emod
    if ediv < 1:
        return "{0:.{c}f} PiB".format(pdiv, c=0 if n % pmod == 0 else 2)

    zmod = pow(2, 70)
    zdiv = n / zmod
    if zdiv < 1:
        return "{0:.{c}f} EiB".format(ediv, c=0 if n % emod == 0 else 2)

    ymod = pow(2, 80)
    ydiv = n / ymod
    if ydiv < 1:
        return "{0:.{c}f} ZiB".format(ediv, c=0 if n % zmod == 0 else 2)

    return "{0:.{c}f} YiB".format(ydiv, c=0 if n % ymod == 0 else 2)

def read_log(log_dir):
    log = None
    for l in os.scandir(log_dir):
        if l.is_file():
            if log is None:
                log = pd.read_csv(l)
            else:
                log = log.append(pd.read_csv(l))
    return log

def mask_first_and_last(x):
    mask = [True]*len(x)
    mask[0] = False
    mask[1] = False
    mask[-2] = False
    mask[-1] = False
    return mask


log_dir = Path(sys.argv[1])
# Read tests logs
log = read_log(log_dir)

# print(log)
# Remove first and last two samples of every test
mask = log.groupby(['bench', 'size', 'unit']).transform(
    mask_first_and_last)['avg_value']
log = log.loc[mask]
print(log)
log.sort_values(by='size', inplace=True)
# log['throughput'] = 8 * log['size'] * log['messages']
log['label'] = [bytes_label(v) for k, v in log['size'].iteritems()]

log['multiplier'] = [time_conversion(v) for _,v in log['unit'].iteritems()]
log['values'] = log['avg_value'] * log['multiplier']

log = log.reset_index()

# ALL msg/s
fig, axes = plt.subplots()

g = sns.lineplot(data=log, x='label', y='values', estimator="median",
             ci='sd', err_style='band', palette=palette,
             hue='bench')
g.set_yscale('log')
plt.grid(which='major', color='grey', linestyle='-', linewidth=0.1)
plt.grid(which='minor', color='grey', linestyle=':', linewidth=0.1, axis='y')

plt.xticks(rotation=72.5)
plt.xlabel('Payload size (Bytes)')

plt.ylabel('Time (s)')
plt.legend(title='Bench', loc='upper center', bbox_to_anchor=(0.5, 1.3))
ticker = mpl.ticker.EngFormatter(unit='')
axes.yaxis.set_major_formatter(ticker)

plt.tight_layout()
#plt.show()
fig.savefig(img_dir.joinpath('zenoh-flow-serde.pdf'))

