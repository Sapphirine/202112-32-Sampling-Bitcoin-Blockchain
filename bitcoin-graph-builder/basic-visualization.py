#!/usr/bin/env python3.8
import pandas as pd
import os
import sys

from matplotlib import pyplot as plt
from pathlib import Path

def usage():
    print("Usage: ./basic-visualization.py <indegree_path> <outdegree_path>")
    sys.exit(1)

if len(sys.argv) < 3:
    usage()

indegree_path = sys.argv[1]
outdegree_path = sys.argv[2]

if len(indegree_path.strip()) == 0 or len(outdegree_path.strip()) == 0:
    usage()

indegree_graph = [x for x in indegree_path.split(os.path.sep) if len(x.strip()) > 0][-1]
outdegree_graph = [x for x in outdegree_path.split(os.path.sep) if len(x.strip()) > 0][-1]

p = Path(indegree_path)
dfs = [pd.read_parquet(f) for f in p.glob("*.parquet")]

df = pd.concat(dfs)

distDf = df.sort_values('indegree')

x = distDf['indegree'].to_numpy()
y = distDf['count'].to_numpy()

plt.plot(x, y)
plt.title(f'Log-Scale In-Degree Distribution ({indegree_graph})')
plt.ylabel('Count')
plt.xlabel('In-Degree')
plt.yscale('log')
plt.xscale('log')
plt.savefig(f'./{indegree_graph}.png')
plt.show()

# Out-degree
p = Path(outdegree_path)
dfs = [pd.read_parquet(f) for f in p.glob("*.parquet")]

df = pd.concat(dfs)

distDf = df.sort_values('outdegree')

x = distDf['outdegree'].to_numpy()
y = distDf['count'].to_numpy()

plt.plot(x, y)
plt.title(f'Log-Scale Out-Degree Distribution ({outdegree_graph})')
plt.ylabel('Count')
plt.xlabel('Out-Degree')
plt.yscale('log')
plt.xscale('log')
plt.savefig(f'./{outdegree_graph}.png')
plt.show()
