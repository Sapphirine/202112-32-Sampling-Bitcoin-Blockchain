import pandas as pd
from matplotlib import pyplot as plt

from pathlib import Path

p = Path('./test-data/address-graph-indegree')
dfs = [pd.read_parquet(f) for f in p.glob("*.parquet")]

df = pd.concat(dfs)

distDf = df.sort_values('indegree')

x = distDf['indegree'].to_numpy()
y = distDf['count'].to_numpy()

plt.plot(x, y)
plt.title('Log-Scale In-Degree Distribution')
plt.ylabel('Count')
plt.xlabel('In-Degree')
plt.yscale('log')
plt.xscale('log')
plt.savefig('./indegree.png')
plt.show()

# Out-degree
p = Path('./test-data/address-graph-outdegree')
dfs = [pd.read_parquet(f) for f in p.glob("*.parquet")]

df = pd.concat(dfs)

distDf = df.sort_values('outdegree')

x = distDf['outdegree'].to_numpy()
y = distDf['count'].to_numpy()

plt.plot(x, y)
plt.title('Log-Scale Out-Degree Distribution')
plt.ylabel('Count')
plt.xlabel('Out-Degree')
plt.yscale('log')
plt.xscale('log')
plt.savefig('./outdegree.png')
plt.show()
