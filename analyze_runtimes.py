import json
import pdb
import pandas as pd
from collections import defaultdict

# file_name = "allQueryRuntimes.json"
file_name = "allQueryRuntimes-trainedOnCM.json"
with open(file_name, "r") as f:
    data = json.loads(f.read())

final_data = defaultdict(list)

for q, vals in data.items():
    final_data["query"].append(q)
    final_data["RL"].append(vals["RL"][-1])
    final_data["RL-worst"].append(max(vals["RL"]))
    final_data["postgres"].append(vals["postgres"][-1])

df = pd.DataFrame(final_data)
df = df.round()
mean = df["RL"].mean() / df["postgres"].mean()
mean_worst = df["RL-worst"].mean() / df["postgres"].mean()
print("RL/postgres: ", mean)
print("RL-worst/postgres: ", mean_worst)
print("RL mean {}, var {}, min {}, max {}".format(df["RL"].mean(),
    df["RL"].var(), df["RL"].min(), df["RL"].max()))
print("PG mean {}, var {}, min {}, max {}".format(df["postgres"].mean(),
    df["postgres"].var(), df["postgres"].min(), df["postgres"].max()))

# print(df["RL", "postgres"].mean())
pdb.set_trace()
