# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import argparse
import json
import os
import pandas as pd
import plotly.express as px
from dash import Dash, html, dash_table, dcc
from json.decoder import JSONDecodeError

def get_jmh_data(base_dir):
    result = []
    for dar_file in os.listdir(base_dir):
        for script in os.listdir("{}/{}".format(base_dir, dar_file)):
            for jmh_data in os.listdir("{}/{}/{}".format(base_dir, dar_file, script)):
                if jmh_data.endswith(".json"):
                    with open("{}/{}/{}/{}".format(base_dir, dar_file, script, jmh_data)) as fd:
                        try:
                            data = json.load(fd)
                        except JSONDecodeError:
                            data = []
                        fd.close()
                        if len(data) > 0:
                            entry = {
                                "dar-file": dar_file,
                                "script": script,
                                "choice": data[0]["params"]["choiceName"],
                                "benchmark-data": data[0]["primaryMetric"],
                            }
                            result.append(entry)
    return result

def get_graph_data(base_data, update_data):
    result = {"type": [], "choice": [], "score": [], "min-score": [], "max-score": [], "score-unit": [], "dar#script": []}
    for entry in base_data:
        result["type"].append("base")
        result["choice"].append(entry["choice"])
        result["score"].append(entry["benchmark-data"]["score"])
        result["min-score"].append(entry["benchmark-data"]["scoreConfidence"][0])
        result["max-score"].append(entry["benchmark-data"]["scoreConfidence"][1])
        result["score-unit"].append(entry["benchmark-data"]["scoreUnit"])
        result["dar#script"].append("{}#{}".format(entry["dar-file"], entry["script"]))
    for entry in update_data:
        result["type"].append("update")
        result["choice"].append(entry["choice"])
        result["score"].append(entry["benchmark-data"]["score"])
        result["min-score"].append(entry["benchmark-data"]["scoreConfidence"][0])
        result["max-score"].append(entry["benchmark-data"]["scoreConfidence"][1])
        result["score-unit"].append(entry["benchmark-data"]["scoreUnit"])
        result["dar#script"].append("{}#{}".format(entry["dar-file"], entry["script"]))
    return pd.DataFrame(result)

def display_graph(graph_data):
    return px.bar(graph_data, x="choice", y="score", color="type", barmode="group", facet_row="dar#script")

def main():
    parser = argparse.ArgumentParser(description="Display JMH Benchmarking Data for Snapshot Files")
    parser.add_argument("base_dir", type=str, help="Directory holding baseline snapshot data")
    parser.add_argument("update_dir", type=str, help="Directory holding updated snapshot data")
    args = parser.parse_args()
    base_data = get_jmh_data(args.base_dir)
    update_data = get_jmh_data(args.update_dir)
    graph_data = get_graph_data(base_data, update_data)

    app = Dash()
    app.layout = [
        html.Div(children="JMH Benchmarking Data for Snapshot Files"),
        html.Div(children=[dcc.Graph(figure=display_graph(graph_data))], style={"width":"100%", "height":"100vh"})
    ]
    app.run(host="127.0.0.1", port=8080)

if __name__ == "__main__":
    main()
