# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import argparse
import os.path
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def strip_package_id(identifier):
    return ':'.join(identifier.split(':')[1:])


def subplot_titles(trigger):
    return [f'{trigger} ACS', f'{trigger} ACS Diff', f'{trigger} Resource Usage']


def trigger_labels(triggers, subplots, data):
    trigger_data = { data['trigger-id'][row]: strip_package_id(data['trigger-def-ref'][row]) for row in range(0, len(data)) }
    return tuple([ subplot_titles(trigger_data[trigger])[subplot-1] for subplot in subplots for trigger in triggers ])


def elapsed_time(data):
    return (data['timestamp'] - min(data['timestamp'])) / 1000


def plot_acs_data(data, triggers, fig, line):
    for index, trigger in enumerate(triggers):
        trigger_data = data.loc[lambda row: row['trigger-id'] == trigger, :]
        fig.update_yaxes(title_text="Number of Contracts", row=line, col=index+1, secondary_y=False)
        fig.add_trace(go.Scatter(x=elapsed_time(trigger_data), y=trigger_data['completion-status-code'], mode="lines+markers", marker=dict(color='olive'), name="Completion Status", opacity=0.5, legendgroup='completion-group', showlegend=index == 0), secondary_y=True, col=index+1, row=line)
        fig.add_trace(go.Scatter(x=elapsed_time(trigger_data), y=trigger_data['submissions'], mode="lines+markers", marker=dict(color='purple'), name="Submissions", opacity=0.5, legendgroup='submission-group', showlegend=index == 0), secondary_y=False, col=index+1, row=line)
        fig.add_trace(go.Scatter(x=elapsed_time(trigger_data), y=trigger_data['active-contracts'], mode="markers", marker=dict(color='blue'), name="Active", legendgroup='active-contract-group', showlegend=index == 0, legendgrouptitle_text="Trigger ACS"), secondary_y=False, col=index+1, row=line)
        fig.add_trace(go.Scatter(x=elapsed_time(trigger_data), y=trigger_data['pending-contracts'], mode="markers", marker=dict(color='red'), name="Pending", legendgroup='pending-contract-group', showlegend=index == 0), secondary_y=False, col=index+1, row=line)


def plot_diff_data(data, diff_data, triggers, fig, line):
    templates = set(diff_data['template-id'])
    for index, trigger in enumerate(triggers):
        trigger_data = data.loc[lambda row: row['trigger-id'] == trigger, :]
        trigger_diff_data = diff_data.loc[lambda row: row['trigger-id'] == trigger, :]
        fig.update_yaxes(title_text="Number of Contracts", row=line, col=index+1, secondary_y=False)
        for template in templates:
            template_data = trigger_diff_data.loc[lambda row: row['template-id'] == template, :]
            fig.add_trace(go.Scatter(x=elapsed_time(trigger_data), y=template_data['contract-additions'], mode="markers", marker=dict(color='blue'), name="Additions", legendgroup=f'{template}-contract-additions-group', legendgrouptitle_text=f"{strip_package_id(template)} Diff", showlegend=index == 0), secondary_y=False, col=index+1, row=line)
            fig.add_trace(go.Scatter(x=elapsed_time(trigger_data), y=template_data['contract-deletions'], mode="markers", marker=dict(color='red'), name="Deletions", legendgroup=f'{template}-contract-deletions-group', showlegend=index == 0), secondary_y=False, col=index+1, row=line)
        fig.add_trace(go.Scatter(x=elapsed_time(trigger_data), y=trigger_data['completion-status-code'], mode="lines+markers", marker=dict(color='olive'), name="Completion Status", opacity=0.5, legendgroup='completion-group', showlegend=False), secondary_y=True, col=index+1, row=line)
        fig.add_trace(go.Scatter(x=elapsed_time(trigger_data), y=trigger_data['submissions'], mode="lines+markers", marker=dict(color='purple'), name="Submissions", opacity=0.5, legendgroup='submission-group', showlegend=False), secondary_y=False, col=index+1, row=line)


def plot_resource_data(data, triggers, fig, line):
    for index, trigger in enumerate(triggers):
        trigger_data = data.loc[lambda row: row['trigger-id'] == trigger, :]
        fig.add_trace(go.Scatter(x=elapsed_time(trigger_data), y=trigger_data['rule-evaluation-time']/1000000, mode="markers", marker=dict(color='blue'), name="Rule Eval Time (ms)", legendgroup='rule-eval-time', legendgrouptitle_text='Resource Usage', showlegend=index == 0), secondary_y=False, col=index+1, row=line)
        fig.add_trace(go.Scatter(x=elapsed_time(trigger_data), y=trigger_data['percentage-heap-used']*100, mode="markers", marker=dict(color='blue'), name="Heap Used (%)", legendgroup='heap-used-group', showlegend=index == 0, visible='legendonly'), secondary_y=False, col=index+1, row=line)
        fig.add_trace(go.Scatter(x=elapsed_time(trigger_data), y=trigger_data['gc-time'], mode="markers", marker=dict(color='blue'), name="GC Time (ms)", legendgroup='gc-time-group', showlegend=index == 0, visible='legendonly'), secondary_y=False, col=index+1, row=line)
        fig.add_trace(go.Scatter(x=elapsed_time(trigger_data), y=trigger_data['completion-status-code'], mode="lines+markers", marker=dict(color='olive'), name="Completion Status", opacity=0.5, legendgroup='completion-group', showlegend=False), secondary_y=True, col=index+1, row=line)
        fig.add_trace(go.Scatter(x=elapsed_time(trigger_data), y=trigger_data['submissions'], mode="lines+markers", marker=dict(color='purple'), name="Submissions", opacity=0.5, legendgroup='submission-group', showlegend=False), secondary_y=False, col=index+1, row=line)


def plot_data(data, diff_data, title):
    subplots = list(range(1, len(subplot_titles(''))+1))
    triggers = set(data['trigger-id'])
    fig = make_subplots(cols=len(triggers), rows=len(subplots), specs=[[{"secondary_y": True} for _ in triggers] for _ in subplots], subplot_titles=trigger_labels(triggers, subplots, data), shared_xaxes=True)
    fig.update_layout(title_text=title)
    plot_acs_data(data, triggers, fig, subplots[0])
    plot_diff_data(data, diff_data, triggers, fig, subplots[1])
    plot_resource_data(data, triggers, fig, subplots[2])
    for index, _ in enumerate(triggers):
        fig.update_xaxes(title_text='Simulation Time (s)', row=3, col=index+1)
    fig.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Plot trigger simulation performance data for analysis")
    parser.add_argument("--data", dest="data", metavar="DATA", help="Directory containing saved trigger simulation data (i.e. CSV files)", required=True)
    parser.add_argument("--title", dest="title", metavar="TITLE", help="Title for trigger simulation graph", default="Trigger Simulation Performance Data")
    args = parser.parse_args()
    if not os.path.isdir(args.data):
        parser.error(f"{args.data} directory does not exist!")
    if not os.path.exists(f"{args.data}/trigger-simulation-metrics-data.csv"):
        parser.error(f"{args.data} directory does not contain the expected CSV file trigger-simulation-metrics-data.csv")
    if not os.path.exists(f"{args.data}/trigger-simulation-acs-data.csv"):
        parser.error(f"{args.data} directory does not contain the expected CSV file trigger-simulation-acs-data.csv")
    data = pd.read_csv(f"{args.data}/trigger-simulation-metrics-data.csv")
    diff_data = pd.read_csv(f"{args.data}/trigger-simulation-acs-data.csv")
    plot_data(data, diff_data, args.title)

