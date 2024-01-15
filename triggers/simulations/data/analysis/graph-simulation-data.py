# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import argparse
import os.path
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots


bar_width = 0.3


def strip_package_id(identifier):
    return ':'.join(identifier.split(':')[1:])


def subplot_titles(trigger):
    return [f'{trigger} ACS', f'{trigger} ACS Diff with Ledger', f'{trigger} Submission Results', f'{trigger} Resource Usage']


def trigger_labels(triggers, subplots, data):
    trigger_data = { data['trigger-id'][row]: strip_package_id(data['trigger-def-ref'][row]) for row in range(0, len(data)) }
    return tuple([ subplot_titles(trigger_data[trigger])[subplot-1] for subplot in subplots for trigger in triggers ])


def completion_labels(code):
    if code == 0 or code == '0':
        return 'OK'
    else:
        return code


def plot_acs_data(data, triggers, fig, line):
    for index, trigger in enumerate(triggers):
        trigger_data = data.loc[lambda row: row['trigger-id'] == trigger, :]
        fig.update_yaxes(title_text="Number of Contracts", row=line, col=index+1, secondary_y=False)
        fig.add_trace(go.Scatter(x=trigger_data['timestamp'], y=trigger_data['active-contracts'], mode="markers", marker=dict(color='blue'), name="Active", legendgroup='active-contract-group', showlegend=index == 0, legendgrouptitle_text="Trigger ACS", legendrank=3), secondary_y=False, col=index+1, row=line)
        fig.add_trace(go.Scatter(x=trigger_data['timestamp'], y=trigger_data['pending-contracts'], mode="markers", marker=dict(color='red'), name="Pending", legendgroup='pending-contract-group', showlegend=index == 0, legendrank=4), secondary_y=False, col=index+1, row=line)
        fig.add_trace(go.Bar(x=trigger_data['timestamp'], y=trigger_data['submissions'], width=[ bar_width for _ in trigger_data['submissions'] ], marker=dict(color='purple'), hovertemplate="%{y}", name="Submissions", opacity=0.3, legendgroup='submission-group', showlegend=index == 0, legendrank=2), secondary_y=False, col=index+1, row=line)


def plot_diff_data(data, diff_data, triggers, fig, line):
    templates = set(diff_data['template-id'])
    for index, trigger in enumerate(triggers):
        trigger_data = data.loc[lambda row: row['trigger-id'] == trigger, :]
        trigger_diff_data = diff_data.loc[lambda row: row['trigger-id'] == trigger, :]
        fig.update_yaxes(title_text="Number of Contracts", row=line, col=index+1, secondary_y=False)
        for template in templates:
            template_data = trigger_diff_data.loc[lambda row: row['template-id'] == template, :]
            fig.add_trace(go.Scatter(x=trigger_data['timestamp'], y=template_data['contract-additions'], mode="markers", marker=dict(color='blue'), name="Additions", legendgroup=f'{template}-contract-additions-group', legendgrouptitle_text=f"{strip_package_id(template)} Diff", showlegend=index == 0), secondary_y=False, col=index+1, row=line)
            fig.add_trace(go.Scatter(x=trigger_data['timestamp'], y=template_data['contract-deletions'], mode="markers", marker=dict(color='red'), name="Deletions", legendgroup=f'{template}-contract-deletions-group', showlegend=index == 0), secondary_y=False, col=index+1, row=line)
        fig.add_trace(go.Bar(x=trigger_data['timestamp'], y=trigger_data['submissions'], width=[ bar_width for _ in trigger_data['submissions'] ], marker=dict(color='purple'), hovertemplate="%{y}", name="Submissions", opacity=0.3, legendgroup='submission-group', showlegend=False, legendrank=2), secondary_y=False, col=index+1, row=line)


def plot_submission_data(data, triggers, fig, line):
    status_codes = sorted(list(set(data['completion-status-code'])))
    status_codes.reverse()
    colours = px.colors.sample_colorscale(px.colors.diverging.RdYlGn, [n/(len(status_codes) - 1) for n in range(len(status_codes))])
    for index, trigger in enumerate(triggers):
        trigger_submission_data = data.loc[lambda row: row['trigger-id'] == trigger, :]
        submission_data = []
        for timestamp in set(trigger_submission_data['timestamp']):
            for status in set(trigger_submission_data['completion-status-code']):
                status_data = trigger_submission_data.loc[lambda row: row['timestamp'] == timestamp, :].loc[lambda row: row['completion-status-code'] == status, :]
                if status == 'INCOMPLETE' or len(status_data['submission-duration'].index) == 0:
                    submission_data.append({'timestamp': timestamp, 'completion-status-code': completion_labels(status), 'submissions': len(status_data.index), 'label': f"count {len(status_data['submission-duration'].index)}"})
                else:
                    status_data['submission-duration'] = status_data['submission-duration'].astype(float) / 1000.0
                    submission_data.append({'timestamp': timestamp, 'completion-status-code': completion_labels(status), 'submissions': len(status_data.index), 'label': status_data['submission-duration'].describe(percentiles=[0.25, 0.5, 0.75, 0.9, 0.95]).to_string().replace("\n", "<br>")})
        fig.update_yaxes(title_text="Number of Contracts", row=line, col=index+1, secondary_y=False)
        fig.update_layout(barmode="stack")
        for status_index, status in enumerate(status_codes):
            df = pd.DataFrame([ d for d in submission_data if d['completion-status-code'] == completion_labels(status)])
            if status_index == 0:
                if df.empty:
                    fig.add_trace(go.Bar(x=[0], y=[0], marker=dict(color=colours[status_index]), name=completion_labels(status), hovertemplate="<br>Submission/Completion times (s):<br>count 0", legendgroup=f'{completion_labels(status)}-group', legendgrouptitle_text='Completion Status', showlegend=index==0), secondary_y=False, col=index+1, row=line)
                else:
                    fig.add_trace(go.Bar(x=df['timestamp'], y=df['submissions'], customdata=df['label'], width=[ bar_width for _ in df['timestamp'] ], marker=dict(color=colours[status_index]), name=completion_labels(status), hovertemplate="<br>Submission/Completion times (s):<br>%{customdata}", legendgroup=f'{completion_labels(status)}-group', legendgrouptitle_text='Completion Status', showlegend=index==0), secondary_y=False, col=index+1, row=line)
            else:
                if df.empty:
                    fig.add_trace(go.Bar(x=[0], y=[0], marker=dict(color=colours[status_index]), name=completion_labels(status), hovertemplate="<br>Submission/Completion times (s):<br>count 0", legendgroup=f'{completion_labels(status)}-group', showlegend=index==0), secondary_y=False, col=index+1, row=line)
                else:
                    fig.add_trace(go.Bar(x=df['timestamp'], y=df['submissions'], customdata=df['label'], width=[ bar_width for _ in df['timestamp'] ], marker=dict(color=colours[status_index]), name=completion_labels(status), hovertemplate="<br>Submission/Completion times (s):<br>%{customdata}", legendgroup=f'{completion_labels(status)}-group', showlegend=index==0), secondary_y=False, col=index+1, row=line)


def plot_resource_data(data, triggers, fig, line):
    for index, trigger in enumerate(triggers):
        trigger_data = data.loc[lambda row: row['trigger-id'] == trigger, :]
        fig.add_trace(go.Scatter(x=trigger_data['timestamp'], y=trigger_data['rule-evaluation-time']/1000000, mode="markers", marker=dict(color='blue'), name="Rule Eval Time (ms)", legendgroup='rule-eval-time', legendgrouptitle_text='Resource Usage', showlegend=index == 0), secondary_y=False, col=index+1, row=line)
        fig.add_trace(go.Scatter(x=trigger_data['timestamp'], y=trigger_data['percentage-heap-used']*100, mode="markers", marker=dict(color='blue'), name="JVM Heap Used (%)", legendgroup='heap-used-group', showlegend=index == 0, visible='legendonly'), secondary_y=False, col=index+1, row=line)
        fig.add_trace(go.Scatter(x=trigger_data['timestamp'], y=trigger_data['gc-time'], mode="markers", marker=dict(color='blue'), name="JVM GC Time (ms)", legendgroup='gc-time-group', showlegend=index == 0, visible='legendonly'), secondary_y=False, col=index+1, row=line)
        fig.add_trace(go.Bar(x=trigger_data['timestamp'], y=trigger_data['submissions'], width=[ bar_width for _ in trigger_data['submissions'] ], marker=dict(color='purple'), hovertemplate="%{y}", name="Submissions", opacity=0.3, legendgroup='submission-group', showlegend=False, legendrank=2), secondary_y=False, col=index+1, row=line)


def plot_data(data, diff_data, submission_data, title):
    subplots = list(range(1, len(subplot_titles(''))+1))
    triggers = list(sorted(set(data['trigger-id'])))
    fig = make_subplots(cols=len(triggers), rows=len(subplots), specs=[[{"secondary_y": True} for _ in triggers] for _ in subplots], subplot_titles=trigger_labels(triggers, subplots, data), shared_xaxes=True)
    fig.update_layout(title_text=title)
    plot_acs_data(data, triggers, fig, subplots[0])
    plot_diff_data(data, diff_data, triggers, fig, subplots[1])
    plot_submission_data(submission_data, triggers, fig, subplots[2])
    plot_resource_data(data, triggers, fig, subplots[3])
    for index, _ in enumerate(triggers):
        fig.update_xaxes(title_text='Simulation Time (s)', row=len(subplots), col=index+1)
    fig.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Plot trigger simulation performance data for analysis")
    parser.add_argument("data", metavar="DIR", help="Directory containing saved trigger simulation data (i.e. CSV files)")
    parser.add_argument("--title", dest="title", metavar="TITLE", help="Title for trigger simulation graph", default="Trigger Simulation Performance Data")
    parser.add_argument("--metric-csv-file", dest="metric_csv_file", metavar="CSV", help="Base file name for trigger simulation CSV metric data", default="trigger-simulation-metrics-data.csv")
    parser.add_argument("--acs-csv-file", dest="acs_csv_file", metavar="CSV", help="Base file name for trigger simulation CSV ACS data", default="trigger-simulation-acs-data.csv")
    parser.add_argument("--submission-csv-file", dest="submission_csv_file", metavar="CSV", help="Base file name for trigger simulation CSV submission data", default="trigger-simulation-submission-data.csv")
    args = parser.parse_args()
    if not os.path.isdir(args.data):
        parser.error(f"{args.data} directory does not exist!")
    if not os.path.exists(f"{args.data}/{args.metric_csv_file}"):
        parser.error(f"{args.data} directory does not contain the expected CSV file {args.metric_csv_file}")
    if not os.path.exists(f"{args.data}/{args.acs_csv_file}"):
        parser.error(f"{args.data} directory does not contain the expected CSV file {args.acs_csv_file}")
    if not os.path.exists(f"{args.data}/{args.submission_csv_file}"):
        parser.error(f"{args.data} directory does not contain the expected CSV file {args.submission_csv_file}")
    data = pd.read_csv(f"{args.data}/{args.metric_csv_file}")
    data.update((data['timestamp'] - min(data['timestamp'])) / 1000)
    diff_data = pd.read_csv(f"{args.data}/{args.acs_csv_file}")
    diff_data.update((diff_data['timestamp'] - min(diff_data['timestamp'])) / 1000)
    submission_data = pd.read_csv(f"{args.data}/{args.submission_csv_file}")
    submission_data.update((submission_data['timestamp'] - min(submission_data['timestamp'])) / 1000)
    plot_data(data, diff_data, submission_data, args.title)
