// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import * as DamlLfTypeF from "../api/DamlLfType";
import { DamlLfValue } from "../api/DamlLfValue";
import * as DamlLfValueF from "../api/DamlLfValue";
import { Section } from "../Guide";
import { default as TimeInput } from "./index";

const timeParameter = DamlLfTypeF.timestamp();
const dateParameter = DamlLfTypeF.date();

const description = `
These inputs include a popover to select a date and time.
`;

export interface State {
  timeValue: DamlLfValue;
  timeValue2: DamlLfValue;
  dateValue: DamlLfValue;
}

export default class TimeInputGuide extends React.Component<{}, State> {
  constructor() {
    super({});
    this.state = {
      timeValue: DamlLfValueF.timestamp("2017-02-03T11:30:00Z"),
      timeValue2: DamlLfValueF.undef(),
      dateValue: DamlLfValueF.undef(),
    };
  }
  render(): JSX.Element {
    return (
      <Section title="Date and time inputs" description={description}>
        <TimeInput
          onChange={timeValue => {
            this.setState({ timeValue });
          }}
          parameter={timeParameter}
          disabled={false}
          argument={this.state.timeValue}
        />
        <p>
          <span>Value seen by parent:</span> {this.state.timeValue}
        </p>
        <TimeInput
          onChange={() => {
            return;
          }}
          parameter={timeParameter}
          disabled={true}
          argument={this.state.timeValue}
        />
        <p>
          <span>Value seen by parent:</span> {this.state.timeValue2}
        </p>
        <TimeInput
          onChange={timeValue2 => {
            this.setState({ timeValue2 });
          }}
          parameter={timeParameter}
          disabled={false}
          argument={this.state.timeValue2}
        />
        <p>
          <span>Value seen by parent:</span> {this.state.dateValue}
        </p>
        <TimeInput
          onChange={dateValue => {
            this.setState({ dateValue });
          }}
          parameter={dateParameter}
          disabled={false}
          argument={this.state.dateValue}
        />
      </Section>
    );
  }
}
