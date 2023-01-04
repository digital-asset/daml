// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as Moment from "moment";
import * as React from "react";
import { DamlLfTypePrim } from "../api/DamlLfType";
import { DamlLfValue } from "../api/DamlLfValue";
import * as DamlLfValueF from "../api/DamlLfValue";
import DayTimePicker from "../DateTimePicker";
import { StyledTextInput } from "../Input";
import Popover from "../Popover";
import { hardcodedStyle } from "../theme";
import { NonExhaustiveMatch, TypeErrorElement } from "../util";

export interface Props {
  parameter: DamlLfTypePrim;
  disabled: boolean;
  onChange(val: DamlLfValue): void;
  argument: DamlLfValue;
}

export interface State {
  open: boolean;
}

function formatMoment(m: Moment.Moment | undefined, t: "timestamp" | "date") {
  if (m === undefined) {
    return "";
  } else {
    if (t === "timestamp") {
      return m.format(hardcodedStyle.defaultTimeFormat);
    } else if (t === "date") {
      return m.format(hardcodedStyle.defaultDateFormat);
    } else {
      throw new NonExhaustiveMatch(t);
    }
  }
}

export default class TimeInput extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = {
      open: false,
    };
  }

  render(): JSX.Element {
    const { argument, parameter, disabled, onChange } = this.props;
    if (
      ((argument.type === "timestamp" || argument.type === "undefined") &&
        parameter.name === "timestamp") ||
      ((argument.type === "date" || argument.type === "undefined") &&
        parameter.name === "date")
    ) {
      const moment = DamlLfValueF.toMoment(argument);
      const paramName = parameter.name;
      return (
        <div>
          <Popover
            isOpen={this.state.open}
            onInteraction={(type, next) =>
              type !== "content" && this.setState({ open: next })
            }
            position={"bottom-start"}
            arrow={false}
            margin={2}
            target={
              <StyledTextInput
                value={formatMoment(moment, parameter.name)}
                onChange={() => {
                  return;
                }}
                placeholder={paramName === "timestamp" ? "Time" : "Date"}
                disabled={disabled}
              />
            }
            content={
              <DayTimePicker
                moment={moment}
                enableTime={paramName === "timestamp"}
                onChange={m => {
                  onChange(DamlLfValueF.fromMoment(m, paramName));
                  this.setState({ open: false });
                }}
              />
            }
          />
        </div>
      );
    } else {
      return <TypeErrorElement parameter={parameter} argument={argument} />;
    }
  }
}
