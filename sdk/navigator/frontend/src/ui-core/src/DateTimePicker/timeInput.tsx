// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// This DateTimePicker code is based on the input-moment library.
// See https://github.com/wangzuo/input-moment.
// The license of this library is included in the current folder.
//
// The code was copied in order to simplify extensive customization.
// In the future, we may decide to use another library to implement
// this component.
import Moment from "moment";
import * as React from "react";
import Button from "../Button";
import { default as styled, hardcodedStyle } from "../theme";

const isValid = (m: Moment.Moment | undefined): m is Moment.Moment =>
  m !== undefined && m.isValid();

const TimeWrapper = styled.div`
  display: flex;
  flex-flow: row nowrap;
  align-items: center;
  align-content: flex-start;
  justify-content: center;
  padding-top: 15px;
`;

const RoundInput = styled.input`
  width: 120px;
  display: flex;
  flex-wrap: nowrap;
  white-space: nowrap;
  justify-content: flex-start;
  align-items: center;
  padding: ${({ theme }) => theme.buttonPadding.join(" ")};
  border-radius: ${({ theme }) => theme.buttonRadius};
  border: none;
  font-size: ${hardcodedStyle.buttonFontSize};

  position: relative;
  left: 20px;
`;

const MovedButton = styled(Button)`
  position: relative;
  left: -20px;
`;

/**
 * Returns a moment that has the same date as `m`,
 * and a time as specfied by `s`.
 *
 * If `s` is not a valid time, returns `m`.
 */
function setTime(s: string, m: Moment.Moment): Moment.Moment {
  const time = Moment(s, ["H:mm", "H:mm:ss"]);
  if (time.isValid()) {
    const result = Moment(m);
    result.hour(time.hour());
    result.minute(time.minute());
    result.second(time.second());
    return result;
  } else {
    return m;
  }
}

export interface Props {
  moment: Moment.Moment | undefined;
  defaultMoment: Moment.Moment;
  onChange(moment: Moment.Moment): void;
  onSubmit(): void;
}

export interface State {
  value?: string;
}

export default class TimeInput extends React.Component<Props, State> {
  private input: HTMLInputElement | null;

  constructor(props: Props) {
    super(props);
    this.state = {
      value: undefined,
    };
  }

  filterCharacters(event: React.KeyboardEvent<HTMLInputElement>): void {
    // Only allow 0-9 and colon.
    if (event.charCode < 48 || event.charCode > 58) {
      event.preventDefault();
    }
  }

  setSelection(): void {
    if (!this.input || this.input.selectionStart === null) {
      return;
    }
    const i = this.input.value.indexOf(":");
    if (this.input.selectionStart < i) {
      this.input.setSelectionRange(0, i);
    } else {
      this.input.setSelectionRange(i + 1, this.input.value.length);
    }
  }

  displayMoment(): Moment.Moment {
    return isValid(this.props.moment)
      ? this.props.moment
      : this.props.defaultMoment;
  }

  displayValue(): string {
    return this.state.value
      ? this.state.value
      : this.displayMoment().format("HH:mm");
  }

  handleChange(): void {
    const result = setTime(this.displayValue(), this.displayMoment());
    this.setState({ value: undefined });
    this.props.onChange(result);
  }

  render(): JSX.Element {
    const displayValue = this.displayValue();
    return (
      <TimeWrapper>
        <RoundInput
          ref={e => {
            this.input = e;
          }}
          value={displayValue}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
            this.setState({ value: e.target.value })
          }
          onClick={() => this.setSelection()}
          onKeyPress={e => this.filterCharacters(e)}
          onBlur={() => this.handleChange()}
        />
        <MovedButton
          type="main"
          onClick={() => {
            this.handleChange();
            this.props.onSubmit();
          }}>
          Set
        </MovedButton>
      </TimeWrapper>
    );
  }
}
