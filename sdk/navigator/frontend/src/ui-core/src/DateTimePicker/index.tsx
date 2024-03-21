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
import styled from "../theme";
import { default as withLedgerTime, LedgerTime } from "../withLedgerTime";
import Calendar from "./calendar";
import TimeInput from "./timeInput";

const CalendarWrapper = styled.div`
  width: 280px;
  padding: 15px;
`;

const CenteringWrapper = styled.div`
  display: flex;
  flex-flow: row nowrap;
  align-items: center;
  align-content: flex-start;
  justify-content: center;
  padding-top: 15px;
`;

export interface Props {
  moment: Moment.Moment | undefined;
  enableTime: boolean;
  onChange(moment: Moment.Moment): void;
}

export interface State {
  defaultMoment: Moment.Moment;
  moment: Moment.Moment | undefined;
}

class InputMoment extends React.Component<
  Props & { ledgerTime: LedgerTime },
  State
> {
  constructor(props: Props & { ledgerTime: LedgerTime }) {
    super(props);

    const defaultMoment =
      props.ledgerTime.value || Moment().utc().startOf("day");

    this.state = {
      defaultMoment: props.enableTime
        ? defaultMoment
        : defaultMoment.startOf("day"),
      moment: props.moment,
    };

    this.onChange = this.onChange.bind(this);
    this.onSubmit = this.onSubmit.bind(this);
  }

  UNSAFE_componentWillReceiveProps(nextProps: Props): void {
    if (
      this.props.moment !== nextProps.moment &&
      this.props.moment !== undefined &&
      !this.props.moment.isSame(nextProps.moment)
    ) {
      this.setState({ moment: nextProps.moment });
    }
  }

  onChange(moment: Moment.Moment): void {
    this.setState({ moment });
  }

  onSubmit(): void {
    if (this.state.moment) {
      this.props.onChange(this.state.moment);
    } else {
      this.props.onChange(this.state.defaultMoment);
    }
  }

  render(): JSX.Element {
    const { enableTime, ledgerTime, children } = this.props;
    return (
      <CalendarWrapper {...{ ledgerTime, children }}>
        <div>
          <Calendar
            moment={this.state.moment}
            defaultMoment={this.state.defaultMoment}
            onChange={this.onChange}
          />
          {enableTime ? (
            <TimeInput
              moment={this.state.moment}
              defaultMoment={this.state.defaultMoment}
              onChange={this.onChange}
              onSubmit={this.onSubmit}
            />
          ) : (
            <CenteringWrapper>
              <Button type="main" onClick={this.onSubmit}>
                Set
              </Button>
            </CenteringWrapper>
          )}
        </div>
      </CalendarWrapper>
    );
  }
}

const UI = withLedgerTime(InputMoment);

export default UI;
