// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { gql } from "@apollo/client";
import { MutateProps, withMutation } from "@apollo/client/react/hoc";
import Moment from "moment";
import * as React from "react";
import styled from "styled-components";
import Button from "../Button";
import DateTimePicker from "../DateTimePicker";
import Icon from "../Icon";
import Popover from "../Popover";
import { hardcodedStyle } from "../theme";
import { momentToUtcString } from "../util";
import withLedgerTime, { InnerProps } from "../withLedgerTime";

const LeftIcon = styled(Icon)`
  margin-right: 0.5rem;
  font-size: 1.4rem;
`;

const RightIcon = styled(Icon)`
  margin-left: 0.5rem;
  font-size: 1rem;
`;

const InlineDiv = styled.div`
  display: inline;
`;

type Props = MutateProps & InnerProps;

interface State {
  isOpen: boolean;
}

class AdvanceTime extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = {
      isOpen: false,
    };
    this.submit = this.submit.bind(this);
  }

  submit(m: Moment.Moment) {
    const mutate = this.props.mutate;
    const time = momentToUtcString(m);
    mutate({ variables: { time } });
    setTimeout(() => this.setState({ isOpen: false }), 10);
  }

  render() {
    const { ledgerTime } = this.props;
    const formattedTime = ledgerTime.value
      ? ledgerTime.value.format(hardcodedStyle.defaultTimeFormat)
      : "LOADING";
    return ledgerTime.readonly ? (
      <InlineDiv>
        <Button
          type="nav-transparent"
          onClick={() => {
            return;
          }}>
          <LeftIcon name="clock" />
          {formattedTime}
        </Button>
      </InlineDiv>
    ) : (
      <Popover
        margin={0}
        isOpen={this.state.isOpen}
        onInteraction={(_t, n) => this.setState({ isOpen: n })}
        position={"bottom"}
        target={
          <InlineDiv>
            <Button
              type="nav-transparent"
              onClick={() => {
                return;
              }}>
              <LeftIcon name="clock" />
              {formattedTime}
              <RightIcon name="chevron-down" />
            </Button>
          </InlineDiv>
        }
        content={
          <DateTimePicker
            moment={ledgerTime.value}
            enableTime={true}
            onChange={m => this.submit(m)}
          />
        }
      />
    );
  }
}

export const defaultAdvanceTimeQuery = gql`
  mutation advanceTime($time: Time!) {
    advanceTime(time: $time) {
      id
      time
      type
    }
  }
`;

/** Make an AdvanceTime component with custom GraphQL queries */
function makeAdvanceTime(advanceTimeQuery: typeof defaultAdvanceTimeQuery) {
  return withMutation(advanceTimeQuery)(withLedgerTime(AdvanceTime));
}

/** An AdvanceTime component with default sandbox GraphQL queries */
export default makeAdvanceTime(defaultAdvanceTimeQuery);
