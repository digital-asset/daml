// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  ApolloClient,
  ApolloQueryResult,
  gql,
  ObservableSubscription,
} from "@apollo/client";
import { withApollo } from "@apollo/client/react/hoc";
import Moment from "moment";
import * as React from "react";
import { TimeType } from "../api/OpaqueTypes";
import { LedgerTimeQuery } from "../api/Queries";
import { utcStringToMoment } from "../util";

// ------------------------------------------------------------------------------------------------
// Props
// ------------------------------------------------------------------------------------------------

export interface LedgerTimeResult {
  id: string;
  time: string;
  type: TimeType;
}

export type LedgerTime = {
  value: Moment.Moment | undefined;
  readonly: boolean;
};

export interface InnerProps {
  ledgerTime: LedgerTime;
}

export interface ApolloProps {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  client: ApolloClient<any>;
}

export interface State {
  ledgerTime: Moment.Moment | undefined;
  queryTime: Moment.Moment | undefined;
  timeType: TimeType | undefined;
}

// ------------------------------------------------------------------------------------------------
// GraphQL query
// ------------------------------------------------------------------------------------------------

function getCurrentTime(state: State): Moment.Moment | undefined {
  switch (state.timeType) {
    case "static":
      return state.ledgerTime;
    case "wallclock":
      return Moment.utc();
    case "simulated":
      return state.ledgerTime
        ? state.ledgerTime.add(Moment.utc().diff(state.queryTime))
        : undefined;
    default:
      return undefined;
  }
}

function isTimeReadOnly(timeType: TimeType | undefined): boolean {
  switch (timeType) {
    case "static":
      return false;
    case "wallclock":
      return true;
    case "simulated":
      return true;
    default:
      return true;
  }
}

/** How often to recompute the current time */
function getUpdateInterval(timeType: TimeType | undefined) {
  switch (timeType) {
    case "static":
      return Infinity;
    case "wallclock":
      return 10000;
    case "simulated":
      return 10000;
    default:
      return Infinity;
  }
}

function resultToState(qr: ApolloQueryResult<LedgerTimeQuery>): State {
  const qd = qr.data; // .data as QueryData;
  if (qd && qd.ledgerTime) {
    return {
      ledgerTime: utcStringToMoment(qd.ledgerTime.time),
      timeType: qd.ledgerTime.type,
      queryTime: Moment.utc(),
    };
  } else {
    return {
      ledgerTime: undefined,
      timeType: undefined,
      queryTime: undefined,
    };
  }
}

export const timeQuery = gql`
  query LedgerTimeQuery {
    ledgerTime {
      id
      time
      type
    }
  }
`;

// ------------------------------------------------------------------------------------------------
// Component
// ------------------------------------------------------------------------------------------------

/**
 * A simple higher order component that adds a `ledgerTime` property, storing the current
 * ledger time.
 *
 * Note: this does not implement any loading or error handling. If the ledger time is not
 * (yet) available for any reason, its value will be undefined.
 */
export default function withLedgerTime<P>(
  C: React.ComponentType<InnerProps & P>,
): React.ComponentType<P> {
  type Props = P & ApolloProps;
  class Component extends React.Component<Props, State> {
    private timeoutId: number | undefined = undefined;
    private querySubscription: ObservableSubscription | undefined = undefined;

    constructor(props: Props) {
      super(props);
      this.state = {
        ledgerTime: undefined,
        queryTime: undefined,
        timeType: undefined,
      };
    }

    fetchData() {
      this.props.client
        .query<LedgerTimeQuery>({ query: timeQuery })
        .then(qr => {
          const newState = resultToState(qr);
          this.setState(newState);
          this.scheduleUpdate(getUpdateInterval(newState.timeType));
        });
    }

    startCacheWatcher() {
      this.stopCacheWatcher();
      const observableQuery = this.props.client.watchQuery<LedgerTimeQuery>({
        fetchPolicy: "cache-only",
        query: timeQuery,
      });
      const next = () => {
        const qr = observableQuery.getCurrentResult();
        const newState = resultToState(qr);
        this.setState(newState);
        this.scheduleUpdate(getUpdateInterval(newState.timeType));
      };
      this.querySubscription = observableQuery.subscribe({ next });
    }

    stopCacheWatcher() {
      if (this.querySubscription) {
        this.querySubscription.unsubscribe();
        this.querySubscription = undefined;
      }
    }

    cancelUpdate() {
      if (this.timeoutId) {
        clearTimeout(this.timeoutId);
        this.timeoutId = undefined;
      }
    }

    scheduleUpdate(interval: number) {
      if (interval < Infinity) {
        this.timeoutId = window.setTimeout(() => {
          this.forceUpdate();
          this.scheduleUpdate(interval);
        }, interval);
      }
    }

    componentDidMount() {
      this.fetchData();
      this.startCacheWatcher();
    }

    componentWillUnmount() {
      this.stopCacheWatcher();
      this.cancelUpdate();
    }

    render() {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const { ...rest } = this.props as any;
      const ledgerTime: LedgerTime = {
        value: getCurrentTime(this.state),
        readonly: isTimeReadOnly(this.state.timeType),
      };

      return <C {...rest} ledgerTime={ledgerTime} />;
    }
  }

  return withApollo<P>(Component) as React.ComponentClass<P>;
}
