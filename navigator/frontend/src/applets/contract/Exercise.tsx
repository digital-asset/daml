// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ApolloClient } from "@apollo/client";
import { withApollo } from "@apollo/client/react/hoc";
import { ParameterForm, Strong } from "@da/ui-core";
import { DamlLfType } from "@da/ui-core/lib/api/DamlLfType";
import { DamlLfValue } from "@da/ui-core/lib/api/DamlLfValue";
import * as DamlLfValueF from "@da/ui-core/lib/api/DamlLfValue";
import { default as ParameterDataProvider } from "@da/ui-core/lib/ParameterForm/ApolloDataProvider";
import * as React from "react";
import { SubHeader } from "./ContractComponent";

interface Props {
  choice: string;
  parameter: DamlLfType;
  isLoading: boolean;
  error?: string;
  onSubmit(
    e: React.MouseEvent<HTMLButtonElement>,
    argument?: DamlLfValue,
  ): void;
  className?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  client: ApolloClient<any>;
}

interface Local {
  argument: DamlLfValue;
}

class Component extends React.Component<Props, Local> {
  private readonly paramDataProvider: ParameterDataProvider;

  constructor(props: Props) {
    super(props);
    this.paramDataProvider = new ParameterDataProvider(props.client);
    this.state = { argument: DamlLfValueF.initialValue(props.parameter) };
  }

  UNSAFE_componentWillReceiveProps(nextProps: Props) {
    // If the choice changes, re-initialize the argument
    // (trying to reuse as much argument values as possible).
    if (this.props.choice !== nextProps.choice) {
      this.setState({
        argument: DamlLfValueF.initialValue(nextProps.parameter),
      });
    }
  }

  render(): React.ReactElement<HTMLDivElement> {
    const { choice, parameter, className, isLoading } = this.props;
    return (
      <div className={className}>
        <SubHeader>
          <Strong>Choice: {choice}</Strong>
        </SubHeader>
        <ParameterForm
          parameter={parameter}
          argument={this.state.argument}
          disabled={isLoading}
          onChange={argument => this.setState({ argument })}
          onSubmit={this.props.onSubmit}
          error={this.props.error}
          partyIdProvider={this.paramDataProvider}
          contractIdProvider={this.paramDataProvider}
          typeProvider={this.paramDataProvider}
        />
      </div>
    );
  }
}

export default withApollo<Omit<Props, "client">>(Component);
