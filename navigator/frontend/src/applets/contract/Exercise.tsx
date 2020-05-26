// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  ParameterForm,
  Strong,
} from '@da/ui-core';
import { DamlLfType } from '@da/ui-core/lib/api/DamlLfType';
import { DamlLfValue } from '@da/ui-core/lib/api/DamlLfValue';
import * as DamlLfValueF from '@da/ui-core/lib/api/DamlLfValue';
import {
  default as ParameterDataProvider,
} from '@da/ui-core/lib/ParameterForm/ApolloDataProvider';
import * as React from 'react';
import { ApolloClient, withApollo } from 'react-apollo';
import { SubHeader } from './ContractComponent';

interface Props {
  choice: string;
  parameter: DamlLfType;
  isLoading: boolean;
  error?: string;
  onSubmit(e: React.MouseEvent<HTMLButtonElement>, argument?: DamlLfValue): void;
  className?: string;
  client: ApolloClient;
}

interface Local {
  argument: DamlLfValue;
}

class Component extends React.Component<Props, Local> {

  private typeProvider: ParameterDataProvider;

  constructor(props: Props) {
    super(props);
    this.typeProvider = new ParameterDataProvider(props.client);
    this.state = { argument: DamlLfValueF.initialValue(props.parameter) };
  }

  componentWillReceiveProps(nextProps: Props) {
    // If the choice changes, re-initialize the argument
    // (trying to reuse as much argument values as possible).
    if (this.props.choice !== nextProps.choice) {
      this.setState({ argument: DamlLfValueF.initialValue(nextProps.parameter) });
    }
  }

  render(): React.ReactElement<HTMLDivElement> {
    const { choice, parameter, className, isLoading } = this.props;
    return (
      <div className={className}>
        <SubHeader><Strong>Choice: {choice}</Strong></SubHeader>
        <ParameterForm
          parameter={parameter}
          argument={this.state.argument}
          disabled={isLoading}
          onChange={(argument) => this.setState({ argument })}
          onSubmit={this.props.onSubmit}
          error={this.props.error}
          typeProvider={this.typeProvider}
        />
      </div>
    );
  }
}

export default withApollo(Component);
