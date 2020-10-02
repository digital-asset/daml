// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ParameterForm, styled, Truncate } from '@da/ui-core';
import * as DamlLfValueF from '@da/ui-core/lib/api/DamlLfValue';
import { DamlLfValue } from '@da/ui-core/lib/api/DamlLfValue';
import {
  default as ParameterDataProvider,
} from '@da/ui-core/lib/ParameterForm/ApolloDataProvider';
import * as React from 'react';
import { ApolloClient, withApollo } from 'react-apollo';
import { Template } from '.'

const Wrapper = styled.div`
  width: 100%
`;

const Header = styled.div`
  height: 5rem;
  display: flex;
  background-color: ${({ theme }) => theme.colorShade};
  font-size: 1.25rem;
  padding: 0;
  padding-left: 2.5rem;
  padding-right: 2.5rem;
  align-items: center;
`;

const Content = styled.div`
  padding-left: 2.5rem;
  padding-right: 2.5rem;
  margin-top: 1.0rem;
  margin-bottom: 1.0rem;
`

interface Props {
  template: Template;
  isLoading: boolean;
  onSubmit(e: React.MouseEvent<HTMLButtonElement>, argument?: DamlLfValue): void;
  error?: string;
  client: ApolloClient;
}

interface Local {
  argument: DamlLfValue;
}

class Component extends React.Component<Props, Local> {

  private paramDataProvider: ParameterDataProvider;

  constructor(props: Props) {
    super(props);
    this.paramDataProvider = new ParameterDataProvider(props.client);
    this.state = {
      argument: DamlLfValueF.initialValue(props.template.parameter),
    };
  }

  render(): React.ReactElement<HTMLDivElement> {
    const { template, isLoading, onSubmit, error } = this.props;
    return (
      <Wrapper>
        <Header><Truncate>Template {template.id}</Truncate></Header>
        <Content>
          <ParameterForm
            parameter={template.parameter}
            argument={this.state.argument}
            disabled={isLoading}
            onChange={(argument) => this.setState({ argument })}
            onSubmit={onSubmit}
            error={error}
            typeProvider={this.paramDataProvider}
          />
        </Content>
      </Wrapper>
    );
  }
}

export default withApollo(Component);
