// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ApolloClient } from "@apollo/client";
import { withApollo } from "@apollo/client/react/hoc";
import { ParameterForm, styled, Truncate } from "@da/ui-core";
import * as DamlLfValueF from "@da/ui-core/lib/api/DamlLfValue";
import { DamlLfValue } from "@da/ui-core/lib/api/DamlLfValue";
import { default as ParameterDataProvider } from "@da/ui-core/lib/ParameterForm/ApolloDataProvider";
import * as React from "react";
import { Template } from ".";

const Wrapper = styled.div`
  width: 100%;
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
  margin-top: 1rem;
  margin-bottom: 1rem;
`;

interface Props {
  template: Template;
  isLoading: boolean;
  onSubmit(
    e: React.MouseEvent<HTMLButtonElement>,
    argument?: DamlLfValue,
  ): void;
  error?: string;
}

interface Local {
  argument: DamlLfValue;
}

interface ApolloProps {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  client: ApolloClient<any>;
}

class Component extends React.Component<Props & ApolloProps, Local> {
  private readonly paramDataProvider: ParameterDataProvider;

  constructor(props: Props & ApolloProps) {
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
        <Header>
          <Truncate>Template {template.id}</Truncate>
        </Header>
        <Content>
          <ParameterForm
            parameter={template.parameter}
            argument={this.state.argument}
            disabled={isLoading}
            onChange={argument => this.setState({ argument })}
            onSubmit={onSubmit}
            error={error}
            partyIdProvider={this.paramDataProvider}
            contractIdProvider={this.paramDataProvider}
            typeProvider={this.paramDataProvider}
          />
        </Content>
      </Wrapper>
    );
  }
}

export default withApollo<Props>(Component);
