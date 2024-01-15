// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { Section } from "../Guide";
import Icon from "../Icon";
import Link from "../Link";
import styled from "../theme";
import {
  makeTabLink,
  TableActionBar,
  TableActionBarButton,
  TableActionBarConfigCheckbox,
  TableActionBarConfigSearchInput,
  TableActionBarSideMargin,
  TableActionBarSpace,
  TableActionBarTitle,
} from "./index";

const TableActionBarTabLink = makeTabLink(Link);

const Container = styled.div`
  margin: 1rem auto;
`;

export interface Config {
  search: string;
  isFrozen: boolean;
  includeArchived: boolean;
}

export interface State {
  config: Config;
  clicks: number;
  tab: number;
}

const description = `
This component displays a custom table action bar.
The content of the action bar is specified via React children.

You can put any element in the action bar.
The following components are styled specifically for table action bars:
- TableActionBarTitle
- TableActionBarButton
- TableActionBarCheckbox
- TableActionBarSearchInput
- TableActionBarTabLink
- TableActionBarTabSpace
- TableActionBarSideMargin
`;

export default class TableActionBarGuide extends React.Component<{}, State> {
  constructor(props: {}) {
    super(props);
    this.state = {
      config: {
        search: "",
        includeArchived: false,
        isFrozen: false,
      },
      tab: 1,
      clicks: 0,
    };
    this.onConfigChange = this.onConfigChange.bind(this);
  }

  render(): JSX.Element {
    return (
      <Section title="Table action bar" description={description}>
        <Container>
          <TableActionBar>
            <TableActionBarSideMargin />
            <TableActionBarTitle>{`Clicks: ${this.state.clicks}`}</TableActionBarTitle>
            <TableActionBarButton
              onClick={() => this.setState({ clicks: this.state.clicks + 1 })}>
              <Icon name="plus" />
              Add
            </TableActionBarButton>
            <TableActionBarButton
              onClick={() => this.setState({ clicks: this.state.clicks - 1 })}>
              <Icon name="cross" />
              Remove
            </TableActionBarButton>
            <TableActionBarButton onClick={() => this.setState({ clicks: 0 })}>
              <Icon name="refresh" />
              Reset
            </TableActionBarButton>
            <TableActionBarSideMargin />
          </TableActionBar>
        </Container>
        <Container>
          <TableActionBar>
            <TableActionBarSideMargin />
            <TableActionBarTitle>Custom action bar</TableActionBarTitle>
            <TableActionBarConfigCheckbox
              config={this.state.config}
              onConfigChange={this.onConfigChange}
              configKey="isFrozen"
              title="Frozen"
            />
            <TableActionBarConfigSearchInput
              width={"50%"}
              config={this.state.config}
              onConfigChange={this.onConfigChange}
              placeholder="Custom search"
            />
          </TableActionBar>
        </Container>
        <Container>
          <TableActionBar>
            <TableActionBarSideMargin />
            <TableActionBarTabLink
              title="Tab 1"
              href="/tab-1"
              onClick={() => this.setState({ tab: 1 })}
              isActive={this.state.tab === 1}
              count={this.state.clicks}
            />
            <TableActionBarTabLink
              title="Tab 2"
              href="/tab-2"
              onClick={() => this.setState({ tab: 2 })}
              isActive={this.state.tab === 2}
            />
            <TableActionBarTabLink
              title="Tab 3"
              href="/tab-3"
              onClick={() => this.setState({ tab: 3 })}
              isActive={this.state.tab === 3}
            />
            <TableActionBarSpace />
          </TableActionBar>
        </Container>
      </Section>
    );
  }

  onConfigChange(config: Config): void {
    this.setState({ config });
  }
}
