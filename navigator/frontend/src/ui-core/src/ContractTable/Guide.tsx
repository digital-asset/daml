// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { Section } from "../Guide";
import {
  TableActionBarButton,
  TableActionBarSideMargin,
  TableActionBarTitle,
} from "../TableActionBar";
import styled from "../theme";
import {
  ContractColumn,
  ContractTableConfig,
  DataProvider,
  default as ContractTable,
  ResultCallback,
  RowData,
} from "./index";

interface Contract {
  id: string;
  createEvent: {
    effectiveAt: string;
  };
  archiveEvent: {} | null;
  template: {
    id: string;
  };
}

const demoResult = {
  contracts: [
    {
      id: "c01",
      createEvent: {
        effectiveAt: "2017-06-05T11:34:21Z",
      },
      archiveEvent: null,
      template: {
        id: "1111",
      },
      argument: {},
    },
    {
      id: "c02",
      createEvent: {
        id: "2",
        effectiveAt: "2017-06-05T11:35:26Z",
      },
      archiveEvent: null,
      template: {
        id: "2222",
      },
      argument: {},
    },
    {
      id: "c03",
      createEvent: {
        id: "2",
        effectiveAt: "2017-06-05T11:36:26Z",
      },
      archiveEvent: {
        id: "3",
      },
      template: {
        id: "3333",
      },
      argument: {},
    },
  ],
  totalCount: 3,
};

class DemoDataProvider implements DataProvider<ContractTableConfig> {
  fetchData(config: ContractTableConfig, onResult: ResultCallback) {
    let contracts = demoResult.contracts.slice();
    if (!config.includeArchived) {
      contracts = contracts.filter(c => c.archiveEvent === null);
    }
    if (config.sort.length > 0 && config.sort[0].direction === "DESCENDING") {
      contracts.reverse();
    }
    onResult({
      contracts,
      totalCount: demoResult.totalCount,
    });
  }

  startCacheWatcher(
    _config: ContractTableConfig,
    _onResult: ResultCallback,
  ): void {
    // empty
  }

  stopCacheWatcher(): void {
    // empty
  }
}

const demoDataProvider = new DemoDataProvider();

const columns: ContractColumn<RowData, string>[] = [
  {
    key: "id",
    title: "ID",
    extractCellData: ({ id }: Contract) => id,
    createCell: createTextCell,
    sortable: true,
    width: 80,
    weight: 0,
    alignment: "left",
  },
  {
    key: "template.id",
    title: "Template ID",
    extractCellData: ({ template }: Contract) => template.id,
    createCell: createTextCell,
    sortable: true,
    width: 200,
    weight: 3,
    alignment: "left",
  },
  {
    key: "time",
    title: "Time",
    extractCellData: (contract: Contract) => contract.createEvent.effectiveAt,
    createCell: createTextCell,
    sortable: false,
    width: 180,
    weight: 2,
    alignment: "left",
  },
];

function createTextCell({ cellData }: { cellData: string | number }) {
  return <span>{cellData}</span>;
}

const Container = styled.div`
  width: 100%;
  height: 200px;
  flex: 0 1 auto;
  margin: 1rem auto;
`;

const StyledContractTable = styled(ContractTable)`
  background-color: white;
`;

export interface State {
  config: ContractTableConfig;
}

export default class ContractTableGuide extends React.Component<{}, State> {
  constructor(props: {}) {
    super(props);
    this.state = {
      config: {
        search: "",
        filter: [],
        includeArchived: false,
        sort: [],
        count: 100,
        isFrozen: false,
      },
    };
    this.onConfigChange = this.onConfigChange.bind(this);
  }

  render(): JSX.Element {
    return (
      <Section
        title="Contract table"
        description="This component displays a list of contracts.">
        <Container>
          <StyledContractTable
            dataProvider={demoDataProvider}
            config={this.state.config}
            hideActionRow={true}
            columns={columns}
            onConfigChange={this.onConfigChange}
            rowClassName="ContractTable__row"
            columnClassName="ContractTable__column"
            headerRowClassName="ContractTable__headerRow"
            headerColumnClassName="ContractTable__headerColumn"
            archivedRowClassName="ContractTable__archived"
            createdRowClassName="ContractTable__created"
            removedRowClassName="ContractTable__removed"
          />
        </Container>
        <Container>
          <StyledContractTable
            title={"Contracts"}
            dataProvider={demoDataProvider}
            config={this.state.config}
            columns={columns}
            onConfigChange={this.onConfigChange}
            rowClassName="ContractTable__row"
            columnClassName="ContractTable__column"
            headerRowClassName="ContractTable__headerRow"
            headerColumnClassName="ContractTable__headerColumn"
            archivedRowClassName="ContractTable__archived"
            createdRowClassName="ContractTable__created"
            removedRowClassName="ContractTable__removed"
          />
        </Container>
        <Container>
          <StyledContractTable
            title={"Contracts"}
            actionRowContent={[
              <TableActionBarSideMargin key="left" />,
              <TableActionBarTitle key="title">
                Custom header
              </TableActionBarTitle>,
              <TableActionBarButton
                key="button1"
                onClick={() => {
                  return;
                }}>
                Action
              </TableActionBarButton>,
            ]}
            dataProvider={demoDataProvider}
            config={this.state.config}
            columns={columns}
            onConfigChange={this.onConfigChange}
            rowClassName="ContractTable__row"
            columnClassName="ContractTable__column"
            headerRowClassName="ContractTable__headerRow"
            headerColumnClassName="ContractTable__headerColumn"
            archivedRowClassName="ContractTable__archived"
            createdRowClassName="ContractTable__created"
            removedRowClassName="ContractTable__removed"
          />
        </Container>
      </Section>
    );
  }

  onConfigChange(config: ContractTableConfig): void {
    this.setState({ config });
  }
}
