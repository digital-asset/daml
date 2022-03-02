// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { Section } from "../Guide";
import styled from "../theme";
import {
  DataColumnConfig,
  DataTableConfig,
  default as DataTable,
} from "./index";

const description = `
This component displays a list of data rows.
It is intended for the use with the graphql()
higher order component from apollo-client.
`;

interface RowData {
  id: string;
  contracts: number;
}

// Data, as loaded by apollo/graphql
interface Data {
  rows: RowData[];
  totalCount: number;
}

const demoData: RowData[] = [
  { id: "callablePayoutNumber_t3@Decimal_5", contracts: 1 },
  { id: "callablePayoutTime_t5@Decimal_4", contracts: 2 },
  { id: "mustPayAgreementNumber_t2@Decimal_2", contracts: 0 },
  { id: "mustPayAgreementTime_t4@Decimal_1", contracts: 10 },
];

const demoDataReversed = [...demoData].reverse();

// Function that transforms above data
// into a form suitable for DataTable
const demoDataExtractor = (data: Data) => ({
  data: data.rows,
  totalCount: data.totalCount,
});

function getData(config: DataTableConfig) {
  if (config.sort.length > 0 && config.sort[0].direction === "DESCENDING") {
    return {
      rows: demoDataReversed,
      totalCount: demoDataReversed.length,
    };
  } else {
    return {
      rows: demoData,
      totalCount: demoData.length,
    };
  }
}

const columns: DataColumnConfig<RowData, string | number>[] = [
  {
    key: "id",
    title: "ID",
    extractCellData: ({ id }: RowData) => id,
    createCell: createTextCell,
    sortable: true,
    width: 200,
    weight: 3,
    alignment: "left",
  },
  {
    key: "contracts",
    title: "# Contracts",
    extractCellData: ({ contracts }: RowData) => contracts,
    createCell: createTextCell,
    sortable: true,
    width: 100,
    weight: 0,
    alignment: "left",
  },
];

function createTextCell({ cellData }: { cellData: string | number }) {
  return <span>{cellData}</span>;
}

const Container = styled.div`
  width: 100%;
  height: 250px;
  flex: 0 1 auto;
  margin: 1rem auto;
`;

const StyledDataTable = styled(DataTable)`
  background-color: white;
`;

export interface State {
  config: DataTableConfig;
}

export default class DataTableGuide extends React.Component<{}, State> {
  constructor(props: {}) {
    super(props);
    this.state = {
      config: {
        search: "",
        filter: [],
        count: 100,
        sort: [],
      },
    };
    this.onConfigChange = this.onConfigChange.bind(this);
  }

  render(): JSX.Element {
    return (
      <Section title="Data table" description={description}>
        <Container>
          <StyledDataTable
            data={getData(this.state.config)}
            extractRowData={demoDataExtractor}
            config={this.state.config}
            hideActionRow={true}
            columns={columns}
            onConfigChange={this.onConfigChange}
            rowClassName={() => "ContractTable__row"}
            columnClassName="ContractTable__column"
            headerRowClassName="ContractTable__headerRow"
            headerColumnClassName="ContractTable__headerColumn"
          />
        </Container>
        <Container>
          <StyledDataTable
            title={"Templates"}
            data={getData(this.state.config)}
            extractRowData={demoDataExtractor}
            config={this.state.config}
            columns={columns}
            onConfigChange={this.onConfigChange}
            rowClassName={() => "ContractTable__row"}
            columnClassName="ContractTable__column"
            headerRowClassName="ContractTable__headerRow"
            headerColumnClassName="ContractTable__headerColumn"
          />
        </Container>
      </Section>
    );
  }

  onConfigChange(config: DataTableConfig): void {
    this.setState({ config });
  }
}
