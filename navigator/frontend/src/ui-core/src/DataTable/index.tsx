// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import deepEqual from "deep-equal";
import * as React from "react";
import { AutoSizer, Table } from "react-virtualized";
import {
  ColumnConfig,
  createColumns,
  TableConfig,
  TableContainer,
  TableOuterWrapper,
  TableRowDataGetter,
} from "../Table";
import {
  TableActionBar,
  TableActionBarConfigSearchInput,
  TableActionBarSideMargin,
  TableActionBarTitle,
} from "../TableActionBar";
import { hardcodedStyle } from "../theme";

// ----------------------------------------------------------------------------
// Table config
// ----------------------------------------------------------------------------
export type DataTableConfig = TableConfig;

export type DataColumnConfig<R, C> = ColumnConfig<R, C>;

export type DataTableRowDataGetter<R, D> = TableRowDataGetter<R, D>;

export function renderActionBar<C extends DataTableConfig>(props: {
  readonly config: C;
  readonly title?: string;
  readonly actionRowContent?: React.ReactNode;
  onConfigChange?(config: C): void;
}): JSX.Element {
  const { actionRowContent, title } = props;
  return actionRowContent !== undefined ? (
    <TableActionBar>{actionRowContent}</TableActionBar>
  ) : (
    <TableActionBar>
      <TableActionBarSideMargin />
      <TableActionBarTitle>{title}</TableActionBarTitle>
      <TableActionBarConfigSearchInput
        onConfigChange={props.onConfigChange}
        config={props.config}
        placeholder="Search"
      />
    </TableActionBar>
  );
}

// ----------------------------------------------------------------------------
// Component
// ----------------------------------------------------------------------------
export type DataTableRowClassGetter<
  C extends DataTableConfig,
  R, // RowData
> = (config: C, row: R, index: number) => string;

export interface Props<
  C extends DataTableConfig,
  R, // RowData
  D, // Raw data
> {
  data: D;
  extractRowData: DataTableRowDataGetter<R, D>;
  config: C;
  columns: DataColumnConfig<R, {}>[];
  title?: string;
  className?: string;
  columnClassName?: string;
  headerRowClassName?: string;
  headerColumnClassName?: string;
  rowClassName?: DataTableRowClassGetter<C, R>;
  hideActionRow?: boolean;
  actionRowContent?: React.ReactNode;
  onRowClick?(rowData: R): void;
  onConfigChange?(config: C): void;
}

export type State<R> = {
  data: R[];
  totalCount: number;
};

export default class DataTable<
  C extends DataTableConfig,
  R, // RowData
  D, // Raw data
> extends React.Component<Props<C, R, D>, State<R>> {
  readonly headerHeight: number = hardcodedStyle.tableHeaderHeight;
  readonly rowHeight: number = hardcodedStyle.tableRowHeight;
  readonly fetchIncrement: number = 100;

  constructor(props: Props<C, R, D>) {
    super(props);
    this.state = this.extractRowData(props);
  }

  // --------------------------------------------------------------------------
  // Life cycle and data loading
  // --------------------------------------------------------------------------

  UNSAFE_componentWillReceiveProps(nextProps: Props<C, R, D>): void {
    if (!deepEqual(this.props, nextProps)) {
      this.setState(this.extractRowData(nextProps));
    }
  }

  extractRowData(props: Props<C, R, D>): State<R> {
    const result = props.extractRowData(props.data);
    if (!Array.isArray(result.data)) {
      throw new Error('Property "data" missing in DataTable');
    }
    if (typeof result.totalCount !== "number") {
      throw new Error('Property "totalCount" missing in DataTable');
    }
    return result;
  }

  onScroll(height: number, y: number): void {
    if (
      y > length * this.rowHeight - height &&
      length < this.state.totalCount
    ) {
      const count = Math.min(
        length + this.fetchIncrement,
        this.state.totalCount,
      );
      if (this.props.onConfigChange) {
        this.props.onConfigChange({
          ...this.props.config,
          count,
        });
      }
    }
  }

  // --------------------------------------------------------------------------
  // Rendering
  // --------------------------------------------------------------------------
  render(): React.ReactElement<Table> {
    const {
      config,
      rowClassName = () => "",
      hideActionRow = false,
    } = this.props;

    return (
      <TableOuterWrapper className={this.props.className}>
        {hideActionRow ? null : renderActionBar(this.props)}
        <TableContainer>
          <AutoSizer>
            {({ width, height }: { width: number; height: number }) => (
              <Table
                width={width}
                height={height}
                headerHeight={this.headerHeight}
                rowHeight={this.rowHeight}
                rowCount={this.state.data.length}
                onRowClick={({ index }) => {
                  if (this.props.onRowClick) {
                    this.props.onRowClick(this.state.data[index]);
                  }
                }}
                rowGetter={({ index }) => this.state.data[index]}
                rowClassName={({ index }) => {
                  if (index >= 0) {
                    return rowClassName(config, this.state.data[index], index);
                  } else {
                    return this.props.headerRowClassName || "";
                  }
                }}
                onScroll={({ scrollTop }: { scrollTop: number }) =>
                  this.onScroll(height, scrollTop)
                }>
                {createColumns(this.props)}
              </Table>
            )}
          </AutoSizer>
        </TableContainer>
      </TableOuterWrapper>
    );
  }
}
