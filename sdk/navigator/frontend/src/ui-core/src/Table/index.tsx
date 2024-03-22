// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { Column } from "react-virtualized";
import styled, { hardcodedStyle } from "../theme";
import { createHeader } from "./HeaderCell";

export {
  createHeader,
  default as HeaderCell,
  HeaderCellProps,
} from "./HeaderCell";

// ----------------------------------------------------------------------------
// Table config
// ----------------------------------------------------------------------------

export type SortDirection = "ASCENDING" | "DESCENDING";

export function flipSortDirection(dir: SortDirection): SortDirection {
  return dir === "ASCENDING" ? "DESCENDING" : "ASCENDING";
}

export interface SortCriterion {
  /** Field to sort by */
  field: string;
  /** Sort direction */
  direction: SortDirection;
}

export interface FilterCriterion {
  /** Field to filter by */
  field: string;
  /** Filter value */
  value: string;
}

export type SearchCriterion = string;

/**
 * Config for the data query
 */
export interface TableConfig {
  /** Search string (smart filter) */
  search: SearchCriterion;
  /** Filter criteria */
  filter: FilterCriterion[];
  /** Number of items the query should return */
  count: number;
  /** Sort criteria */
  sort: SortCriterion[];
}

export interface CellRenderParams<C, R> {
  cellData: C;
  columnIndex: number;
  rowData: R;
  rowIndex: number;
}

export type CellAlignment =
  | "none" // Do nothing, output content as is
  | "left" // Left-align content
  | "center" // Center content
  | "right"; // Right-align content

/**
 * Column configuration
 */
export interface ColumnConfig<
  R, // RowData
  C, // CellData
> {
  /* Corresponding field (for sorting) */
  key: string;
  /** */
  title: string;
  /** If true, the user can click on the column header to sort by the column */
  sortable: boolean;
  /** Flexbox: initial width, in pixels */
  width: number;
  /** Flexbox: weight */
  weight: number;
  /** Horizontal alignment */
  alignment: CellAlignment;
  extractCellData(o: R): C;
  createCell(params: CellRenderParams<C, R>): JSX.Element;
}

/**
 * A function that maps raw query results to row data
 */
export type TableRowDataGetter<
  R, // RowData
  D, // Raw query data
> = (data: D) => {
  /** Currently loaded rows */
  data: R[];
  /** Total number of rows (some may be not loaded yet) */
  totalCount: number;
};

// ----------------------------------------------------------------------------
// Styling
// ----------------------------------------------------------------------------

/** Inner wrapper (table) */
export const TableContainer: React.FC<
  React.HTMLProps<HTMLDivElement>
> = styled.div`
  flex: 1;
  overflow: hidden;
  border-bottom-left-radius: ${({ theme }) => theme.radiusBorder};
  border-bottom-right-radius: ${({ theme }) => theme.radiusBorder};

  .ReactVirtualized__Table__headerRow {
    display: flex;
    flex-direction: row;
    align-items: center;
  }
  .ReactVirtualized__Table__row {
    display: flex;
    flex-direction: row;
    align-items: center;
    outline: none;
  }
  .ReactVirtualized__Table__row:nth-child(2n + 1) {
    background-color: ${hardcodedStyle.tableStripeBackgroundColor};
  }
  .ReactVirtualized__Table__row.ContractTable__created {
    ${hardcodedStyle.tableRowAppear}
  }
  .ReactVirtualized__Table__row.ContractTable__removed {
    ${hardcodedStyle.tableRowDisappear}
  }
  .ReactVirtualized__Table__row.ContractTable__archived {
    ${hardcodedStyle.tableRowArchived}
  }
  .ReactVirtualized__Table__row:hover {
    cursor: pointer;
    border-left: ${hardcodedStyle.tableHoverBorderWidth} solid
      ${({ theme }) => theme.colorPrimary[0]};
    background-color: ${hardcodedStyle.tableHoverBackgroundColor};
  }
  .ReactVirtualized__Table__row:hover
    .ReactVirtualized__Table__rowColumn:first-of-type {
    margin-left: calc(
      ${hardcodedStyle.tableSideMargin} -
        ${hardcodedStyle.tableHoverBorderWidth}
    );
  }
  .ReactVirtualized__Table__headerTruncatedText {
    display: inline-block;
    max-width: 100%;
    white-space: nowrap;
    text-overflow: ellipsis;
    overflow: hidden;
  }
  .ReactVirtualized__Table__headerColumn,
  .ReactVirtualized__Table__rowColumn {
    margin-right: ${hardcodedStyle.tableCellHorizontalMargin};
    min-width: 0px;
  }
  .ReactVirtualized__Table__rowColumn {
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .ReactVirtualized__Table__headerColumn:first-of-type,
  .ReactVirtualized__Table__rowColumn:first-of-type {
    margin-left: ${hardcodedStyle.tableSideMargin};
  }
  .ReactVirtualized__Table__headerColumn:last-of-type,
  .ReactVirtualized__Table__rowColumn:last-of-type {
    margin-right: ${hardcodedStyle.tableSideMargin};
  }
  .ReactVirtualized__Table__sortableHeaderColumn {
    cursor: pointer;
  }
  .ReactVirtualized__Table__sortableHeaderIconContainer {
    display: flex;
    align-items: center;
  }
  .ReactVirtualized__Table__sortableHeaderIcon {
    flex: 0 0 24px;
    height: 1em;
    width: 1em;
    fill: currentColor;
  }
`;

/** Outer wrapper (action bar + table) */
export const TableOuterWrapper: React.FC<
  React.HTMLProps<HTMLDivElement>
> = styled.div`
  height: 100%;
  width: 100%;
  display: flex;
  flex-direction: column;
  border-radius: ${({ theme }) => theme.radiusBorder};
`;

// ----------------------------------------------------------------------------
// Independent parts
// ----------------------------------------------------------------------------

const Cell = styled.div`
  display: flex;
`;

const CellPadding = styled.div`
  flex: 1;
`;

export function align(
  alignment: CellAlignment,
  content: JSX.Element,
): JSX.Element {
  switch (alignment) {
    case "none":
      return content;
    case "left":
      return content;
    case "right":
      return (
        <Cell>
          <CellPadding />
          {content}
        </Cell>
      );
    case "center":
      return (
        <Cell>
          <CellPadding />
          {content}
          <CellPadding />
        </Cell>
      );
  }
}

/** Creates react-virtualized <Columns> from a ColumnConfig */
export function createColumns<C extends TableConfig, R>(props: {
  readonly config: C;
  readonly columns: ColumnConfig<R, unknown>[];
  onConfigChange?(config: C): void;
}): JSX.Element[] {
  const { columns } = props;
  return columns.map((col: ColumnConfig<R, unknown>, idx: number) => (
    <Column
      key={idx}
      dataKey={col.key}
      width={col.width}
      flexGrow={col.weight}
      headerRenderer={() =>
        align(col.alignment, createHeader<C, R>(col, props))
      }
      cellDataGetter={({ rowData }) => col.extractCellData(rowData)}
      cellRenderer={
        // cellData is not optional because we use a cellDataGetter
        params =>
          align(col.alignment, col.createCell(params as CellRenderParams<C, R>))
      }
      className={"column"}
      headerClassName={"headerColumn"}
    />
  ));
}
