// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import cx from "classnames";
import deepEqual from "deep-equal";
import * as React from "react";
import { AutoSizer, Table } from "react-virtualized";
import * as UUID from "uuidjs";
import {
  ColumnConfig,
  createColumns,
  TableConfig,
  TableContainer,
  TableOuterWrapper,
} from "../Table";
import {
  TableActionBar,
  TableActionBarConfigCheckbox,
  TableActionBarConfigSearchInput,
  TableActionBarSideMargin,
  TableActionBarTitle,
} from "../TableActionBar";
import { hardcodedStyle } from "../theme";

const DEFAULT_POLL_INTERVAL = 5000;
const AGE_THREASHOLD = 3000;

export type ResultCallback = (data: ContractsResult) => void;

export interface DataProvider<C extends TableConfig> {
  fetchData(config: C, onResult: ResultCallback): void;
  startCacheWatcher(config: C, onResult: ResultCallback): void;
  stopCacheWatcher(): void;
}

export interface RowData {
  id: string;
  archiveEvent: {} | null;
}

export interface RowInfo {
  addedAt?: Date;
  removedAt?: Date;
}

// eslint-disable-next-line  @typescript-eslint/no-explicit-any
export type ContractColumn<T, R = any> = ColumnConfig<T, R>;

interface RowClassProps<C> {
  index: number;
  info: RowInfo;
  config: C;
  rowClassName: string;
  headerRowClassName: string;
  removedRowClassName: string;
  createdRowClassName: string;
  archivedRowClassName: string;
}

function rowClasses<C extends ContractTableConfig>(
  props: RowClassProps<C>,
): string {
  const { config, index, info } = props;
  if (index < 0) {
    return props.headerRowClassName;
  } else {
    const now = new Date().getTime();
    return cx(props.rowClassName, {
      [props.archivedRowClassName]:
        (config.isFrozen || config.includeArchived) &&
        info.removedAt !== undefined,
      [props.createdRowClassName]:
        !config.isFrozen &&
        info.addedAt &&
        now - info.addedAt.getTime() < AGE_THREASHOLD,
      [props.removedRowClassName]:
        !config.isFrozen &&
        !config.includeArchived &&
        info.removedAt &&
        now - info.removedAt.getTime() < AGE_THREASHOLD,
    });
  }
}

export interface ContractTableConfig extends TableConfig {
  includeArchived: boolean;
  isFrozen: boolean;
}

export interface ContractsResult {
  contracts: RowData[];
  totalCount: number;
}

export interface Props<C extends ContractTableConfig> {
  dataProvider: DataProvider<C>;
  config: C;
  columns: ContractColumn<RowData>[];
  title?: string;
  className?: string;
  columnClassName?: string;
  rowClassName?: string;
  headerRowClassName?: string;
  headerColumnClassName?: string;
  removedRowClassName?: string;
  createdRowClassName?: string;
  archivedRowClassName?: string;
  hideActionRow?: boolean;
  actionRowContent?: React.ReactNode;
  onContractClick?(contract: RowData): void;
  onConfigChange?(config: C): void;
  onRegisterContracts?(componentId: string, contractIds: string[]): void;
  onUnregisterContracts?(componentId: string): void;
}

export interface State {
  isLoading: boolean;
}

/**
 * Contract table.
 *
 * Updating mode:
 *   The table polls the server every n seconds, updates the rows and highlights
 *   new and soon to be removed rows with different CSS styles. After a timeout
 *   the new rows are displayed without the highlighting style and the old rows
 *   are removed.
 *   When the sort order or the filter is changed, all data is reloaded and
 *   possible highlighting styles are removed.
 *
 * Frozen mode:
 *   The current result is kept until the sort order or the filter is
 *   changed. No polling occurs. However the table registers the contracts with
 *   the contract watcher and subscribes to cache updates that affect the
 *   current query. If a contract has been archived since the data was fetched,
 *   the row is greyed out.
 */
export default class ContractTable<
  C extends ContractTableConfig,
> extends React.Component<Props<C>, State> {
  readonly componentId: string = UUID.generate();

  readonly headerHeight: number = hardcodedStyle.tableHeaderHeight;
  readonly rowHeight: number = hardcodedStyle.tableRowHeight;
  readonly fetchIncrement: number = 100;
  contracts: RowData[] = [];
  totalCount: number = 0;
  /**
   * Map (row ID -> RowInfo) that contains additional data for table rows.
   * When the table data is reloaded this map is cleared.
   * When a row is removed from the table the corresponding entry is removed
   * from this map to ensure is does not grow infinitely.
   */
  idToInfo: { [index: string]: RowInfo } = {};
  table: Table;
  reloadTimer?: number;
  rerenderTimer?: number;

  constructor(props: Props<C>) {
    super(props);
    this.state = { isLoading: true };
    this.reload = this.reload.bind(this);
    this.updateContracts = this.updateContracts.bind(this);
    this.registerContracts = this.registerContracts.bind(this);
    this.unregisterContracts = this.unregisterContracts.bind(this);
    this.scheduleReload = this.scheduleReload.bind(this);
  }

  componentDidMount(): void {
    this.reload(true);
  }

  componentWillUnmount(): void {
    this.props.dataProvider.stopCacheWatcher();
    this.unregisterContracts();
    if (this.reloadTimer) {
      clearTimeout(this.reloadTimer);
    }
    if (this.rerenderTimer) {
      clearTimeout(this.rerenderTimer);
    }
  }

  componentDidUpdate(prevProps: Props<C>): void {
    const { config, dataProvider } = this.props;
    if (!deepEqual(prevProps.config, config)) {
      if (config.isFrozen) {
        // If view is frozen, unschedule any reloads and don't schedule new ones
        if (this.reloadTimer) {
          clearTimeout(this.reloadTimer);
        }
        dataProvider.startCacheWatcher(config, contractsResult => {
          this.contracts = contractsResult.contracts;
          const now = new Date();
          for (const c of this.contracts) {
            if (c.archiveEvent !== null) {
              const info = this.getRowInfo(c);
              info.removedAt = info.removedAt || now;
            }
          }
          this.totalCount = contractsResult.totalCount;
          this.forceUpdate();
        });
      } else {
        dataProvider.stopCacheWatcher();
      }
      this.reload(true);
    }
  }

  /**
   * Reloads the data from the server.
   * flush==false: update data, keep metadata.
   * flush==true: replace data, flush metadata.
   */
  reload(flush: boolean): void {
    const { dataProvider, config } = this.props;
    this.setState({ isLoading: true });
    dataProvider.fetchData(config, contractsResult => {
      if (flush) {
        this.idToInfo = {};
        this.contracts = contractsResult.contracts;
      }
      this.totalCount = contractsResult.totalCount;
      this.updateContracts(contractsResult.contracts);
      this.setState({ isLoading: false });
      if (this.props.config.isFrozen) {
        this.registerContracts();
      } else {
        this.scheduleReload();
        this.scheduleRerender();
      }
    });
  }

  /**
   * Merges the current list of contracts with the list that was just loaded
   * from the server and updates the metadata.
   */
  updateContracts(newContracts: RowData[]): void {
    const updatedContracts: RowData[] = [];
    const now = new Date();
    // Maps ID to list index for the current rows
    const index: { [index: string]: number } = {};
    for (let i = 0; i < this.contracts.length; i++) {
      index[this.contracts[i].id] = i;
    }
    let last = -1;
    // Go through the new rows:
    // If it does not match on of the current rows, continue.
    // If is does match, insert all rows of the current data between this and
    // the previous match.
    // Then insert the new row.
    for (const c of newContracts) {
      if (index[c.id] === undefined) {
        this.getRowInfo(c).addedAt = now;
      } else {
        for (let i = last + 1; i < index[c.id]; i++) {
          const info = this.getRowInfo(this.contracts[i]);
          if (
            info.removedAt &&
            now.getTime() - info.removedAt.getTime() > AGE_THREASHOLD
          ) {
            delete this.idToInfo[this.contracts[i].id];
          } else {
            updatedContracts.push(this.contracts[i]);
            info.removedAt = info.removedAt || now;
          }
        }
        last = index[c.id];
      }
      updatedContracts.push(c);
      if (c.archiveEvent !== null) {
        const info = this.getRowInfo(c);
        info.removedAt = info.removedAt || now;
      }
    }
    // Now insert all remaining rows after the last match.
    for (let i = last + 1; i < this.contracts.length; i++) {
      const info = this.getRowInfo(this.contracts[i]);
      if (
        info.removedAt &&
        now.getTime() - info.removedAt.getTime() > AGE_THREASHOLD
      ) {
        delete this.idToInfo[this.contracts[i].id];
      } else {
        updatedContracts.push(this.contracts[i]);
        info.removedAt = info.removedAt || now;
      }
    }
    this.contracts = updatedContracts;
  }

  getRowInfo(c: RowData): RowInfo {
    if (!c) {
      return {};
    }
    let info: RowInfo = this.idToInfo[c.id];
    if (!info) {
      info = {};
      this.idToInfo[c.id] = info;
    }
    return info;
  }

  registerContracts(): void {
    if (this.props.config.isFrozen) {
      const contractIds = this.contracts
        .filter(c => this.getRowInfo(c).removedAt === undefined)
        .map(({ id }) => id);
      if (this.props.onRegisterContracts) {
        this.props.onRegisterContracts(this.componentId, contractIds);
      }
    } else {
      this.unregisterContracts();
    }
  }

  unregisterContracts(): void {
    if (this.props.onUnregisterContracts) {
      this.props.onUnregisterContracts(this.componentId);
    }
  }

  /**
   * Poll query every n seconds.
   */
  scheduleReload(): void {
    if (!this.state.isLoading) {
      // If we're not loading, schedule reload after clearing existing one.
      if (this.reloadTimer) {
        clearTimeout(this.reloadTimer);
      }
      this.reloadTimer = window.setTimeout(() => {
        this.reload(false);
      }, DEFAULT_POLL_INTERVAL);
    }
  }

  /**
   * Find the row that has to be updated first and set a timeout. When the
   * timeout expires, rerender the table and do the same again.
   */
  scheduleRerender(): void {
    if (this.rerenderTimer) {
      clearTimeout(this.rerenderTimer);
    }
    let oldest = -1;
    const now = new Date().getTime();
    for (const c of this.contracts) {
      const info = this.getRowInfo(c);
      let age;
      if (info.removedAt && info.addedAt) {
        age = now - Math.min(info.removedAt.getTime(), info.addedAt.getTime());
      } else if (info.removedAt) {
        age = now - info.removedAt.getTime();
      } else if (info.addedAt) {
        age = now - info.addedAt.getTime();
      }
      if (age && age < AGE_THREASHOLD && age > oldest) {
        oldest = age;
      }
    }
    if (oldest >= 0) {
      this.rerenderTimer = window.setTimeout(() => {
        this.rerenderTimer = undefined;
        this.table.recomputeRowHeights(0);
        this.forceUpdate();
        this.scheduleRerender();
      }, AGE_THREASHOLD - oldest);
    }
  }

  onScroll(height: number, y: number): void {
    const count = this.contracts.length;
    if (y > count * this.rowHeight - height && count < this.totalCount) {
      const newCount = Math.min(count + this.fetchIncrement, this.totalCount);
      if (this.props.onConfigChange) {
        this.props.onConfigChange({
          ...this.props.config,
          count: newCount,
        });
      }
    }
  }

  render(): React.ReactElement<HTMLDivElement> {
    const {
      title,
      className,
      hideActionRow = false,
      actionRowContent,
    } = this.props;
    const rowClassProps = (index: number) => ({
      index,
      info: this.getRowInfo(this.contracts[index]),
      config: this.props.config,
      rowClassName: this.props.rowClassName || "",
      headerRowClassName: this.props.headerRowClassName || "",
      removedRowClassName: this.props.removedRowClassName || "",
      createdRowClassName: this.props.createdRowClassName || "",
      archivedRowClassName: this.props.archivedRowClassName || "",
    });
    let actionBarEl;
    if (!hideActionRow) {
      actionBarEl =
        actionRowContent !== undefined ? (
          <TableActionBar>{actionRowContent}</TableActionBar>
        ) : (
          <TableActionBar>
            <TableActionBarSideMargin />
            <TableActionBarTitle>{title}</TableActionBarTitle>
            <TableActionBarConfigCheckbox
              title="Include archived"
              configKey="includeArchived"
              config={this.props.config}
              onConfigChange={this.props.onConfigChange}
            />
            <TableActionBarConfigCheckbox
              title="Frozen"
              configKey="isFrozen"
              config={this.props.config}
              onConfigChange={this.props.onConfigChange}
            />
            <TableActionBarConfigSearchInput
              placeholder="Search"
              config={this.props.config}
              onConfigChange={this.props.onConfigChange}
            />
          </TableActionBar>
        );
    }

    return (
      <TableOuterWrapper className={className}>
        {actionBarEl}
        <TableContainer>
          <AutoSizer>
            {({ width, height }: { width: number; height: number }) => (
              <Table
                ref={table => {
                  if (table) {
                    this.table = table;
                  }
                }}
                width={width}
                height={height}
                headerHeight={this.headerHeight}
                rowHeight={({ index }) => {
                  const c = this.contracts[index];
                  const info = this.getRowInfo(c);
                  const now = new Date().getTime();
                  return !this.props.config.isFrozen &&
                    !this.props.config.includeArchived &&
                    info.removedAt &&
                    now - info.removedAt.getTime() > AGE_THREASHOLD
                    ? 0
                    : this.rowHeight;
                }}
                rowCount={this.contracts.length}
                onRowClick={({ index }) => {
                  if (this.props.onContractClick) {
                    this.props.onContractClick(this.contracts[index]);
                  }
                }}
                rowGetter={({ index }) => this.contracts[index]}
                rowClassName={({ index }) => rowClasses(rowClassProps(index))}
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
