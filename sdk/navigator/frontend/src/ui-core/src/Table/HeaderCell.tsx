// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import Icon from "../Icon";
import { StyledLabelText } from "../Label";
import styled from "../theme";
import {
  ColumnConfig,
  flipSortDirection,
  SortCriterion,
  SortDirection,
  TableConfig,
} from "./";

export interface HeaderCellProps {
  sortKey: string;
  sortable: boolean;
  sort: SortCriterion | undefined;
  onSortChange(field: string, direction: SortDirection): void;
}

const Cell = styled(StyledLabelText)``;

const SortableCell = styled(Cell)`
  cursor: pointer;
  user-select: none;
`;

const StyledIcon = styled(Icon)`
  margin-left: 0.625rem;
`;

const SortIcon = ({ sort }: { sort?: SortCriterion }) => {
  if (sort && sort.direction === "ASCENDING") {
    return <StyledIcon name="sort-asc" />;
  } else if (sort && sort.direction === "DESCENDING") {
    return <StyledIcon name="sort-desc" />;
  } else {
    return <span />;
  }
};
export default class HeaderCell extends React.Component<HeaderCellProps, {}> {
  constructor(props: HeaderCellProps) {
    super(props);
    this.sort = this.sort.bind(this);
  }

  render(): React.ReactElement<HTMLDivElement> {
    if (!this.props.sortable) {
      return (
        <Cell>
          {this.props.children}
          <SortIcon sort={this.props.sort} />
        </Cell>
      );
    }
    return (
      <SortableCell onClick={this.sort}>
        {this.props.children}
        <SortIcon sort={this.props.sort} />
      </SortableCell>
    );
  }

  sort(e: React.MouseEvent<HTMLDivElement>): void {
    e.preventDefault();
    if (this.props.onSortChange) {
      if (this.props.sort) {
        const dir = flipSortDirection(this.props.sort.direction);
        this.props.onSortChange(this.props.sortKey, dir);
      } else {
        this.props.onSortChange(this.props.sortKey, "ASCENDING");
      }
    }
  }
}

export function createHeader<C extends TableConfig, R>(
  col: ColumnConfig<R, unknown>,
  props: {
    readonly config: C;
    onConfigChange?(config: C): void;
  },
): JSX.Element {
  const { sort } = props.config;
  const colSort = sort.filter(c => c.field === col.key)[0];
  const onSortChanged = (b: string, d: SortDirection) => {
    if (props.onConfigChange) {
      props.onConfigChange({
        ...props.config,
        sort: [
          {
            field: b,
            direction: d,
          },
        ],
      });
    }
  };
  return (
    <HeaderCell
      sortable={col.sortable}
      sortKey={col.key}
      sort={colSort}
      onSortChange={onSortChanged}>
      {col.title}
    </HeaderCell>
  );
}
