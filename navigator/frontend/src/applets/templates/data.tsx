// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { gql, QueryFunctionOptions } from "@apollo/client";
import { QueryControls } from "@apollo/client/react/hoc";
import { DataTableConfig, DataTableProps } from "@da/ui-core";
import { DocumentNode } from "graphql";
import {
  TemplatesQuery,
  TemplatesQuery_templates_edges_node,
  TemplatesQueryVariables,
} from "../../api/Queries";

// GraphQL query
export const query: DocumentNode = gql`
  query TemplatesQuery(
    $filter: [FilterCriterion!]
    $search: String!
    $count: Int!
    $sort: [SortCriterion!]
  ) {
    templates(search: $search, filter: $filter, count: $count, sort: $sort) {
      totalCount
      edges {
        node {
          __typename
          id
          implementedInterfaces
          ... on Template {
            topLevelDecl
            contracts {
              totalCount
            }
          }
        }
      }
    }
  }
`;

// Data returned by graphql (needs to be consistent with the above query)
export type RawData = TemplatesQuery;

// Data stored for a single row in the table
export type Template = TemplatesQuery_templates_edges_node;

// Table config (sort, filter, ...)
export type TableConfig = DataTableConfig;

// Props for our data table
export type TableProps = DataTableProps<TableConfig, Template, RawData>;

// Computing query variables from table config
export function makeQueryVariables({
  config,
}: TableProps): QueryFunctionOptions {
  return {
    variables: {
      search: config.search,
      filter: config.filter,
      count: config.count,
      sort: config.sort,
    } as TemplatesQueryVariables,
    pollInterval: 5000,
  };
}

// Computing row data from raw data
export function dataToRows(data: QueryControls & TemplatesQuery): {
  data: Template[];
  totalCount: number;
} {
  if (data.loading || data.error) {
    return { data: [], totalCount: 0 };
  } else {
    const contracts = data.templates.edges.map(template => template.node);
    const totalCount = data.templates.totalCount;
    return { data: contracts, totalCount };
  }
}
