// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  ContractTableConfig,
} from '@da/ui-core';
import { DocumentNode } from 'graphql';
import { gql, QueryProps } from 'react-apollo';
import {
  ContractsQuery,
  ContractsQuery_contracts_edges_node,
  ContractsQueryVariables,
  SortCriterion,
} from '../../api/Queries';

export type Contract = ContractsQuery_contracts_edges_node;

export const query: DocumentNode = gql`
query ContractsQuery($filter: [FilterCriterion!], $search: String!,
  $includeArchived: Boolean!, $count: Int!, $sort: [SortCriterion!]) {
  contracts(
    filter: $filter,
    search: $search,
    includeArchived: $includeArchived,
    count: $count,
    sort: $sort) {
    totalCount
    edges {
      node {
        __typename
        id
        ... on Contract {
          createEvent {
            id
            transaction {
              effectiveAt
            }
          }
          archiveEvent {
              id
          }
          argument
          template {
            id
            choices { name }
          }
        }
      }
    }
  }
}
`;

export function makeQueryVariables(config: ContractTableConfig): ContractsQueryVariables {
  return {
    search: config.search,
    filter: config.filter,
    includeArchived: config.includeArchived,
    count: config.count,
    // Type cast because apollo codegen generates enums
    sort: config.sort as SortCriterion[],
  };
}

export function dataToRows(data: QueryProps & ContractsQuery) {
  if (data.loading || data.error) {
    return { contracts: [], totalCount: 0 }
  } else {
    const contracts = data.contracts.edges.map((edge) => edge.node);
    const totalCount = data.contracts.totalCount;
    return { contracts, totalCount };
  }
}
