// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { gql } from "@apollo/client";
import { QueryControls } from "@apollo/client/react/hoc";
import { DocumentNode } from "graphql";
import { State, TableConfig } from ".";
import {
  ContractsByTemplateParamQuery,
  ContractsByTemplateParamQueryVariables,
  ContractsByTemplateQuery,
  ContractsByTemplateQuery_node_Template_contracts_edges_node,
  ContractsByTemplateQueryVariables,
  SortCriterion,
} from "../../api/Queries";

export type ParamQueryData = ContractsByTemplateParamQuery;
export type Template = ContractsByTemplateParamQuery;
export type Contract =
  ContractsByTemplateQuery_node_Template_contracts_edges_node;

export const paramQuery: DocumentNode = gql`
  query ContractsByTemplateParamQuery($templateId: ID!) {
    node(typename: "Template", id: $templateId) {
      ... on Template {
        id
        parameterDef {
          dataType
        }
      }
    }
  }
`;

export const query: DocumentNode = gql`
  query ContractsByTemplateQuery(
    $templateId: ID!
    $filter: [FilterCriterion!]
    $search: String!
    $count: Int!
    $sort: [SortCriterion!]
    $includeArchived: Boolean!
  ) {
    node(typename: "Template", id: $templateId) {
      ... on Template {
        id
        choices {
          name
          inheritedInterface
        }
        contracts(
          search: $search
          filter: $filter
          includeArchived: $includeArchived
          count: $count
          sort: $sort
        ) {
          totalCount
          edges {
            node {
              ... on Contract {
                id
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
                  choices {
                    name
                    inheritedInterface
                  }
                }
              }
            }
          }
        }
      }
    }
  }
`;

export function makeQueryVariables(
  config: TableConfig,
): ContractsByTemplateQueryVariables {
  return {
    search: config.search,
    filter: config.filter,
    includeArchived: config.includeArchived,
    count: config.count,
    sort: config.sort as SortCriterion[],
    templateId: config.id,
  };
}

export function makeParamQueryVariables<P extends { state: State }>({
  state,
}: P): { variables: ContractsByTemplateParamQueryVariables } {
  return {
    variables: {
      templateId: state.id,
    },
  };
}

export function dataToRows(data: QueryControls & ContractsByTemplateQuery): {
  contracts: Contract[];
  totalCount: number;
} {
  if (data.loading || data.error) {
    return { contracts: [], totalCount: 0 };
  } else if (data.node && data.node.__typename === "Template") {
    const contracts = data.node.contracts.edges.map(edge => edge.node);
    const totalCount = data.node.contracts.totalCount;
    return { contracts, totalCount };
  } else {
    return { contracts: [], totalCount: 0 };
  }
}
