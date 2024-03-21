// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ApolloClient, gql } from "@apollo/client";
import * as DamlLfTypeF from "../api/DamlLfType";
import { DamlLfDefDataType, DamlLfIdentifier } from "../api/DamlLfType";
import {
  ParameterFormContractIdQuery,
  ParameterFormContractIdQueryVariables,
  ParameterFormPartyQuery,
  ParameterFormTypeQuery,
  ParameterFormTypeQueryVariables,
} from "../api/Queries";
import {
  ContractIdProvider,
  ParameterFormContract,
  ParameterFormParty,
  PartyIdProvider,
  TypeProvider,
} from "./";

const MAX_CONTRACTS = 30;

const partyIdQuery = gql`
  query ParameterFormPartyQuery($filter: String!) {
    parties(search: $filter)
  }
`;

const contractIdQuery = gql`
  query ParameterFormContractIdQuery(
    $filter: String!
    $includeArchived: Boolean!
    $count: Int!
    $sort: [SortCriterion!]
  ) {
    contracts(
      search: $filter
      includeArchived: $includeArchived
      count: $count
      sort: $sort
    ) {
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
              transaction {
                effectiveAt
              }
            }
            template {
              id
            }
          }
        }
      }
    }
  }
`;

const typeQuery = gql`
  query ParameterFormTypeQuery($id: ID!) {
    node(typename: "DamlLfDefDataType", id: $id) {
      ... on DamlLfDefDataType {
        dataType
        typeVars
      }
    }
  }
`;

export default class ApolloDataProvider
  implements PartyIdProvider, ContractIdProvider, TypeProvider
{
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  readonly client: ApolloClient<any>;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(client: ApolloClient<any>) {
    this.client = client;
  }

  fetchParties(
    filter: string,
    onResult: (result: ParameterFormParty[]) => void,
  ): void {
    this.client
      .query<ParameterFormPartyQuery>({
        query: partyIdQuery,
        variables: {
          filter,
        },
        fetchPolicy: "network-only",
      })
      .then(({ data }) => {
        onResult(
          data.parties.map(id => {
            return { id };
          }),
        );
      })
      .catch(reason => {
        console.error(reason);
      });
  }

  fetchContracts(
    filter: string,
    onResult: (result: ParameterFormContract[]) => void,
  ): void {
    this.client
      .query<ParameterFormContractIdQuery>({
        query: contractIdQuery,
        variables: {
          filter,
          includeArchived: false,
          count: MAX_CONTRACTS,
          sort: [
            {
              field: "id",
              direction: "ASCENDING",
            },
          ],
        } as ParameterFormContractIdQueryVariables,
        fetchPolicy: "network-only",
      })
      .then(({ data }) => {
        if (data.contracts) {
          onResult(data.contracts.edges.map(e => e.node));
        } else {
          onResult([]);
        }
      })
      .catch(err => {
        console.error("Error fetching contract archiving updates:", err);
      });
  }

  fetchType(
    id: DamlLfIdentifier,
    onResult: (
      id: DamlLfIdentifier,
      result: DamlLfDefDataType | undefined,
    ) => void,
  ): void {
    this.client
      .query<ParameterFormTypeQuery>({
        query: typeQuery,
        variables: {
          id: DamlLfTypeF.opaqueIdentifier(id),
        } as ParameterFormTypeQueryVariables,
        fetchPolicy: "cache-first",
      })
      .then(({ data }) => {
        if (data.node && data.node.__typename === "DamlLfDefDataType") {
          onResult(id, data.node);
        } else {
          onResult(id, undefined);
        }
      })
      .catch(err => {
        console.error("Error fetching contract archiving updates:", err);
        onResult(id, undefined);
      });
  }
}
