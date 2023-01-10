// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ApolloClient } from "@apollo/client";
import gql from "graphql-tag";
import {
  CommandResultsQuery,
  CommandResultsQuery_nodes,
  CommandResultsQuery_nodes_CreateCommand_status,
  CommandResultsQueryVariables,
  ContractsByIdQuery,
  ContractsByIdQueryVariables,
} from "../api/Queries";
export { UI } from "./UI";

const CONTRACTS_POLL_INTERVAL = 5000;
const COMMANDS_POLL_INTERVAL = 1000;

// Time in milliseconds to keep command data after the result was received.
export const MAX_COMMAND_AGE = 5000;

const contractsQuery = gql`
  query ContractsByIdQuery($contractIds: [ID!]!) {
    nodes(typename: "Contract", ids: $contractIds) {
      __typename
      id
      ... on Contract {
        archiveEvent {
          __typename
          id
        }
      }
    }
  }
`;

const commandResultsQuery = gql`
  query CommandResultsQuery($commandIds: [ID!]!) {
    nodes(typename: "Command", ids: $commandIds) {
      __typename
      id
      ... on Command {
        status {
          __typename
          ... on CommandStatusError {
            code
            details
          }
        }
      }
    }
  }
`;

export type ResultType =
  | { type: "SUCCESS"; processedAt: Date }
  | { type: "ERROR"; message: string; processedAt: Date };

export interface WatchedCommand {
  commandId: string;
  result?: ResultType;
}

export interface State {
  contracts: WatchedContracts;
  commands: WatchedCommand[];
}

export type Action =
  | {
      type: "REGISTER_CONTRACTS";
      componentId: string;
      contractIds: string[];
    }
  | {
      type: "UNREGISTER_CONTRACTS";
      componentId: string;
    }
  | {
      type: "SET_COMMAND_RESULT";
      commandData?: WatchedCommand;
    }
  | {
      type: "REGISTER_COMMAND";
      commandId: string;
    };

export const register = (
  componentId: string,
  contractIds: string[],
): Action => ({
  type: "REGISTER_CONTRACTS",
  componentId,
  contractIds,
});

export const unregister = (componentId: string): Action => ({
  type: "UNREGISTER_CONTRACTS",
  componentId,
});

export const setCommandResult = (commandData?: WatchedCommand): Action => ({
  type: "SET_COMMAND_RESULT",
  commandData,
});

export const registerCommand = (commandId: string): Action => ({
  type: "REGISTER_COMMAND",
  commandId,
});

/**
 * NOTE(cbaatz): This should be changed to a pure object that has the component
 * ID keys at the top level. It's not great practice to use classes in Redux
 * state and this produces to levels of `contracts` keys in the state which
 * isn't all that nice.
 */
export class WatchedContracts {
  contracts: { [index: string]: string[] };

  constructor(prev?: WatchedContracts) {
    this.addContractIds = this.addContractIds.bind(this);
    this.removeContractIds = this.removeContractIds.bind(this);
    this.getDistinctContractIds = this.getDistinctContractIds.bind(this);
    this.contracts = {};
    if (prev) {
      for (const componentId in prev.contracts) {
        this.contracts[componentId] = prev.contracts[componentId];
      }
    }
  }

  addContractIds(componentId: string, contractIds: string[]): WatchedContracts {
    this.contracts[componentId] = contractIds;
    return this;
  }

  removeContractIds(componentId: string): WatchedContracts {
    delete this.contracts[componentId];
    return this;
  }

  getDistinctContractIds(): string[] {
    const set: { [index: string]: string } = {};
    const val = "";
    for (const componentId in this.contracts) {
      const contractIds = this.contracts[componentId];
      for (const contractId of contractIds) {
        set[contractId] = val;
      }
    }
    const distinctContractIds: string[] = [];
    for (const contractId in set) {
      distinctContractIds.push(contractId);
    }
    return distinctContractIds;
  }
}

// Reducer function
export function reduce(prev?: State, action?: Action): State {
  if (prev === undefined) {
    return {
      contracts: new WatchedContracts(),
      commands: [],
    };
  }
  if (action === undefined) {
    return prev;
  }
  return {
    contracts: watchedContracts(prev.contracts, action),
    commands: watchedCommands(prev.commands, action),
  };
}

function watchedContracts(
  prev: WatchedContracts,
  action: Action,
): WatchedContracts {
  switch (action.type) {
    case "REGISTER_CONTRACTS":
      return new WatchedContracts(prev).addContractIds(
        action.componentId,
        action.contractIds,
      );
    case "UNREGISTER_CONTRACTS":
      return new WatchedContracts(prev).removeContractIds(action.componentId);
  }
  return prev;
}

function watchedCommands(
  prev: WatchedCommand[],
  action: Action,
): WatchedCommand[] {
  switch (action.type) {
    case "REGISTER_COMMAND": {
      const result = processCommands(prev);
      result.push({ commandId: action.commandId });
      return result;
    }
    case "SET_COMMAND_RESULT": {
      const result = processCommands(prev);
      if (action.commandData) {
        for (let i = 0; i < result.length; i++) {
          if (result[i].commandId === action.commandData.commandId) {
            result[i] = action.commandData;
            break;
          }
        }
      }
      return result;
    }
  }
  return prev;
}

/**
 * Creates a new array and adds all WatchedComands from the previous state
 * that have not expired.
 */
function processCommands(prev?: WatchedCommand[]): WatchedCommand[] {
  const result: WatchedCommand[] = [];
  if (prev) {
    const now = new Date().getTime();
    for (const cmd of prev) {
      if (
        !cmd.result ||
        now - cmd.result.processedAt.getTime() <= MAX_COMMAND_AGE
      ) {
        result.push(cmd);
      }
    }
  }
  return result;
}

class Watcher {
  contractsTimer: number;
  commandsTimer: number;
  constructor(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    private client: ApolloClient<any>,
    private getWatcherState: () => State,
    private dispatch: (action: Action) => void,
  ) {
    this.start = this.start.bind(this);
    this.stop = this.stop.bind(this);
    this.processContracts = this.processContracts.bind(this);
    this.processCommands = this.processCommands.bind(this);
  }

  start(): void {
    this.contractsTimer = window.setTimeout(
      this.processContracts,
      CONTRACTS_POLL_INTERVAL,
    );
    this.commandsTimer = window.setTimeout(
      this.processCommands,
      COMMANDS_POLL_INTERVAL,
    );
  }

  stop(): void {
    clearTimeout(this.contractsTimer);
    clearTimeout(this.commandsTimer);
  }

  processContracts(): void {
    const contractIds =
      this.getWatcherState().contracts.getDistinctContractIds();
    if (contractIds.length > 0) {
      // Run a query and date the client cache with the data manually.
      this.client
        .query<ContractsByIdQuery>({
          query: contractsQuery,
          variables: { contractIds } as ContractsByIdQueryVariables,
          fetchPolicy: "network-only",
        })
        .catch(err => {
          console.error("Error fetching contract archiving updates:", err);
        });
    }
    this.contractsTimer = window.setTimeout(
      this.processContracts,
      CONTRACTS_POLL_INTERVAL,
    );
  }

  processCommands(): void {
    const { commands } = this.getWatcherState();
    if (commands) {
      const commandIds: string[] = [];
      for (const cmd of commands) {
        if (!cmd.result) {
          commandIds.push(cmd.commandId);
        }
      }
      if (commands.length > 0) {
        // Remove expired commands in the reducer function.
        this.dispatch(setCommandResult());
      }
      if (commandIds.length > 0) {
        this.client
          .query<CommandResultsQuery>({
            query: commandResultsQuery,
            variables: { commandIds } as CommandResultsQueryVariables,
            fetchPolicy: "network-only",
          })
          .then(({ data, loading }) => {
            if (!loading && data) {
              data.nodes
                .map(parseCommand)
                .filter(r => r.result !== undefined)
                .map(setCommandResult)
                .forEach(this.dispatch);
            }
          })
          .catch(err => {
            console.error("Error fetching command status:", err);
          });
      }
    }
    this.commandsTimer = window.setTimeout(
      this.processCommands,
      COMMANDS_POLL_INTERVAL,
    );
  }
}

function parseCommand(node: CommandResultsQuery_nodes): WatchedCommand {
  if (
    node.__typename === "CreateCommand" ||
    node.__typename === "ExerciseCommand"
  ) {
    return { commandId: node.id, result: parseCommandResult(node.status) };
  } else {
    return { commandId: node.id, result: undefined };
  }
}

function parseCommandResult(
  status: CommandResultsQuery_nodes_CreateCommand_status,
): ResultType | undefined {
  const processedAt = new Date();
  switch (status.__typename) {
    case "CommandStatusSuccess":
      return { type: "SUCCESS", processedAt };
    case "CommandStatusError":
      return {
        type: "ERROR",
        message: `${status.code}: ${status.details}`,
        processedAt,
      };
    default:
      return undefined;
  }
}

export default Watcher;
