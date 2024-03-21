import * as OpaqueTypes from "./OpaqueTypes";
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL mutation operation: advanceTime
// ====================================================

export interface advanceTime_advanceTime {
  __typename: "LedgerTime";
  id: string;
  time: OpaqueTypes.Time;
  type: TimeType;
}

export interface advanceTime {
  advanceTime: advanceTime_advanceTime;
}

export interface advanceTimeVariables {
  time: OpaqueTypes.Time;
}

/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: ContractsByIdQuery
// ====================================================

export interface ContractsByIdQuery_nodes_CreateCommand {
  __typename: "CreateCommand" | "CreatedEvent" | "DamlLfDefDataType" | "ExerciseCommand" | "ExercisedEvent" | "Template" | "Transaction";
  id: string;
}

export interface ContractsByIdQuery_nodes_Contract_archiveEvent {
  __typename: "ExercisedEvent";
  id: string;
}

export interface ContractsByIdQuery_nodes_Contract {
  __typename: "Contract";
  id: string;
  archiveEvent: ContractsByIdQuery_nodes_Contract_archiveEvent | null;
}

export type ContractsByIdQuery_nodes = ContractsByIdQuery_nodes_CreateCommand | ContractsByIdQuery_nodes_Contract;

export interface ContractsByIdQuery {
  nodes: ContractsByIdQuery_nodes[];
}

export interface ContractsByIdQueryVariables {
  contractIds: string[];
}

/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: CommandResultsQuery
// ====================================================

export interface CommandResultsQuery_nodes_Contract {
  __typename: "Contract" | "CreatedEvent" | "DamlLfDefDataType" | "ExercisedEvent" | "Template" | "Transaction";
  id: string;
}

export interface CommandResultsQuery_nodes_CreateCommand_status_CommandStatusWaiting {
  __typename: "CommandStatusWaiting" | "CommandStatusSuccess" | "CommandStatusUnknown";
}

export interface CommandResultsQuery_nodes_CreateCommand_status_CommandStatusError {
  __typename: "CommandStatusError";
  code: string;
  details: string;
}

export type CommandResultsQuery_nodes_CreateCommand_status = CommandResultsQuery_nodes_CreateCommand_status_CommandStatusWaiting | CommandResultsQuery_nodes_CreateCommand_status_CommandStatusError;

export interface CommandResultsQuery_nodes_CreateCommand {
  __typename: "CreateCommand" | "ExerciseCommand";
  id: string;
  status: CommandResultsQuery_nodes_CreateCommand_status;
}

export type CommandResultsQuery_nodes = CommandResultsQuery_nodes_Contract | CommandResultsQuery_nodes_CreateCommand;

export interface CommandResultsQuery {
  nodes: CommandResultsQuery_nodes[];
}

export interface CommandResultsQueryVariables {
  commandIds: string[];
}

/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: ParameterFormPartyQuery
// ====================================================

export interface ParameterFormPartyQuery {
  parties: OpaqueTypes.Party[];
}

export interface ParameterFormPartyQueryVariables {
  filter: string;
}

/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: ParameterFormContractIdQuery
// ====================================================

export interface ParameterFormContractIdQuery_contracts_edges_node_createEvent_transaction {
  __typename: "Transaction";
  effectiveAt: OpaqueTypes.Time;
}

export interface ParameterFormContractIdQuery_contracts_edges_node_createEvent {
  __typename: "CreatedEvent";
  id: string;
  transaction: ParameterFormContractIdQuery_contracts_edges_node_createEvent_transaction;
}

export interface ParameterFormContractIdQuery_contracts_edges_node_archiveEvent_transaction {
  __typename: "Transaction";
  effectiveAt: OpaqueTypes.Time;
}

export interface ParameterFormContractIdQuery_contracts_edges_node_archiveEvent {
  __typename: "ExercisedEvent";
  transaction: ParameterFormContractIdQuery_contracts_edges_node_archiveEvent_transaction;
}

export interface ParameterFormContractIdQuery_contracts_edges_node_template {
  __typename: "Template";
  id: string;
}

export interface ParameterFormContractIdQuery_contracts_edges_node {
  __typename: "Contract";
  id: string;
  createEvent: ParameterFormContractIdQuery_contracts_edges_node_createEvent;
  archiveEvent: ParameterFormContractIdQuery_contracts_edges_node_archiveEvent | null;
  template: ParameterFormContractIdQuery_contracts_edges_node_template;
}

export interface ParameterFormContractIdQuery_contracts_edges {
  __typename: "ContractEdge";
  node: ParameterFormContractIdQuery_contracts_edges_node;
}

export interface ParameterFormContractIdQuery_contracts {
  __typename: "ContractPagination";
  totalCount: number;
  edges: ParameterFormContractIdQuery_contracts_edges[];
}

export interface ParameterFormContractIdQuery {
  contracts: ParameterFormContractIdQuery_contracts;
}

export interface ParameterFormContractIdQueryVariables {
  filter: string;
  includeArchived: boolean;
  count: number;
  sort?: SortCriterion[] | null;
}

/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: ParameterFormTypeQuery
// ====================================================

export interface ParameterFormTypeQuery_node_Contract {
  __typename: "Contract" | "CreateCommand" | "CreatedEvent" | "ExerciseCommand" | "ExercisedEvent" | "Template" | "Transaction";
}

export interface ParameterFormTypeQuery_node_DamlLfDefDataType {
  __typename: "DamlLfDefDataType";
  dataType: OpaqueTypes.DamlLfDataType;
  typeVars: string[];
}

export type ParameterFormTypeQuery_node = ParameterFormTypeQuery_node_Contract | ParameterFormTypeQuery_node_DamlLfDefDataType;

export interface ParameterFormTypeQuery {
  node: ParameterFormTypeQuery_node | null;
}

export interface ParameterFormTypeQueryVariables {
  id: string;
}

/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: LedgerTimeQuery
// ====================================================

export interface LedgerTimeQuery_ledgerTime {
  __typename: "LedgerTime";
  id: string;
  time: OpaqueTypes.Time;
  type: TimeType;
}

export interface LedgerTimeQuery {
  ledgerTime: LedgerTimeQuery_ledgerTime;
}

/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

//==============================================================
// START Enums and Input Objects
//==============================================================

export enum Direction {
  ASCENDING = "ASCENDING",
  DESCENDING = "DESCENDING",
}

export enum TimeType {
  simulated = "simulated",
  static = "static",
  wallclock = "wallclock",
}

export interface SortCriterion {
  field: string;
  direction: Direction;
}

//==============================================================
// END Enums and Input Objects
//==============================================================
