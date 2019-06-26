import * as OpaqueTypes from '@da/ui-core/lib/api/OpaqueTypes'
/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: ContractDetailsById
// ====================================================

export interface ContractDetailsById_node_Transaction {
  __typename: "Transaction" | "Template" | "DamlLfDefDataType" | "CreatedEvent" | "ExercisedEvent" | "CreateCommand" | "ExerciseCommand";
}

export interface ContractDetailsById_node_Contract_archiveEvent {
  __typename: "ExercisedEvent";
  id: string;
}

export interface ContractDetailsById_node_Contract_template_choices {
  __typename: "Choice";
  name: string;
  parameter: OpaqueTypes.DamlLfType;
}

export interface ContractDetailsById_node_Contract_template {
  __typename: "Template";
  id: string;
  topLevelDecl: string;
  choices: ContractDetailsById_node_Contract_template_choices[];
}

export interface ContractDetailsById_node_Contract {
  __typename: "Contract";
  id: string;
  argument: OpaqueTypes.DamlLfValueRecord;
  archiveEvent: ContractDetailsById_node_Contract_archiveEvent | null;
  agreementText: string | null;
  signatories: string[];
  observers: string[];
  template: ContractDetailsById_node_Contract_template;
}

export type ContractDetailsById_node = ContractDetailsById_node_Transaction | ContractDetailsById_node_Contract;

export interface ContractDetailsById {
  node: ContractDetailsById_node | null;
}

export interface ContractDetailsByIdVariables {
  id: string;
}

/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL mutation operation: ContractExercise
// ====================================================

export interface ContractExercise {
  exercise: OpaqueTypes.CommandId;
}

export interface ContractExerciseVariables {
  contractId: string;
  choiceId: string;
  argument?: OpaqueTypes.DamlLfValue | null;
}

/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: ContractsQuery
// ====================================================

export interface ContractsQuery_contracts_edges_node_createEvent_transaction {
  __typename: "Transaction";
  effectiveAt: OpaqueTypes.Time;
}

export interface ContractsQuery_contracts_edges_node_createEvent {
  __typename: "CreatedEvent";
  id: string;
  transaction: ContractsQuery_contracts_edges_node_createEvent_transaction;
}

export interface ContractsQuery_contracts_edges_node_archiveEvent {
  __typename: "ExercisedEvent";
  id: string;
}

export interface ContractsQuery_contracts_edges_node_template_choices {
  __typename: "Choice";
  name: string;
}

export interface ContractsQuery_contracts_edges_node_template {
  __typename: "Template";
  id: string;
  choices: ContractsQuery_contracts_edges_node_template_choices[];
}

export interface ContractsQuery_contracts_edges_node {
  __typename: "Contract";
  id: string;
  createEvent: ContractsQuery_contracts_edges_node_createEvent;
  archiveEvent: ContractsQuery_contracts_edges_node_archiveEvent | null;
  argument: OpaqueTypes.DamlLfValueRecord;
  template: ContractsQuery_contracts_edges_node_template;
}

export interface ContractsQuery_contracts_edges {
  __typename: "ContractEdge";
  node: ContractsQuery_contracts_edges_node;
}

export interface ContractsQuery_contracts {
  __typename: "ContractPagination";
  totalCount: number;
  edges: ContractsQuery_contracts_edges[];
}

export interface ContractsQuery {
  contracts: ContractsQuery_contracts;
}

export interface ContractsQueryVariables {
  filter?: FilterCriterion[] | null;
  search: string;
  includeArchived: boolean;
  count: number;
  sort?: SortCriterion[] | null;
}

/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: TemplateInstance
// ====================================================

export interface TemplateInstance_node_Transaction {
  __typename: "Transaction" | "DamlLfDefDataType" | "Contract" | "CreatedEvent" | "ExercisedEvent" | "CreateCommand" | "ExerciseCommand";
}

export interface TemplateInstance_node_Template {
  __typename: "Template";
  id: string;
  parameter: OpaqueTypes.DamlLfType;
  topLevelDecl: string;
}

export type TemplateInstance_node = TemplateInstance_node_Transaction | TemplateInstance_node_Template;

export interface TemplateInstance {
  node: TemplateInstance_node | null;
}

export interface TemplateInstanceVariables {
  templateId: string;
}

/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL mutation operation: CreateContract
// ====================================================

export interface CreateContract {
  create: OpaqueTypes.CommandId;
}

export interface CreateContractVariables {
  templateId: string;
  argument?: OpaqueTypes.DamlLfValue | null;
}

/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: ContractsByTemplateParamQuery
// ====================================================

export interface ContractsByTemplateParamQuery_node_Transaction {
  __typename: "Transaction" | "DamlLfDefDataType" | "Contract" | "CreatedEvent" | "ExercisedEvent" | "CreateCommand" | "ExerciseCommand";
}

export interface ContractsByTemplateParamQuery_node_Template_parameterDef {
  __typename: "DamlLfDefDataType";
  dataType: OpaqueTypes.DamlLfDataType;
}

export interface ContractsByTemplateParamQuery_node_Template {
  __typename: "Template";
  id: string;
  parameterDef: ContractsByTemplateParamQuery_node_Template_parameterDef;
}

export type ContractsByTemplateParamQuery_node = ContractsByTemplateParamQuery_node_Transaction | ContractsByTemplateParamQuery_node_Template;

export interface ContractsByTemplateParamQuery {
  node: ContractsByTemplateParamQuery_node | null;
}

export interface ContractsByTemplateParamQueryVariables {
  templateId: string;
}

/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: ContractsByTemplateQuery
// ====================================================

export interface ContractsByTemplateQuery_node_Transaction {
  __typename: "Transaction" | "DamlLfDefDataType" | "Contract" | "CreatedEvent" | "ExercisedEvent" | "CreateCommand" | "ExerciseCommand";
}

export interface ContractsByTemplateQuery_node_Template_choices {
  __typename: "Choice";
  name: string;
}

export interface ContractsByTemplateQuery_node_Template_contracts_edges_node_createEvent_transaction {
  __typename: "Transaction";
  effectiveAt: OpaqueTypes.Time;
}

export interface ContractsByTemplateQuery_node_Template_contracts_edges_node_createEvent {
  __typename: "CreatedEvent";
  id: string;
  transaction: ContractsByTemplateQuery_node_Template_contracts_edges_node_createEvent_transaction;
}

export interface ContractsByTemplateQuery_node_Template_contracts_edges_node_archiveEvent {
  __typename: "ExercisedEvent";
  id: string;
}

export interface ContractsByTemplateQuery_node_Template_contracts_edges_node_template_choices {
  __typename: "Choice";
  name: string;
}

export interface ContractsByTemplateQuery_node_Template_contracts_edges_node_template {
  __typename: "Template";
  id: string;
  choices: ContractsByTemplateQuery_node_Template_contracts_edges_node_template_choices[];
}

export interface ContractsByTemplateQuery_node_Template_contracts_edges_node {
  __typename: "Contract";
  id: string;
  createEvent: ContractsByTemplateQuery_node_Template_contracts_edges_node_createEvent;
  archiveEvent: ContractsByTemplateQuery_node_Template_contracts_edges_node_archiveEvent | null;
  argument: OpaqueTypes.DamlLfValueRecord;
  template: ContractsByTemplateQuery_node_Template_contracts_edges_node_template;
}

export interface ContractsByTemplateQuery_node_Template_contracts_edges {
  __typename: "ContractEdge";
  node: ContractsByTemplateQuery_node_Template_contracts_edges_node;
}

export interface ContractsByTemplateQuery_node_Template_contracts {
  __typename: "ContractPagination";
  totalCount: number;
  edges: ContractsByTemplateQuery_node_Template_contracts_edges[];
}

export interface ContractsByTemplateQuery_node_Template {
  __typename: "Template";
  id: string;
  choices: ContractsByTemplateQuery_node_Template_choices[];
  contracts: ContractsByTemplateQuery_node_Template_contracts;
}

export type ContractsByTemplateQuery_node = ContractsByTemplateQuery_node_Transaction | ContractsByTemplateQuery_node_Template;

export interface ContractsByTemplateQuery {
  node: ContractsByTemplateQuery_node | null;
}

export interface ContractsByTemplateQueryVariables {
  templateId: string;
  filter?: FilterCriterion[] | null;
  search: string;
  count: number;
  sort?: SortCriterion[] | null;
  includeArchived: boolean;
}

/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: TemplatesQuery
// ====================================================

export interface TemplatesQuery_templates_edges_node_contracts {
  __typename: "ContractPagination";
  totalCount: number;
}

export interface TemplatesQuery_templates_edges_node {
  __typename: "Template";
  id: string;
  topLevelDecl: string;
  contracts: TemplatesQuery_templates_edges_node_contracts;
}

export interface TemplatesQuery_templates_edges {
  __typename: "TemplateEdge";
  node: TemplatesQuery_templates_edges_node;
}

export interface TemplatesQuery_templates {
  __typename: "TemplatePagination";
  totalCount: number;
  edges: TemplatesQuery_templates_edges[];
}

export interface TemplatesQuery {
  templates: TemplatesQuery_templates;
}

export interface TemplatesQueryVariables {
  filter?: FilterCriterion[] | null;
  search: string;
  count: number;
  sort?: SortCriterion[] | null;
}

/* tslint:disable */
/* eslint-disable */
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
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: ContractsByIdQuery
// ====================================================

export interface ContractsByIdQuery_nodes_Transaction {
  __typename: "Transaction" | "Template" | "DamlLfDefDataType" | "CreatedEvent" | "ExercisedEvent" | "CreateCommand" | "ExerciseCommand";
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

export type ContractsByIdQuery_nodes = ContractsByIdQuery_nodes_Transaction | ContractsByIdQuery_nodes_Contract;

export interface ContractsByIdQuery {
  nodes: ContractsByIdQuery_nodes[];
}

export interface ContractsByIdQueryVariables {
  contractIds: string[];
}

/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: CommandResultsQuery
// ====================================================

export interface CommandResultsQuery_nodes_Transaction {
  __typename: "Transaction" | "Template" | "DamlLfDefDataType" | "Contract" | "CreatedEvent" | "ExercisedEvent";
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

export type CommandResultsQuery_nodes = CommandResultsQuery_nodes_Transaction | CommandResultsQuery_nodes_CreateCommand;

export interface CommandResultsQuery {
  nodes: CommandResultsQuery_nodes[];
}

export interface CommandResultsQueryVariables {
  commandIds: string[];
}

/* tslint:disable */
/* eslint-disable */
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
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: ParameterFormTypeQuery
// ====================================================

export interface ParameterFormTypeQuery_node_Transaction {
  __typename: "Transaction" | "Template" | "Contract" | "CreatedEvent" | "ExercisedEvent" | "CreateCommand" | "ExerciseCommand";
}

export interface ParameterFormTypeQuery_node_DamlLfDefDataType {
  __typename: "DamlLfDefDataType";
  dataType: OpaqueTypes.DamlLfDataType;
  typeVars: string[];
}

export type ParameterFormTypeQuery_node = ParameterFormTypeQuery_node_Transaction | ParameterFormTypeQuery_node_DamlLfDefDataType;

export interface ParameterFormTypeQuery {
  node: ParameterFormTypeQuery_node | null;
}

export interface ParameterFormTypeQueryVariables {
  id: string;
}

/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL mutation operation: WithExerciseQuery
// ====================================================

export interface WithExerciseQuery {
  exercise: OpaqueTypes.CommandId;
}

export interface WithExerciseQueryVariables {
  contractId: string;
  choiceId: string;
  argument?: OpaqueTypes.DamlLfValue | null;
}

/* tslint:disable */
/* eslint-disable */
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

export interface FilterCriterion {
  field: string;
  value: string;
}

export interface SortCriterion {
  field: string;
  direction: Direction;
}

//==============================================================
// END Enums and Input Objects
//==============================================================
