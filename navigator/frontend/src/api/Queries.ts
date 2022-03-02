import * as OpaqueTypes from "@da/ui-core/lib/api/OpaqueTypes";
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: ContractDetailsById
// ====================================================

export interface ContractDetailsById_node_CreateCommand {
  __typename: "CreateCommand" | "CreatedEvent" | "DamlLfDefDataType" | "ExerciseCommand" | "ExercisedEvent" | "Template" | "Transaction";
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
  key: OpaqueTypes.DamlLfValue | null;
  template: ContractDetailsById_node_Contract_template;
}

export type ContractDetailsById_node = ContractDetailsById_node_CreateCommand | ContractDetailsById_node_Contract;

export interface ContractDetailsById {
  node: ContractDetailsById_node | null;
}

export interface ContractDetailsByIdVariables {
  id: string;
}

/* tslint:disable */
/* eslint-disable */
// @generated
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
// @generated
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
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: TemplateInstance
// ====================================================

export interface TemplateInstance_node_Contract {
  __typename: "Contract" | "CreateCommand" | "CreatedEvent" | "DamlLfDefDataType" | "ExerciseCommand" | "ExercisedEvent" | "Transaction";
}

export interface TemplateInstance_node_Template {
  __typename: "Template";
  id: string;
  parameter: OpaqueTypes.DamlLfType;
  topLevelDecl: string;
}

export type TemplateInstance_node = TemplateInstance_node_Contract | TemplateInstance_node_Template;

export interface TemplateInstance {
  node: TemplateInstance_node | null;
}

export interface TemplateInstanceVariables {
  templateId: string;
}

/* tslint:disable */
/* eslint-disable */
// @generated
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
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: ContractsByTemplateParamQuery
// ====================================================

export interface ContractsByTemplateParamQuery_node_Contract {
  __typename: "Contract" | "CreateCommand" | "CreatedEvent" | "DamlLfDefDataType" | "ExerciseCommand" | "ExercisedEvent" | "Transaction";
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

export type ContractsByTemplateParamQuery_node = ContractsByTemplateParamQuery_node_Contract | ContractsByTemplateParamQuery_node_Template;

export interface ContractsByTemplateParamQuery {
  node: ContractsByTemplateParamQuery_node | null;
}

export interface ContractsByTemplateParamQueryVariables {
  templateId: string;
}

/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: ContractsByTemplateQuery
// ====================================================

export interface ContractsByTemplateQuery_node_Contract {
  __typename: "Contract" | "CreateCommand" | "CreatedEvent" | "DamlLfDefDataType" | "ExerciseCommand" | "ExercisedEvent" | "Transaction";
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

export type ContractsByTemplateQuery_node = ContractsByTemplateQuery_node_Contract | ContractsByTemplateQuery_node_Template;

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
// @generated
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
// @generated
// This file was automatically generated and should not be edited.

//==============================================================
// START Enums and Input Objects
//==============================================================

export enum Direction {
  ASCENDING = "ASCENDING",
  DESCENDING = "DESCENDING",
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
