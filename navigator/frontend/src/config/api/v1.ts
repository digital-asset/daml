// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// eslint-disable  @typescript-eslint/no-explicit-any

// ----------------------------------------------------------------------------
// Sort and filter
// ----------------------------------------------------------------------------
export type SortDirection = "ASCENDING" | "DESCENDING";

export interface SortCriterion {
  /** Field to sort by */
  field: string;
  /** Sort direction */
  direction: SortDirection;
}

export interface FilterCriterion {
  /** Field to filter by */
  field: string;
  /** Filter value */
  value: string;
}

/** Detailed filter */
export type FilterConfig = FilterCriterion[];

/** Generic search */
export type SearchConfig = string;

/** A list of sort criteria */
export type SortConfig = SortCriterion[];

// ----------------------------------------------------------------------------
// Table view sources
// ----------------------------------------------------------------------------
/** All contracts (filtered and sorted) */
export interface TableViewSourceContracts {
  type: "contracts";
  count?: number;
  includeArchived?: boolean;
  filter?: FilterConfig;
  search?: SearchConfig;
  sort?: SortConfig;
}

/** All templates (filtered and sorted) */
export interface TableViewSourceTemplates {
  type: "templates";
  count?: number;
  includeArchived?: boolean;
  filter?: FilterConfig;
  search?: SearchConfig;
  sort?: SortConfig;
}

/** All contracts for a given template (filtered and sorted) */
export interface TableViewSourceTemplateContracts {
  type: "template-contracts";
  template: string;
  count?: number;
  includeArchived?: boolean;
  filter?: FilterConfig;
  search?: SearchConfig;
  sort?: SortConfig;
}

/** Each of these corresponds to a separate GraphQL query */
export type TableViewSource =
  | TableViewSourceContracts
  | TableViewSourceTemplates
  | TableViewSourceTemplateContracts;

// ----------------------------------------------------------------------------
// Custom table view
// ----------------------------------------------------------------------------
export type CellAlignment =
  | "none" // Do nothing, output content as is
  | "left" // Left-align content
  | "center" // Center content
  | "right"; // Right-align content

export interface TableViewCellText {
  type: "text";
  value: string;
}

export interface TableViewCellReact {
  type: "react";
  value: JSX.Element;
}

export interface TableViewCellChoicesButton {
  type: "choices-button";
}

export type TableViewCell =
  | TableViewCellText
  | TableViewCellReact
  | TableViewCellChoicesButton;

export interface TableViewColumn {
  /* Corresponding field (for sorting) */
  key: string;
  /** */
  title: string;
  /** If true, the user can click on the column header to sort by the column */
  sortable: boolean;
  /** Flexbox: initial width, in pixels */
  width: number;
  /** Flexbox: weight */
  weight: number;
  /** Horizontal alignment */
  alignment: CellAlignment;
  /** Function to render table cells */
  createCell(params: {
    /** Same as rowData */
    cellData: ContractsRowData | TemplatesRowData;
    /** zero-based column index of the rendered cell */
    columnIndex: number;
    /**
     * Source data for the table row. Depends on the table view source.
     * For 'contracts' and 'template-contracts', it's a ContractsRowData.
     * For 'templates', it's a TemplatesRowData.
     */
    rowData: ContractsRowData | TemplatesRowData;
    /** zero-based row index of the rendered cell */
    rowIndex: number;
  }): TableViewCell;
}

// ----------------------------------------------------------------------------
// Row Data
// ----------------------------------------------------------------------------

export type Argument =
  | RecordArgument
  | ListArgument
  | string
  | boolean
  | number
  | null;

// eslint-disable-next-line  @typescript-eslint/no-empty-interface
export interface ListArgument extends Array<Argument> {}
export interface RecordArgument {
  [index: string]: Argument;
}
/**
 * A variant argument is a record argument with exactly one property.
 * This type is just for convenience and validation.
 */
export type VariantArgument = RecordArgument;

/** A Parameter describes the type of an Argument */
export type Parameter =
  | { type: "text" }
  | { type: "party" }
  | { type: "contractId" }
  | { type: "decimal" }
  | { type: "integer" }
  | { type: "bool" }
  | { type: "time" }
  | { type: "unit" }
  | UnsupportedParameter
  | RecordParameter
  | VariantParameter
  | ListParameter;

export type RecordParameter = {
  type: "record";
  fields: { [index: string]: Parameter };
};
export type VariantParameter = {
  type: "variant";
  options: { [index: string]: Parameter };
};
export type ListParameter = { type: "list"; elementType: Parameter };
export type UnsupportedParameter = { type: "unsupported"; name: string };

/** rowData for TableViewSourceContracts */
export interface ContractsRowData {
  __typename: "Contract";
  /** Contract ID */
  id: string;
  createTx: {
    /** Time the contract was created */
    effectiveAt: string;
  };
  /**
   * Use to see whether the contract is archived (if __typename=='Transaction')
   * or active (if __typename=='Block')
   */
  activeAtOrArchiveTx: {
    __typename: "Transaction" | "Block";
    id: string;
  };
  /** Contract argument */
  argument: Argument;
  template: {
    /** Template ID */
    id: string;
    /** List of template choice */
    choices: {
      name: string;
      parameter: Parameter;
      consuming: boolean;
      obligatory: boolean;
    }[];
  };
}

/** rowData for TableViewSourceTemplateContracts */
export type TemplateContractsRowData = ContractsRowData;

/** rowData for TableViewSourceTemplates */
export interface TemplatesRowData {
  __typename: "Template";
  /** template ID */
  id: string;
  /** template name (as it appears in the Daml source) */
  topLevelDecl: string;
  contracts: {
    /** number of contracts for this template */
    totalCount: number;
  };
}

// ----------------------------------------------------------------------------
// Custom views
// ----------------------------------------------------------------------------
export interface CustomTableView {
  type: "table-view";
  title: string;
  source: TableViewSource;
  columns: TableViewColumn[];
}

export type CustomView = CustomTableView;

// ----------------------------------------------------------------------------
// Theme
// ----------------------------------------------------------------------------

/**
 * Same as ui-core theme, but all properties optional.
 * Undefined properties default to ui-core default theme
 */
export interface Theme {
  radiusBorder?: string;
  colorBackground?: string;
  colorForeground?: string;
  colorPrimary?: [string, string];
  colorSecondary?: [string, string];
  colorWarning?: [string, string];
  colorDanger?: [string, string];
  colorShade?: string;
  colorFaded?: string;
  colorInputBackground?: string;
  colorNavForeground?: string;
  colorNavFaded?: string;
  colorWeakIcon?: [string, string];
  colorNavPrimary?: [string, string];
  colorNavSecondary?: [string, string];
  documentBackground?: string;
  buttonPadding?: [string, string];
  buttonRadius?: string;
  tooltipRadius?: string;
  guideWidthMax?: string;
  guideWidthMin?: string;
  iconPrefix?: string;
}

// ----------------------------------------------------------------------------
// Config file
// ----------------------------------------------------------------------------

export interface Version {
  schema: string;
  major: number;
  minor: number;
}

export interface ConfigFile {
  /** Required: version */
  version: Version;
  /** If undefined: use default theme */
  theme?(userId: string, party: string, role: string): Theme;
  /** If undefined: no custom views (same as empty array) */
  customViews?(
    userId: string,
    party: string,
    role: string,
  ): { [id: string]: CustomView };
}
