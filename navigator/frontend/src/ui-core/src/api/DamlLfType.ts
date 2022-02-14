// Copyright (c) 2020, Digital Asset (Switzerland) GmbH and/or its affiliates.
// All rights reserved.

import { NonExhaustiveMatch } from "../util";

// --------------------------------------------------------------------------------------------------------------------
// Type definitions
// --------------------------------------------------------------------------------------------------------------------
export interface DamlLfIdentifier {
  package: string;
  module: string;
  name: string;
}

export type DamlLfPrimType =
  | "text"
  | "int64"
  | "bool"
  | "contractid"
  | "timestamp"
  | "date"
  | "party"
  | "unit"
  | "optional"
  | "list"
  | "textmap"
  | "genmap";

export type DamlLfTypePrim = {
  type: "primitive";
  name: DamlLfPrimType;
  args: DamlLfType[];
};
export type DamlLfTypeVar = { type: "typevar"; name: string };
export type DamlLfTypeCon = {
  type: "typecon";
  name: DamlLfIdentifier;
  args: DamlLfType[];
};
export type DamlLfTypeNumeric = { type: "numeric"; scale: number };

export type DamlLfType =
  | DamlLfTypePrim
  | DamlLfTypeVar
  | DamlLfTypeCon
  | DamlLfTypeNumeric;

export type DamlLFFieldWithType = { name: string; value: DamlLfType };

export type DamlLfRecord = { type: "record"; fields: DamlLFFieldWithType[] };
export type DamlLfVariant = { type: "variant"; fields: DamlLFFieldWithType[] };
export type DamlLfEnum = { type: "enum"; constructors: string[] };
export type DamlLfDataType = DamlLfRecord | DamlLfVariant | DamlLfEnum;

export type DamlLfDefDataType = {
  dataType: DamlLfDataType;
  typeVars: string[];
};

// --------------------------------------------------------------------------------------------------------------------
// Constructors
// --------------------------------------------------------------------------------------------------------------------

export function unit(): DamlLfTypePrim {
  return { type: "primitive", name: "unit", args: [] };
}
export function bool(): DamlLfTypePrim {
  return { type: "primitive", name: "bool", args: [] };
}
export function int64(): DamlLfTypePrim {
  return { type: "primitive", name: "int64", args: [] };
}
export function text(): DamlLfTypePrim {
  return { type: "primitive", name: "text", args: [] };
}
export function numeric(s: number): DamlLfTypeNumeric {
  return { type: "numeric", scale: s };
}
export function party(): DamlLfTypePrim {
  return { type: "primitive", name: "party", args: [] };
}
export function contractid(): DamlLfTypePrim {
  return { type: "primitive", name: "contractid", args: [] };
}
export function timestamp(): DamlLfTypePrim {
  return { type: "primitive", name: "timestamp", args: [] };
}
export function date(): DamlLfTypePrim {
  return { type: "primitive", name: "date", args: [] };
}
export function list(type: DamlLfType): DamlLfTypePrim {
  return { type: "primitive", name: "list", args: [type] };
}
export function textmap(type: DamlLfType): DamlLfTypePrim {
  return { type: "primitive", name: "textmap", args: [type] };
}
export function genmap(
  keytype: DamlLfType,
  valueType: DamlLfType,
): DamlLfTypePrim {
  return { type: "primitive", name: "genmap", args: [keytype, valueType] };
}
export function optional(type: DamlLfType): DamlLfTypePrim {
  return { type: "primitive", name: "optional", args: [type] };
}
export function typevar(name: string): DamlLfTypeVar {
  return { type: "typevar", name };
}

// --------------------------------------------------------------------------------------------------------------------
// Utility functions
// --------------------------------------------------------------------------------------------------------------------

/** The type of the parameter of the given template */
export function templateType(
  name: string,
  module: string,
  pack: string,
): DamlLfTypeCon {
  return { type: "typecon", name: { name, module, package: pack }, args: [] };
}

/** Returns a string representation of the given identifier, as it is used in the GraphQL API */
export function opaqueIdentifier(id: DamlLfIdentifier): string {
  return `${id.module}:${id.name}@${id.package}`;
}

export function equal(t1: DamlLfType, t2: DamlLfType): boolean {
  // Could be optimized by doing a proper deep equal
  return t1 === t2 || JSON.stringify(t1) === JSON.stringify(t2);
}

export function equalId(t1: DamlLfIdentifier, t2: DamlLfIdentifier): boolean {
  return (
    t1 === t2 ||
    (t1.module === t2.module &&
      t1.name === t2.name &&
      t1.package === t1.package)
  );
}

/** Replace all type variables that occur *anywhere* in the given type */
export function mapTypeVars(
  t: DamlLfType,
  f: (t: DamlLfTypeVar) => DamlLfType,
): DamlLfType {
  switch (t.type) {
    case "typevar":
      return f(t);
    case "primitive":
      return {
        type: "primitive",
        name: t.name,
        args: t.args.map(a => mapTypeVars(a, f)),
      };
    case "typecon":
      return {
        type: "typecon",
        name: t.name,
        args: t.args.map(a => mapTypeVars(a, f)),
      };
    case "numeric":
      return t;
    default:
      throw new NonExhaustiveMatch(t);
  }
}

/**
 * Instantiate a type constructor.
 * `tc.name` should the identifier of the given `ddt`
 * The result is a closed type (i.e., one without type variables).
 */
export function instantiate(
  tc: DamlLfTypeCon,
  ddt: DamlLfDefDataType,
): DamlLfDataType {
  if (ddt.typeVars.length !== tc.args.length) {
    // This should never happen for valid Daml-LF types
    // Instead of throwing an exception, return the data type as is (in open form).
    // The result may contain occurrences of DamlLfTypeVar
    return ddt.dataType;
  }

  const typeMap: { [index: string]: DamlLfType } = {};
  ddt.typeVars.forEach((v, i) => (typeMap[v] = tc.args[i]));

  switch (ddt.dataType.type) {
    case "record":
      return {
        type: "record",
        fields: ddt.dataType.fields.map(f => ({
          name: f.name,
          value: mapTypeVars(f.value, n => typeMap[n.name]),
        })),
      };
    case "variant":
      return {
        type: "variant",
        fields: ddt.dataType.fields.map(f => ({
          name: f.name,
          value: mapTypeVars(f.value, n => typeMap[n.name]),
        })),
      };
    case "enum":
      return { type: "enum", constructors: ddt.dataType.constructors };
    default:
      throw new NonExhaustiveMatch(ddt.dataType);
  }
}
