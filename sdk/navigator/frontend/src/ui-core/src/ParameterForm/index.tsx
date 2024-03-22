// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import * as DamlLfTypeF from "../api/DamlLfType";
import {
  DamlLfDataType,
  DamlLfDefDataType,
  DamlLfEnum,
  DamlLfIdentifier,
  DamlLfPrimType,
  DamlLfRecord,
  DamlLfType,
  DamlLfTypeCon,
  DamlLfTypeNumeric,
  DamlLfTypePrim,
  DamlLfVariant,
} from "../api/DamlLfType";
import {
  DamlLfValue,
  DamlLfValueBool,
  DamlLfValueEnum,
  DamlLfValueGenMap,
  DamlLfValueInt64,
  DamlLfValueList,
  DamlLfValueNumeric,
  DamlLfValueOptional,
  DamlLfValueRecord,
  DamlLfValueText,
  DamlLfValueTextMap,
  DamlLfValueUnit,
  DamlLfValueVariant,
  enumCon,
  genMapEntry,
  textMapEntry,
} from "../api/DamlLfValue";
import Button from "../Button";
import { StyledTextInput } from "../Input";
import { LabeledElement } from "../Label";
import NestedForm from "../NestedForm";
import Select from "../Select";
import styled from "../theme";
import TimeInput from "../TimeInput";
import { NonExhaustiveMatch, TypeErrorElement } from "../util";
import ContractIdInput from "./ContractIdInput";
import PartyInput from "./PartyInput";

import * as DamlLfValueF from "@da/ui-core/lib/api/DamlLfValue";

/* eslint-disable @typescript-eslint/no-use-before-define */

//------------------------------------------------------------------------------
// Input Types
//------------------------------------------------------------------------------

interface InputProps<T> {
  parameter: DamlLfTypePrim;
  disabled: boolean;
  onChange(val: T): void;
  argument: DamlLfValue;
  validate?(val: T): boolean;
  name: string;
}

/** Returns true if both the `value` and the `type` are valid for the given type. */
export function matchPrimitiveType(
  value: DamlLfValue,
  type: DamlLfTypePrim,
  name: DamlLfPrimType,
): boolean {
  return (
    (value.type === name || value.type === "undefined") && type.name === name
  );
}

interface NumericInputProps {
  parameter: DamlLfTypeNumeric;
  disabled: boolean;
  onChange(val: DamlLfValueNumeric): void;
  argument: DamlLfValue;
  validate?(val: DamlLfValueNumeric): boolean;
}

function matchNumeric(value: DamlLfValue, type: DamlLfTypeNumeric): boolean {
  return (
    (value.type === "numeric" || value.type === "undefined") &&
    type.type === "numeric"
  );
}

/** Returns true if both the `value` and the `type` are valid for the given type. */
function matchDataType(
  value: DamlLfValue,
  type: DamlLfDataType,
  name: "record",
): value is DamlLfValueRecord;
function matchDataType(
  value: DamlLfValue,
  type: DamlLfDataType,
  name: "variant",
): value is DamlLfValueVariant;
function matchDataType(
  value: DamlLfValue,
  type: DamlLfDataType,
  name: "enum",
): value is DamlLfValueEnum;
function matchDataType(
  value: DamlLfValue,
  type: DamlLfDataType,
  name: "record" | "variant" | "enum",
): boolean {
  return value.type === name && type.type === name;
}

//-------------------------------------------------------------------------------------------------
// Text - primitive value
//-------------------------------------------------------------------------------------------------

const TextInput = (props: InputProps<DamlLfValueText>): JSX.Element => {
  const { argument, parameter, disabled, onChange } = props;
  if (matchPrimitiveType(argument, parameter, "text")) {
    const displayValue = argument.type === "text" ? argument.value : undefined;
    return (
      <StyledTextInput
        type="text"
        disabled={disabled}
        placeholder="Text"
        value={displayValue}
        onChange={e => {
          onChange(DamlLfValueF.text((e.target as HTMLInputElement).value));
        }}
      />
    );
  } else {
    return <TypeErrorElement parameter={parameter} argument={argument} />;
  }
};

/*
// Note: the ledger may use decimals with unlimited precision,
// do not validate by parsing into JS numbers.
const decimalPattern = /^[-]?[0-9]+([.][0-9]*)?$/;
const integerPattern = /^[-]?[0-9]+$/;

const decimalTest = decimalPattern.test.bind(decimalPattern);
const integerTest = integerPattern.test.bind(integerPattern);

function getNextValue<T>(
  prevValue: T | undefined,
  nextValue: string,
  con: (val: string) => T,
  validate: (val: T) => boolean,
  userValidate?: (val: T) => boolean,
) {
  if (nextValue === '') {
    // Empty value - user has deleted all input
    // Reset to undefined, as if no value was entered yet
    return undefined;
  } else {
    // Only accept the new value if it is valid
    // Better would be to allow invalid values, but display the input in an error state.
    // Note: browsers that support number inputs already validate their input
    return nextValue !== undefined &&
      validate(nextValue) &&
      (userValidate === undefined || userValidate(nextValue)) ?
      con(nextValue) : prevValue
  }
}
*/

//-------------------------------------------------------------------------------------------------
// Decimal - primitive value
//-------------------------------------------------------------------------------------------------

const NumericInput = (props: NumericInputProps): JSX.Element => {
  const { parameter, argument, disabled, onChange } = props;
  if (matchNumeric(argument, parameter)) {
    const displayValue =
      argument.type === "numeric" ? argument.value : undefined;
    const prettyType = "Numeric " + parameter.scale;
    return (
      <StyledTextInput
        type="number"
        disabled={disabled}
        placeholder={prettyType}
        step="any"
        value={displayValue}
        onChange={e => {
          onChange(DamlLfValueF.numeric((e.target as HTMLInputElement).value));
        }}
      />
    );
  } else {
    return <TypeErrorElement parameter={parameter} argument={argument} />;
  }
};

//-------------------------------------------------------------------------------------------------
// Int64 - primitive value
//-------------------------------------------------------------------------------------------------

const IntegerInput = (props: InputProps<DamlLfValueInt64>): JSX.Element => {
  const { parameter, argument, disabled, onChange } = props;
  if (matchPrimitiveType(argument, parameter, "int64")) {
    const displayValue = argument.type === "int64" ? argument.value : undefined;
    return (
      <StyledTextInput
        type="number"
        disabled={disabled}
        placeholder="Integer"
        value={displayValue}
        onChange={e => {
          onChange(DamlLfValueF.int64((e.target as HTMLInputElement).value));
        }}
      />
    );
  } else {
    return <TypeErrorElement parameter={parameter} argument={argument} />;
  }
};

//-------------------------------------------------------------------------------------------------
// Unit - primitive value
//-------------------------------------------------------------------------------------------------

const UnitInput = (props: InputProps<DamlLfValueUnit>): JSX.Element => {
  const { parameter, argument } = props;
  if (matchPrimitiveType(argument, parameter, "unit")) {
    return (
      <StyledTextInput
        type="text"
        disabled={true}
        placeholder="unit"
        value="unit"
        onChange={() => {
          return;
        }}
      />
    );
  } else {
    return <TypeErrorElement parameter={parameter} argument={argument} />;
  }
};

//-------------------------------------------------------------------------------------------------
// Variant - nested input form
//-------------------------------------------------------------------------------------------------

interface VariantTypeInputProps {
  parameter: DamlLfVariant;
  disabled: boolean;
  onChange(val: string): void;
  varType: string | undefined;
}

/**
 * A string representation of an unset variant type.
 * This must not be a valid Daml variant name to avoid name clashes.
 */
const variantTypeNone = "";

const VariantTypeInput = (props: VariantTypeInputProps): JSX.Element => {
  const { parameter, disabled, onChange, varType } = props;
  const options = parameter.fields.map(f => ({ value: f.name, label: f.name }));
  return (
    <Select
      disabled={disabled}
      value={varType === undefined ? variantTypeNone : varType}
      onChange={value => {
        onChange(value);
      }}
      options={options}
    />
  );
};

interface VariantInputProps {
  id: DamlLfIdentifier;
  parameter: DamlLfVariant;
  disabled: boolean;
  onChange(val: DamlLfValueVariant): void;
  argument: DamlLfValue;
  level: number;
  partyIdProvider?: PartyIdProvider;
  contractIdProvider?: ContractIdProvider;
  typeProvider: TypeProvider;
}

const VariantInput = (props: VariantInputProps): JSX.Element => {
  const {
    id,
    argument,
    parameter,
    level,
    onChange,
    disabled,
    partyIdProvider,
    contractIdProvider,
    typeProvider,
  } = props;
  if (matchDataType(argument, parameter, "variant")) {
    const value = argument.value;
    const constructorName = argument.constructor;
    const constructor = parameter.fields.filter(
      f => f.name === constructorName,
    )[0];
    return (
      <NestedForm level={level}>
        <LabeledElement label={"Type"} key={"type"}>
          <VariantTypeInput
            parameter={parameter}
            disabled={disabled}
            varType={constructorName}
            onChange={val => {
              const newConstructor = parameter.fields.filter(
                f => f.name === val,
              )[0];
              if (newConstructor === undefined) {
                // Resetting variant to initial state
                onChange(DamlLfValueF.initialVariantValue(id, parameter));
              } else if (constructor === undefined) {
                // Setting a value for the first time
                onChange(
                  DamlLfValueF.variant(
                    id,
                    newConstructor.name,
                    DamlLfValueF.initialValue(newConstructor.value),
                  ),
                );
              } else if (
                DamlLfTypeF.equal(constructor.value, newConstructor.value)
              ) {
                // Constructor changed, but has same type - reuse value
                onChange(DamlLfValueF.variant(id, newConstructor.name, value));
              } else {
                // Constructor changed, type differs - reset value
                onChange(
                  DamlLfValueF.variant(
                    id,
                    newConstructor.name,
                    DamlLfValueF.initialValue(newConstructor.value),
                  ),
                );
              }
            }}
          />
        </LabeledElement>
        {constructor !== undefined ? (
          <LabeledElement label={"Value"} key={"value"}>
            <ParameterInput
              partyIdProvider={partyIdProvider}
              contractIdProvider={contractIdProvider}
              typeProvider={typeProvider}
              parameter={constructor.value}
              name={`${name}.value`}
              argument={value}
              disabled={disabled}
              onChange={val => {
                onChange(DamlLfValueF.variant(id, constructor.name, val));
              }}
              level={level + 1}
            />
          </LabeledElement>
        ) : null}
      </NestedForm>
    );
  } else {
    return <TypeErrorElement parameter={parameter} argument={argument} />;
  }
};

//-------------------------------------------------------------------------------------------------
// Record - nested input form
//-------------------------------------------------------------------------------------------------

interface RecordInputProps {
  id: DamlLfIdentifier;
  parameter: DamlLfRecord;
  disabled: boolean;
  onChange(val: DamlLfValueRecord): void;
  argument: DamlLfValue;
  level: number;
  partyIdProvider?: PartyIdProvider;
  contractIdProvider?: ContractIdProvider;
  typeProvider: TypeProvider;
}

const RecordInput = (props: RecordInputProps): JSX.Element => {
  const {
    id,
    argument,
    parameter,
    level,
    onChange,
    disabled,
    partyIdProvider,
    contractIdProvider,
    typeProvider,
  } = props;
  if (matchDataType(argument, parameter, "record")) {
    const fields = argument.fields;
    return (
      <NestedForm level={level}>
        {parameter.fields.map((f, i) => (
          <LabeledElement label={f.name} key={f.name}>
            <ParameterInput
              partyIdProvider={partyIdProvider}
              contractIdProvider={contractIdProvider}
              typeProvider={typeProvider}
              parameter={f.value}
              name={`${name}.${f.name}`}
              argument={
                fields[i] && fields[i].label === f.name
                  ? fields[i].value
                  : DamlLfValueF.undef()
              }
              disabled={disabled}
              onChange={val => {
                const newFields = fields.slice(0);
                newFields[i] = { label: f.name, value: val };
                onChange(DamlLfValueF.record(id, newFields));
              }}
              level={level + 1}
            />
          </LabeledElement>
        ))}
      </NestedForm>
    );
  } else {
    return <TypeErrorElement parameter={parameter} argument={argument} />;
  }
};

//-------------------------------------------------------------------------------------------------
// Enum - non-nested value
//-------------------------------------------------------------------------------------------------

interface EnumInputProps {
  id: DamlLfIdentifier;
  parameter: DamlLfEnum;
  disabled: boolean;
  onChange(val: DamlLfValueEnum): void;
  argument: DamlLfValue;
  level: number;
}

const EnumInput = (props: EnumInputProps): JSX.Element => {
  const { id, parameter, level, onChange, argument, disabled } = props;
  if (matchDataType(argument, parameter, "enum")) {
    const options = parameter.constructors.map(c => ({ value: c, label: c }));
    return (
      <NestedForm level={level}>
        <LabeledElement label={"Constructor"} key={"constructor"}>
          <Select
            disabled={disabled}
            value={argument.constructor}
            onChange={value => {
              if (value === undefined) {
                onChange(DamlLfValueF.initialEnumValue(id, parameter));
              } else {
                onChange(enumCon(id, value));
              }
            }}
            options={options}
          />
        </LabeledElement>
      </NestedForm>
    );
  } else {
    return <TypeErrorElement parameter={parameter} argument={argument} />;
  }
};

//-------------------------------------------------------------------------------------------------
// Optional - nested value
//-------------------------------------------------------------------------------------------------
interface OptionalInputProps {
  parameter: DamlLfTypePrim;
  disabled: boolean;
  onChange(val: DamlLfValueOptional): void;
  argument: DamlLfValue;
  level: number;
  partyIdProvider?: PartyIdProvider;
  contractIdProvider?: ContractIdProvider;
  typeProvider: TypeProvider;
}

const OptionalInput = (props: OptionalInputProps): JSX.Element => {
  const {
    argument,
    parameter,
    level,
    onChange,
    disabled,
    partyIdProvider,
    contractIdProvider,
    typeProvider,
  } = props;
  if (matchPrimitiveType(argument, parameter, "optional")) {
    const value =
      argument.type === "optional" ? argument.value : DamlLfValueF.undef();
    const valueType = parameter.args[0];
    const constructor = value !== null ? "Some" : "None";
    return (
      <NestedForm level={level}>
        <LabeledElement label={"Type"} key={"type"}>
          <Select
            disabled={disabled}
            value={constructor}
            onChange={newConstructor => {
              if (newConstructor !== constructor) {
                switch (newConstructor) {
                  case "Some":
                    onChange(
                      DamlLfValueF.optional(
                        DamlLfValueF.initialValue(valueType),
                      ),
                    );
                    break;
                  case "None":
                    onChange(DamlLfValueF.optional(null));
                    break;
                }
              }
            }}
            options={[
              { value: "Some", label: "Some" },
              { value: "None", label: "None" },
            ]}
          />
        </LabeledElement>
        {value !== null ? (
          <LabeledElement label={"Value"} key={"value"}>
            <ParameterInput
              partyIdProvider={partyIdProvider}
              contractIdProvider={contractIdProvider}
              typeProvider={typeProvider}
              parameter={valueType}
              name={`${name}.value`}
              argument={value}
              disabled={disabled}
              onChange={val => {
                onChange(DamlLfValueF.optional(val));
              }}
              level={level + 1}
            />
          </LabeledElement>
        ) : null}
      </NestedForm>
    );
  } else {
    return <TypeErrorElement parameter={parameter} argument={argument} />;
  }
};

//-------------------------------------------------------------------------------------------------
// Bool - primitive value
//-------------------------------------------------------------------------------------------------

const RadioLabel = styled.label`
  display: inline-block;
  padding-right: 0.625rem;
  position: relative;
  margin-bottom: 10px;
  cursor: pointer;
  min-height: 20px;
  padding-left: 26px;
  text-transform: none;
  line-height: 16px;
`;

const ControlIndicator = styled.span`
  cursor: pointer;
  width: 16px;
  height: 16px;
  line-height: 16px;
  position: absolute;
  top: 0;
  left: 0;
  margin: 0;
  border: none;
  border-radius: 50%;
  font-size: 6px;
  color: rgba(0, 0, 0, 0.6);
  background: linear-gradient(to bottom, #ffffff, rgba(255, 255, 255, 0)) left
      no-repeat,
    center no-repeat #f5f8fa;
  box-shadow: inset 0 0 0 1px rgba(16, 22, 26, 0.4),
    inset 0 -1px 0 rgba(16, 22, 26, 0.2);
  background-clip: padding-box;
  user-select: none;
  box-sizing: border-box;
  input:checked + & {
    background: linear-gradient(
          to bottom,
          rgba(255, 255, 255, 0.1),
          rgba(255, 255, 255, 0)
        )
        left no-repeat,
      center no-repeat #137cbd;
    color: #ffffff;
  }
  &:before {
    display: inline-block;
    position: absolute;
    top: 50%;
    left: 50%;
    -webkit-transform: translate(-50%, -50%);
    transform: translate(-50%, -50%);
    border-radius: 50%;
    background: #ffffff;
    width: 1em;
    height: 1em;
    content: "";
  }
`;

const StyledRadioInput = styled.input`
  position: absolute;
  top: 0;
  left: 0;
  opacity: 0;
  z-index: -1;
`;

const BoolInput = (props: InputProps<DamlLfValueBool>): JSX.Element => {
  const { parameter, argument, disabled, onChange } = props;
  if (matchPrimitiveType(argument, parameter, "bool")) {
    const value = argument.type === "bool" ? argument.value : undefined;
    return (
      <div>
        <RadioLabel>
          <StyledRadioInput
            type="radio"
            disabled={disabled}
            name={`${name}.true`}
            checked={value}
            onChange={() => {
              onChange(DamlLfValueF.bool(true));
            }}
          />
          <ControlIndicator />
          True
        </RadioLabel>
        <RadioLabel>
          <StyledRadioInput
            type="radio"
            disabled={disabled}
            name={`${name}.false`}
            checked={!value}
            onChange={() => {
              onChange(DamlLfValueF.bool(false));
            }}
          />
          <ControlIndicator />
          False
        </RadioLabel>
      </div>
    );
  } else {
    return <TypeErrorElement parameter={parameter} argument={argument} />;
  }
};

//-------------------------------------------------------------------------------------------------
// List - nested input form
//-------------------------------------------------------------------------------------------------

const ListControls = styled.div`
  display: flex;
  align-items: center;
`;

const ListControlButton = styled(Button)`
  margin-right: 10px;
`;

interface ListInputProps extends InputProps<DamlLfValueList> {
  parameter: DamlLfTypePrim;
  name: string;
  level: number;
  partyIdProvider?: PartyIdProvider;
  contractIdProvider?: ContractIdProvider;
  typeProvider: TypeProvider;
}

const ListInput = (props: ListInputProps): JSX.Element => {
  const {
    argument,
    parameter,
    level,
    name,
    onChange,
    disabled,
    partyIdProvider,
    contractIdProvider,
    typeProvider,
  } = props;
  if (matchPrimitiveType(argument, parameter, "list")) {
    const elements = argument && argument.type === "list" ? argument.value : [];
    const elementType = parameter.args[0] || DamlLfTypeF.unit();
    return (
      <NestedForm level={level}>
        {elements.map((k, i) => (
          <LabeledElement label={`[${i}]`} key={i}>
            <ParameterInput
              partyIdProvider={partyIdProvider}
              contractIdProvider={contractIdProvider}
              typeProvider={typeProvider}
              parameter={elementType}
              name={`${name}[${i}]`}
              argument={k}
              disabled={disabled}
              onChange={val => {
                const newElements = elements.slice(0);
                newElements[i] = val;
                onChange(DamlLfValueF.list(newElements));
              }}
              level={level + 1}
            />
          </LabeledElement>
        ))}
        <ListControls>
          <ListControlButton
            type="main"
            onClick={_ => {
              const newElements = elements.slice(0);
              newElements.push(DamlLfValueF.initialValue(elementType));
              onChange(DamlLfValueF.list(newElements));
            }}>
            Add new element
          </ListControlButton>
          <ListControlButton
            type="main"
            onClick={_ => {
              onChange(DamlLfValueF.list(elements.slice(0, -1)));
            }}
            disabled={elements.length === 0}>
            Delete last element
          </ListControlButton>
        </ListControls>
      </NestedForm>
    );
  } else {
    return <TypeErrorElement parameter={parameter} argument={argument} />;
  }
};

interface MapInputProps extends InputProps<DamlLfValueTextMap> {
  parameter: DamlLfTypePrim;
  name: string;
  level: number;
  partyIdProvider?: PartyIdProvider;
  contractIdProvider?: ContractIdProvider;
  typeProvider: TypeProvider;
}

const MapInput = (props: MapInputProps): JSX.Element => {
  const {
    argument,
    parameter,
    level,
    onChange,
    disabled,
    partyIdProvider,
    contractIdProvider,
    typeProvider,
  } = props;
  if (matchPrimitiveType(argument, parameter, "textmap")) {
    const elements =
      argument && argument.type === "textmap" ? argument.value : [];
    const elementType = parameter.args[0] || DamlLfTypeF.unit();
    return (
      <NestedForm level={level}>
        {elements.map((entry, i) => (
          <LabeledElement label={`entry[${i}]`} key={`entry[${i}]`}>
            <NestedForm level={level + 1}>
              <LabeledElement label={`key`} key={`key[${i}]`}>
                <StyledTextInput
                  type="string"
                  disabled={disabled}
                  placeholder="String"
                  step="any"
                  value={elements[i].key}
                  onChange={key => {
                    const newElements = elements.slice(0);
                    newElements[i] = textMapEntry(
                      (key.target as HTMLInputElement).value,
                      newElements[i].value,
                    );
                    onChange(DamlLfValueF.textmap(newElements));
                  }}
                />
              </LabeledElement>
              <LabeledElement label={`value`} key={`value[${i}]`}>
                <ParameterInput
                  partyIdProvider={partyIdProvider}
                  contractIdProvider={contractIdProvider}
                  typeProvider={typeProvider}
                  parameter={elementType}
                  name={`value[${i}]`}
                  argument={entry.value}
                  disabled={disabled}
                  onChange={val => {
                    const newElements = elements.slice(0);
                    newElements[i] = textMapEntry(elements[i].key, val);
                    onChange(DamlLfValueF.textmap(newElements));
                  }}
                  level={level + 2}
                />
              </LabeledElement>
            </NestedForm>
          </LabeledElement>
        ))}
        <ListControls>
          <ListControlButton
            type="main"
            onClick={_ => {
              const newElements = elements.slice(0);
              newElements.push(
                DamlLfValueF.textMapEntry(
                  "",
                  DamlLfValueF.initialValue(elementType),
                ),
              );
              onChange(DamlLfValueF.textmap(newElements));
            }}>
            Add new entry
          </ListControlButton>
          <ListControlButton
            type="main"
            onClick={_ => {
              onChange(DamlLfValueF.textmap(elements.slice(0, -1)));
            }}
            disabled={elements.length === 0}>
            Delete last entry
          </ListControlButton>
        </ListControls>
      </NestedForm>
    );
  } else {
    return <TypeErrorElement parameter={parameter} argument={argument} />;
  }
};

interface GenMapInputProps extends InputProps<DamlLfValueGenMap> {
  parameter: DamlLfTypePrim;
  name: string;
  level: number;
  partyIdProvider?: PartyIdProvider;
  contractIdProvider?: ContractIdProvider;
  typeProvider: TypeProvider;
}

const GenMapInput = (props: GenMapInputProps): JSX.Element => {
  const {
    argument,
    parameter,
    level,
    onChange,
    disabled,
    partyIdProvider,
    contractIdProvider,
    typeProvider,
  } = props;
  if (matchPrimitiveType(argument, parameter, "genmap")) {
    const entries =
      argument && argument.type === "genmap" ? argument.value : [];
    const keyType = parameter.args[0] || DamlLfTypeF.unit();
    const valueType = parameter.args[1] || DamlLfTypeF.unit();
    return (
      <NestedForm level={level}>
        {entries.map((entry, i) => (
          <LabeledElement label={`entries[${i}]`} key={`entries[${i}]`}>
            <NestedForm level={level + 1}>
              <LabeledElement label={`key`} key={`entries[${i}].key`}>
                <ParameterInput
                  partyIdProvider={partyIdProvider}
                  contractIdProvider={contractIdProvider}
                  typeProvider={typeProvider}
                  parameter={keyType}
                  name={`entries[${i}].key`}
                  argument={entry.key}
                  disabled={disabled}
                  onChange={key => {
                    const newElements = entries.slice(0);
                    newElements[i] = genMapEntry(key, entries[i].value);
                    onChange(DamlLfValueF.genmap(newElements));
                  }}
                  level={level + 2}
                />
              </LabeledElement>
              <LabeledElement label={`value`} key={`entries[${i}].value`}>
                <ParameterInput
                  partyIdProvider={partyIdProvider}
                  contractIdProvider={contractIdProvider}
                  typeProvider={typeProvider}
                  parameter={valueType}
                  name={`entries[${i}].value`}
                  argument={entry.value}
                  disabled={disabled}
                  onChange={val => {
                    const newElements = entries.slice(0);
                    newElements[i] = genMapEntry(entries[i].key, val);
                    onChange(DamlLfValueF.genmap(newElements));
                  }}
                  level={level + 2}
                />
              </LabeledElement>
            </NestedForm>
          </LabeledElement>
        ))}
        <ListControls>
          <ListControlButton
            type="main"
            onClick={_ => {
              const newElements = entries.slice(0);
              newElements.push(
                DamlLfValueF.genMapEntry(
                  DamlLfValueF.initialValue(keyType),
                  DamlLfValueF.initialValue(valueType),
                ),
              );
              onChange(DamlLfValueF.genmap(newElements));
            }}>
            Add new entry
          </ListControlButton>
          <ListControlButton
            type="main"
            onClick={_ => {
              onChange(DamlLfValueF.genmap(entries.slice(0, -1)));
            }}
            disabled={entries.length === 0}>
            Delete last entry
          </ListControlButton>
        </ListControls>
      </NestedForm>
    );
  } else {
    return <TypeErrorElement parameter={parameter} argument={argument} />;
  }
};

//-------------------------------------------------------------------------------------------------
// DamlLfTypeCon - user defined data type
//-------------------------------------------------------------------------------------------------

interface TypeConInputProps {
  argument: DamlLfValue;
  parameter: DamlLfTypeCon;
  onChange(value: DamlLfValue): void;
  disabled: boolean;
  name: string;
  level: number;
  partyIdProvider?: PartyIdProvider;
  contractIdProvider?: ContractIdProvider;
  typeProvider: TypeProvider;
}

interface TypeConInputState {
  dataType: DamlLfDataType | undefined;
}

class TypeConInput extends React.Component<
  TypeConInputProps,
  TypeConInputState
> {
  constructor(props: TypeConInputProps) {
    super(props);
    this.state = {
      dataType: undefined,
    };
  }

  private onParamDefChanged(ddt: DamlLfDefDataType | undefined) {
    const { parameter, onChange } = this.props;
    if (ddt) {
      const dataType = DamlLfTypeF.instantiate(parameter, ddt);
      switch (dataType.type) {
        case "record":
          onChange(DamlLfValueF.initialRecordValue(parameter.name, dataType));
          break;
        case "variant":
          onChange(DamlLfValueF.initialVariantValue(parameter.name, dataType));
          break;
        case "enum":
          onChange(DamlLfValueF.initialEnumValue(parameter.name, dataType));
          break;
        default:
          throw new NonExhaustiveMatch(dataType);
      }
      this.setState({ dataType });
    } else {
      this.setState({ dataType: undefined });
      onChange(DamlLfValueF.undef());
    }
  }

  private onParamChanged() {
    const { parameter, typeProvider } = this.props;
    this.setState({ dataType: undefined });
    typeProvider.fetchType(parameter.name, (id, ddt) => {
      if (parameter.name === id) {
        this.onParamDefChanged(ddt);
      }
    });
  }

  componentDidUpdate(prevProps: TypeConInputProps) {
    if (!DamlLfTypeF.equal(prevProps.parameter, this.props.parameter)) {
      this.onParamChanged();
    }
  }

  componentDidMount() {
    this.onParamChanged();
  }

  render() {
    const {
      argument,
      parameter,
      disabled,
      onChange,
      level,
      partyIdProvider,
      contractIdProvider,
      typeProvider,
    } = this.props;
    const { dataType } = this.state;

    if (dataType === undefined) {
      return <em>Loading type {parameter.name.name}...</em>;
    } else {
      switch (dataType.type) {
        case "record":
          return (
            <RecordInput
              argument={argument}
              id={parameter.name}
              parameter={dataType}
              disabled={disabled}
              onChange={onChange}
              level={level}
              partyIdProvider={partyIdProvider}
              contractIdProvider={contractIdProvider}
              typeProvider={typeProvider}
            />
          );
        case "variant":
          return (
            <VariantInput
              argument={argument}
              id={parameter.name}
              parameter={dataType}
              disabled={disabled}
              onChange={onChange}
              level={level}
              partyIdProvider={partyIdProvider}
              contractIdProvider={contractIdProvider}
              typeProvider={typeProvider}
            />
          );
        case "enum":
          return (
            <EnumInput
              id={parameter.name}
              parameter={dataType}
              disabled={disabled}
              onChange={onChange}
              argument={argument}
              level={level}
            />
          );

        default:
          throw new NonExhaustiveMatch(dataType);
      }
    }
  }
}

//-------------------------------------------------------------------------------------------------
// DataProvider - returns a list of contracts, used for the contract id input
//-------------------------------------------------------------------------------------------------
export interface ParameterFormContract {
  id: string;
  createEvent: { transaction: { effectiveAt: string } };
  archiveEvent: { transaction: { effectiveAt: string } } | null;
  template: { id: string };
}
export interface ContractIdProvider {
  fetchContracts(
    filter: string,
    onResult: (result: ParameterFormContract[]) => void,
  ): void;
}

export interface TypeProvider {
  fetchType(
    id: DamlLfIdentifier,
    onResult: (
      id: DamlLfIdentifier,
      result: DamlLfDefDataType | undefined,
    ) => void,
  ): void;
}

export interface ParameterFormParty {
  id: string;
}
export interface PartyIdProvider {
  fetchParties(
    filter: string,
    onResult: (result: ParameterFormParty[]) => void,
  ): void;
}

//-------------------------------------------------------------------------------------------------
// Parameter Input
//-------------------------------------------------------------------------------------------------

export interface ParameterInputProps {
  parameter: DamlLfType;
  disabled: boolean;
  onChange(value: DamlLfValue): void;
  argument: DamlLfValue;
  validate?(val: DamlLfValue): boolean;
  name: string;
  level: number;
  partyIdProvider?: PartyIdProvider;
  contractIdProvider?: ContractIdProvider;
  typeProvider: TypeProvider;
}

export const ParameterInput = (props: ParameterInputProps): JSX.Element => {
  const {
    argument,
    parameter,
    name,
    disabled,
    onChange,
    partyIdProvider,
    contractIdProvider,
    typeProvider,
    validate,
    level,
  } = props;

  if (parameter.type === "primitive") {
    const primt = parameter.name;
    switch (primt) {
      case "text":
        return (
          <TextInput
            parameter={parameter}
            disabled={disabled}
            onChange={onChange}
            argument={argument}
            validate={validate}
            name={name}
          />
        );
      case "party":
        return (
          <PartyInput
            onFetchParties={
              partyIdProvider &&
              partyIdProvider.fetchParties.bind(partyIdProvider)
            }
            parameter={parameter}
            disabled={disabled}
            onChange={onChange}
            argument={argument}
          />
        );
      case "contractid":
        return (
          <ContractIdInput
            onFetchContracts={
              contractIdProvider &&
              contractIdProvider.fetchContracts.bind(contractIdProvider)
            }
            parameter={parameter}
            disabled={disabled}
            onChange={onChange}
            argument={argument}
          />
        );
      case "int64":
        return (
          <IntegerInput
            parameter={parameter}
            disabled={disabled}
            onChange={onChange}
            argument={argument}
            validate={validate}
            name={name}
          />
        );
      case "timestamp":
        return (
          <TimeInput
            parameter={parameter}
            disabled={disabled}
            onChange={onChange}
            argument={argument}
          />
        );
      case "date":
        return (
          <TimeInput
            parameter={parameter}
            disabled={disabled}
            onChange={onChange}
            argument={argument}
          />
        );
      case "bool":
        return (
          <BoolInput
            parameter={parameter}
            disabled={disabled}
            onChange={onChange}
            argument={argument}
            validate={validate}
            name={name}
          />
        );
      case "unit":
        return (
          <UnitInput
            parameter={parameter}
            disabled={disabled}
            onChange={onChange}
            argument={argument}
            validate={validate}
            name={name}
          />
        );
      case "list": {
        return (
          <ListInput
            parameter={parameter}
            disabled={disabled}
            onChange={onChange}
            argument={argument}
            validate={validate}
            level={level}
            name={name}
            partyIdProvider={partyIdProvider}
            contractIdProvider={contractIdProvider}
            typeProvider={typeProvider}
          />
        );
      }
      case "optional": {
        return (
          <OptionalInput
            parameter={parameter}
            disabled={disabled}
            onChange={onChange}
            argument={argument}
            level={level}
            partyIdProvider={partyIdProvider}
            typeProvider={typeProvider}
          />
        );
      }
      case "textmap": {
        return (
          <MapInput
            parameter={parameter}
            name={name}
            level={level}
            partyIdProvider={partyIdProvider}
            typeProvider={typeProvider}
            disabled={disabled}
            onChange={onChange}
            argument={argument}
          />
        );
      }
      case "genmap": {
        return (
          <GenMapInput
            parameter={parameter}
            name={name}
            level={level}
            typeProvider={typeProvider}
            disabled={disabled}
            onChange={onChange}
            argument={argument}
          />
        );
      }
      default:
        throw new NonExhaustiveMatch(primt);
    }
  } else if (parameter.type === "numeric") {
    return (
      <NumericInput
        parameter={parameter}
        disabled={disabled}
        onChange={onChange}
        argument={argument}
        validate={validate}
      />
    );
  } else if (parameter.type === "typecon") {
    return (
      <TypeConInput
        parameter={parameter}
        disabled={disabled}
        argument={argument}
        level={level}
        name={name}
        onChange={onChange}
        partyIdProvider={partyIdProvider}
        contractIdProvider={contractIdProvider}
        typeProvider={typeProvider}
      />
    );
  } else if (parameter.type === "typevar") {
    return (
      <em>
        Type variable &apos;{parameter.name}&apos;. If you see this, it means
        there is a problem with Navigator.
      </em>
    );
  } else {
    throw new NonExhaustiveMatch(parameter);
  }
};

//------------------------------------------------------------------------------
// Parameter Form
//------------------------------------------------------------------------------

export interface Props {
  parameter: DamlLfType;
  disabled: boolean;
  onChange(argument: DamlLfValue): void;
  onSubmit(e: React.MouseEvent<HTMLButtonElement>, argument: DamlLfValue): void;
  argument: DamlLfValue;
  className?: string;
  error?: string;
  partyIdProvider?: PartyIdProvider;
  contractIdProvider?: ContractIdProvider;
  typeProvider: TypeProvider;
}

export const StyledForm: React.FC<
  React.HTMLProps<HTMLFormElement>
> = styled.form`
  display: flex;
  flex-direction: column;
  align-content: center;
  flex-wrap: wrap;
  justify-content: flex-start;
`;

const StyledButton = styled(Button)`
  width: 100%;
`;

export interface ErrorProps {
  error?: string;
}

const ErrorContainer = styled.div`
  line-height: 1.5;
  font-size: 14px;
  position: realtive;
  border-radius: 3px;
  padding: 10px 12px 9px;
  background-color: ${props => props.theme.colorDanger};
`;

const ErrorText = styled.pre`
  color: #c23030;
  margin: 0;
  padding: 0;
  box-shadow: none;
  border-radius: 0;
  background: none;
  overflow: scroll;
  word-wrap: break-word;
  white-space: pre-wrap;
`;

export const ErrorMessage = (props: ErrorProps): JSX.Element => {
  if (!props.error || props.error.length === 0) {
    return <span />;
  }
  return (
    <div>
      <ErrorContainer>
        <h5>Error</h5>
        <ErrorText>{props.error}</ErrorText>
      </ErrorContainer>
      <p />
    </div>
  );
};

const ParameterForm = (props: Props): JSX.Element => {
  const {
    className,
    parameter,
    argument,
    disabled,
    onChange,
    onSubmit,
    error,
    partyIdProvider,
    contractIdProvider,
    typeProvider,
  } = props;

  const submit = (e: React.MouseEvent<HTMLButtonElement>) => {
    onSubmit(e, argument);
  };

  return (
    <StyledForm className={className}>
      <ParameterInput
        partyIdProvider={partyIdProvider}
        contractIdProvider={contractIdProvider}
        typeProvider={typeProvider}
        parameter={parameter}
        name="root"
        level={0}
        argument={argument}
        disabled={disabled}
        onChange={onChange}
      />
      <ErrorMessage error={error} />
      <StyledButton type="main" onClick={submit}>
        Submit
      </StyledButton>
    </StyledForm>
  );
};

export default ParameterForm;
