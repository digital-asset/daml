// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from 'react';
import { DamlLfTypePrim } from '../api/DamlLfType';
import { DamlLfValue } from '../api/DamlLfValue';
import * as DamlLfValueF from '../api/DamlLfValue';
import Autosuggest from '../Autosuggest'
import styled from '../theme';
import { TypeErrorElement } from '../util';
import { matchPrimitiveType, ParameterFormContract } from './index'

const Container = styled.div`
  display: flex;
  width: 560px;
`;

const SmallColumn = styled.div`
  flex: 0.2;
  width: 100%;
`;

const BigColumn = styled.div`
  flex: 0.4;
  width: 100%;
`;

function renderSuggestion(c: ParameterFormContract): JSX.Element {
  return (
    <Container>
      <SmallColumn>{c.id}</SmallColumn>
      <BigColumn>{c.template.id}</BigColumn>
      <BigColumn>{c.createEvent.transaction.effectiveAt}</BigColumn>
    </Container>
  );
}

export interface Props {
  className?: string;
  parameter: DamlLfTypePrim;
  disabled: boolean;
  onChange(val: DamlLfValue): void;
  argument: DamlLfValue;
  onFetchContracts?(
    filter: string,
    onResult: (result: ParameterFormContract[]) => void,
  ): void;
}

type AutosuggestType = Autosuggest<ParameterFormContract>;
type AutosuggestCtor = new() => AutosuggestType;
const TypedAutosuggest: AutosuggestCtor = Autosuggest as AutosuggestCtor;

const ContractIdInput: React.StatelessComponent<Props> = (props) => {
  const { argument, parameter, className, disabled, onChange, onFetchContracts = () => []} = props;
  if (matchPrimitiveType(argument, parameter, 'contractid')) {
    const displayValue = argument.type === 'contractid' ? argument.value : undefined;
    return (
      <TypedAutosuggest
        className={className}
        initialValue={displayValue}
        disabled={disabled}
        placeholder="Contract ID"
        onFetchSuggestions={onFetchContracts}
        renderSuggestion={renderSuggestion}
        getSuggestionValue={(c) => c.id}
        onChange={(str) => onChange(DamlLfValueF.contractid(str))}
      />
    );
  } else {
    return (<TypeErrorElement parameter={parameter} argument={argument} />);
  }
};

export default ContractIdInput;
