// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { DamlLfTypePrim } from "../api/DamlLfType";
import { DamlLfValue } from "../api/DamlLfValue";
import * as DamlLfValueF from "../api/DamlLfValue";
import { shortenPartyId } from "../api/IdentifierShortening";
import Autosuggest from "../Autosuggest";
import styled from "../theme";
import { TypeErrorElement } from "../util";
import { matchPrimitiveType, ParameterFormParty } from "./index";

const Container = styled.div`
  display: flex;
  width: 560px;
`;

const BigColumn = styled.div`
  flex: 0.4;
  width: 100%;
`;

function renderSuggestion(p: ParameterFormParty): JSX.Element {
  return (
    <Container>
      <BigColumn>{shortenPartyId(p.id)}</BigColumn>
    </Container>
  );
}

export interface Props {
  className?: string;
  parameter: DamlLfTypePrim;
  disabled: boolean;
  onChange(val: DamlLfValue): void;
  argument: DamlLfValue;
  onFetchParties?(
    filter: string,
    onResult: (result: ParameterFormParty[]) => void,
  ): void;
}

type AutosuggestType = Autosuggest<ParameterFormParty>;
type AutosuggestCtor = new () => AutosuggestType;
const TypedAutosuggest: AutosuggestCtor = Autosuggest as AutosuggestCtor;

const PartyInput: React.StatelessComponent<Props> = props => {
  const {
    argument,
    parameter,
    className,
    disabled,
    onChange,
    onFetchParties = () => [],
  } = props;
  if (matchPrimitiveType(argument, parameter, "party")) {
    const displayValue = argument.type === "party" ? argument.value : undefined;
    return (
      <TypedAutosuggest
        className={className}
        initialValue={displayValue}
        disabled={disabled}
        placeholder="Party"
        onFetchSuggestions={onFetchParties}
        renderSuggestion={renderSuggestion}
        getSuggestionValue={p => p.id}
        onChange={str => onChange(DamlLfValueF.party(str))}
      />
    );
  } else {
    return <TypeErrorElement parameter={parameter} argument={argument} />;
  }
};

export default PartyInput;
