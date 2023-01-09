// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import Autosuggest from "../Autosuggest";
import { Section } from "../Guide";
import styled from "../theme";

interface Contract {
  id: string;
  createTx: {
    id: string;
    effectiveAt: string;
  };
  template: {
    id: string;
  };
}

const contracts: Contract[] = [
  {
    id: "c1230001",
    createTx: {
      id: "1",
      effectiveAt: "2017-06-05T11:34:21Z",
    },
    template: {
      id: "t90001111",
    },
  },
  {
    id: "c1230002",
    createTx: {
      id: "2",
      effectiveAt: "2017-06-05T11:35:26Z",
    },
    template: {
      id: "t90002222",
    },
  },
  {
    id: "c1230003",
    createTx: {
      id: "3",
      effectiveAt: "2017-06-05T11:35:26Z",
    },
    template: {
      id: "t90003333",
    },
  },
] as Contract[];

function onFetchSuggestions(
  query: string,
  onResult: (result: Contract[]) => void,
): void {
  onResult(
    contracts.filter(
      c => c.id.indexOf(query) >= 0 || c.template.id.indexOf(query) >= 0,
    ),
  );
}

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

function renderSuggestion(c: Contract): JSX.Element {
  return (
    <Container>
      <SmallColumn>{c.id}</SmallColumn>
      <BigColumn>{c.template.id}</BigColumn>
      <BigColumn>{c.createTx.effectiveAt}</BigColumn>
    </Container>
  );
}

type AutosuggestType = Autosuggest<Contract>;
type AutosuggestCtor = new () => AutosuggestType;
const TypedAutosuggest: AutosuggestCtor = Autosuggest as AutosuggestCtor;

export default (): JSX.Element => (
  <Section
    title="Autosuggest"
    description="This input field shows suggestions based on the current value.">
    <TypedAutosuggest
      placeholder="Start typing"
      onFetchSuggestions={onFetchSuggestions}
      getSuggestionValue={c => c.id}
      renderSuggestion={renderSuggestion}
    />
    <p />
    <TypedAutosuggest
      placeholder="Disabled"
      disabled={true}
      onFetchSuggestions={onFetchSuggestions}
      getSuggestionValue={c => c.id}
      renderSuggestion={renderSuggestion}
    />
  </Section>
);
