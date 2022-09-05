// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import ChoicesButton, { Contract } from ".";
import { Section } from "../Guide";
import styled from "../theme";

const contracts: Contract[] = [
  {
    id: "26:0_",
    template: {
      choices: [
        {
          name: "call",
        },
        {
          name: "transfer",
        },
      ],
    },
    archiveEvent: null,
  },
  {
    id: "26:0_",
    template: {
      choices: [],
    },
    archiveEvent: null,
  },
  {
    id: "26:0_",
    template: {
      choices: [],
    },
    archiveEvent: { id: "1970-01-01T00:00:00Z" },
  },
] as Contract[];

const Grid = styled.div`
  display: flex;
  justify-content: flex-start;
  flex-wrap: wrap;
`;

const ExhibitWrapper = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;
  margin: 0.5rem;
  margin-bottom: 1rem;
  width: 7rem;
`;

const description = `Icon that opens a popover with contract choices.`;

export default (): JSX.Element => (
  <Section title="Choice buttons" description={description}>
    <Grid>
      <ExhibitWrapper>
        <ChoicesButton
          contract={contracts[0]}
          renderLink={(_id, name) => <div>{name}</div>}
        />
      </ExhibitWrapper>
      <ExhibitWrapper>
        <ChoicesButton
          contract={contracts[1]}
          renderLink={(_id, name) => <div>{name}</div>}
        />
      </ExhibitWrapper>
      <ExhibitWrapper>
        <ChoicesButton
          contract={contracts[2]}
          renderLink={(_id, name) => <div>{name}</div>}
        />
      </ExhibitWrapper>
    </Grid>
  </Section>
);
