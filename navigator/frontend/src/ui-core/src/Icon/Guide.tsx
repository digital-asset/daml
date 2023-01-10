// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { Section } from "../Guide";
import { UntypedIcon } from "../Icon";
import styled from "../theme";

/**
 * This icon guide component works with untyped icons.
 *
 * The exported guide component is untyped (accepting any string name as an icon
 * name) but can be specialised with the exported `IconGuideType<T>`.
 */

const Grid = styled.div`
  display: flex;
  justify-content: flex-start;
  flex-wrap: wrap;
`;

const PaddedIcon = styled(UntypedIcon)`
  padding: 1rem;
`;

const ExhibitWrapper = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;
  margin: 0.5rem;
  margin-bottom: 1rem;
  width: 7rem;
`;

const NameInput = styled.input`
  font-family: monospace;
  text-align: center;
  width: 100%;
  border: none;
  background-color: $color-field-background;
  padding: 0 0.25rem;
`;

interface ExhibitProps {
  name: string;
}

const Exhibit = ({ name }: ExhibitProps) => (
  <ExhibitWrapper>
    <PaddedIcon name={name} />
    <NameInput type="text" value={name} disabled={true} />
  </ExhibitWrapper>
);

export interface IconGuideProps<T> {
  names: T[];
}

const description = `
These are the icons assumed to exist by the Navigator components. Each app must
provide these themselves with global, prefixed classes with the names specified
here. The default prefix is \`icon-\` but this can be customised by setting the
\`iconPrefix\` in the theme.
`;

// Define type for exported IconGuide component such that clients can specialise
// it.
export type IconGuideType<T> = React.StatelessComponent<IconGuideProps<T>>;

export default ({ names }: IconGuideProps<string>): JSX.Element => (
  <Section title="Icons" description={description}>
    <Grid>
      {names.map(name => (
        <Exhibit key={name} name={name} />
      ))}
    </Grid>
  </Section>
);
