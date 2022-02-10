// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { Section } from "../Guide";
import styled from "../theme";
import Truncate from "../Truncate";

const longText = `Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor
incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation
ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit
in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat
non proident, sunt in culpa qui officia deserunt mollit anim id est laborum`;

const description = `
This component makes sure its text content is limited to a single line, truncating overflowing
text if necessary.
Note that the parent element must have an explicit width (or max-width). Also, the ellipsis is
only rendered if child elements are plain text (other HTML elements are simply cut off).
`;

const Wrapper = styled.div<{ size: number }>`
  max-width: ${props => props.size}px;
  padding: 0.5em;
  border: 1px solid ${({ theme }) => theme.colorFaded};
  border-radius: ${({ theme }) => theme.radiusBorder};
  margin-bottom: 1rem;
`;

export default (): JSX.Element => (
  <Section title="Truncate text" description={description}>
    <Wrapper size={200}>
      <Truncate>{longText}</Truncate>
    </Wrapper>
    <Wrapper size={400}>
      <Truncate>{longText}</Truncate>
    </Wrapper>
  </Section>
);
