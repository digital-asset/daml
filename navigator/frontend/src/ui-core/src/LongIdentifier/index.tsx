// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { default as styled } from "../theme";

const Identifier = styled.span`
  cursor: pointer;
  justify-content: space-between;
  padding: 0.5em 1em;
  align-items: center;
  text-decoration: none;
  margin-right: 1rem;
  box-shadow: 0 0 0 1px rgba(16, 22, 26, 0.1), 0 2px 4px rgba(16, 22, 26, 0.2);
  color: ${({ theme }) => theme.colorPrimary[1]};
  &,
  &:hover {
    color: ${({ theme }) => theme.colorSecondary[1]};
  }
  border-radius: ${_ => "999rem"};
  background-color: ${({ theme }) => theme.colorPrimary[0]};
  &:hover {
    background-color: ${({ theme }) => theme.colorSecondary[0]};
  }
`;

const LongIdentifier: React.JSXElementConstructor<any> = ({
  text,
  identifier,
}: {
  text: string;
  identifier: string;
}) => (
  <Identifier
    onClick={() => navigator.clipboard.writeText(identifier)}
    title="Click to copy the full identifier to the clipboard">
    {text}
  </Identifier>
);

export default LongIdentifier;
