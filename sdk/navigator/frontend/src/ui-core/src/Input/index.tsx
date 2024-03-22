// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import styled from "../theme";

export interface StyledProps {
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  innerRef?(instance: any): void;
}

export const StyledTextInput: React.FC<
  React.HTMLProps<HTMLInputElement> & StyledProps
> = styled.input<StyledProps>`
  width: 100%;
  display: block;
  outline: none;
  border: 0;
  border-bottom: 1px ${({ theme }) => theme.colorFaded} solid;
  border-radius: ${({ theme }) => theme.radiusBorder};
  color: ${({ theme }) => theme.colorForeground};
  background: ${({ theme }) => theme.colorBackground};
  height: 2em;
  padding: 0 10px;
  vertical-align: middle;
  line-height: 2em;
  transition: box-shadow 100ms cubic-bezier(0.4, 1, 0.75, 0.9);

  &:focus {
    border-color: ${({ theme }) => theme.colorPrimary[0]};
  }

  &:disabled {
    color: ${({ theme }) => theme.colorFaded};
    background-color: ${({ theme }) => theme.colorShade};
  }

  &::placeholder {
    color: ${({ theme }) => theme.colorFaded};
  }
`;
