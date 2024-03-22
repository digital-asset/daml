// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import styled, { hardcodedStyle } from "../theme";
import Truncate from "../Truncate";

export const StyledLabel: React.FC<
  React.HTMLProps<HTMLLabelElement>
> = styled.label`
  display: block;
  margin: 0 0 15px;
`;

export const StyledLabelText: React.FC<
  React.HTMLProps<HTMLSpanElement>
> = styled(Truncate)`
  color: ${({ theme }) => theme.colorFaded};
  text-transform: ${hardcodedStyle.labelTextTransform};
  font-weight: ${hardcodedStyle.labelFontWeight};
  display: block;
  margin-bottom: 0.25rem;
`;

export interface Props {
  label: string;
  children?: React.ReactChild;
  className?: string;
}

/**
 * Wraps an element to add a label to it.
 */
export const LabeledElement = ({
  label,
  children,
  className,
}: Props): JSX.Element => (
  <StyledLabel key={label} className={className}>
    <StyledLabelText>{label}</StyledLabelText>
    {children}
  </StyledLabel>
);
