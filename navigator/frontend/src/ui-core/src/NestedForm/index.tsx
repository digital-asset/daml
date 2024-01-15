// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import styled, { hardcodedStyle } from "../theme";

export interface NestedFormProps {
  level: number;
  children?: React.ReactNode;
  className?: string;
}

const LeftMargin = styled.div`
  margin-left: ${hardcodedStyle.formNestedIndent};
  padding-left: ${hardcodedStyle.formNestedIndent};
  border-left: ${hardcodedStyle.formNestedLeftBorder};
`;

/**
 * Adds horizontal indentation for nested forms
 */
const NestedForm = ({
  level,
  children,
  className,
}: NestedFormProps): JSX.Element =>
  level > 0 ? (
    <LeftMargin className={className}>{children}</LeftMargin>
  ) : (
    <div className={className}>{children}</div>
  );

export default NestedForm;
