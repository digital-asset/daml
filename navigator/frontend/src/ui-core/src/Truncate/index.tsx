// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { default as styled, ThemeInterface } from "../theme";
export { StyledComponent } from "styled-components";
import { StyledComponent } from "styled-components";

const Truncate: StyledComponent<
  "span",
  ThemeInterface,
  React.HTMLProps<HTMLSpanElement>
> = styled.span`
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  display: block;
`;

export default Truncate;
