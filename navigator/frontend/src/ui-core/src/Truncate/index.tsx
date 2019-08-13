// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { default as styled, ThemeInterface } from '../theme';
export { StyledComponentClass } from 'styled-components';
import { StyledComponentClass } from 'styled-components';

const Truncate: StyledComponentClass<React.HTMLProps<HTMLSpanElement>,
ThemeInterface, React.HTMLProps<HTMLSpanElement>> = styled.span`
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  display: block;
`

export default Truncate;
