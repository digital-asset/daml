// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0


import { defaultTheme, ThemeInterface } from '@da/ui-core';
import * as styled from 'styled-components';

export const theme: ThemeInterface = defaultTheme;

export type StyledFunction<T> = styled.ThemedStyledFunction<T, ThemeInterface>;

export function withProps<T, U extends HTMLElement = HTMLElement>(
  styledFunction: StyledFunction<React.HTMLProps<U>>,
): StyledFunction<T & React.HTMLProps<U>> {
  return styledFunction;
}
