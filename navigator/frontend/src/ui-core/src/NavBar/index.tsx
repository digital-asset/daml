// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { default as styled, hardcodedStyle } from "../theme";

export interface Props {
  children: (React.ReactChild | null | undefined)[];
  logo: React.ReactNode;
}

// The left margin is to align the nav bar with text inside sidebar links
const Wrapper = styled.nav`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-left: ${hardcodedStyle.buttonFontSize};
`;

const Left = styled.div`
  display: flex;
  justify-content: flex-start;
  align-items: center;
  flex: 0;
  height: calc(
    ${hardcodedStyle.buttonTextHeight} + 2 *
      ${({ theme }) => theme.buttonPadding[0]}
  );
  min-width: calc(${hardcodedStyle.sidebarWidth} - 1rem);
  width: calc(${hardcodedStyle.sidebarWidth} - 1rem);
`;

const Right = styled.div`
  display: flex;
  justify-content: flex-end;
  align-items: center;
  flex: 1;
`;

/**
 * The <NavBar> component renders a default navigation bar, consisting of:
 * - A left-aligned logo, the height of a default button
 * - A right-aligned list of custom controls
 */
const NavBar: React.FC<Props> = ({ logo, children }: Props) => (
  <Wrapper>
    <Left>{logo}</Left>
    <Right>{children}</Right>
  </Wrapper>
);

export default NavBar;
