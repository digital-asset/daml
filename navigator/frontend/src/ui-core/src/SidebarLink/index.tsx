// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import UntypedIcon from "../Icon";
import { default as styled, hardcodedStyle, ThemeInterface } from "../theme";
import Truncate from "../Truncate";
export { StyledComponent } from "styled-components";
import { StyledComponent } from "styled-components";

// This is a bit messy and is probably possible to clean up, but the idea is
// that we export a component factory that takes the outer component (usually a
// link of some sort) and returns a version of it that looks like a sidebar
// link.

export interface Props {
  title: string;
  icon?: string;
  count?: number;
  isActive?: boolean;
}

const caretWidth = "1.25rem";

const Group = styled.div`
  display: flex;
  width: calc(100% - ${caretWidth});
  align-items: center;
`;

const MainIcon = styled(UntypedIcon)`
  margin-right: 0.5em;
`;

const SmallIcon = styled(UntypedIcon)`
  font-size: 1rem;
  text-align: right;
  width: ${caretWidth};
`;

const FlexTruncate = styled(Truncate)`
  flex: 1;
`;

const Count = styled.span`
  ${hardcodedStyle.smallNumberIcon}
  color: ${({ theme }) => theme.colorPrimary[1]};
  background-color: ${({ theme }) => theme.colorPrimary[0]};
  box-shadow: ${hardcodedStyle.buttonShadow};
  min-width: 1.75rem;
  min-height: 1.75rem;
`;

export function makeSidebarLink<P>(
  Link: React.ComponentType<P>,
): StyledComponent<React.FC<Props & P>, ThemeInterface, Props & P> {
  // First create the component with the required API. This uses the Link
  // component as the outer wrapper.

  const A = (props: Props & P) => {
    // The no-any is a hack because of a TypeScript issue
    // (https://github.com/Microsoft/TypeScript/pull/13288)
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const { title, icon, count, isActive, ...params } = props as any;
    const iconEl = icon ? <MainIcon name={icon} /> : null;
    const countEl = count !== undefined ? <Count>{count}</Count> : null;
    const caretEl = isActive ? <SmallIcon name="chevron-right" /> : null;
    return (
      <Link {...params}>
        <Group>
          {iconEl}
          <FlexTruncate>{title}</FlexTruncate>
          {countEl}
        </Group>
        {caretEl}
      </Link>
    );
  };

  // Then style this (note that we're using isActive for conditional styling).
  const B: StyledComponent<typeof A, ThemeInterface, Props & P> = styled(A)<
    Props & P
  >`
    display: flex;
    justify-content: space-between;
    font-size: ${hardcodedStyle.sidebarFontSize};
    min-height: 2.75rem;
    padding: ${({ theme }) => theme.buttonPadding.join(" ")};
    align-items: center;
    text-decoration: none;
    margin: 0.25em 0;
    color: ${({ theme }) => theme.colorNavPrimary[1]};
    border-radius: ${({ theme }) => theme.buttonRadius};
    background-color: ${({ isActive, theme }) =>
      isActive ? theme.colorNavPrimary[0] : "transparent"};
    &:hover {
      background-color: ${({ theme }) => theme.colorNavPrimary[0]};
    }
  `;

  // And return the styled version.
  return B;
}
