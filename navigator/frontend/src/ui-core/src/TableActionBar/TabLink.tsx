// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from 'react';
import UntypedIcon from '../Icon';
import { default as styled, hardcodedStyle, ThemeInterface } from '../theme';
import Truncate from '../Truncate';
export { StyledComponentClass } from 'styled-components';
import { StyledComponentClass } from 'styled-components';

// This is a bit messy and is probably possible to clean up, but the idea is
// that we export a component factory that takes the outer component (usually a
// link of some sort) and returns a version of it that looks like a tab link.

export interface Props {
  title: string;
  icon?: string;
  count?: number;
  isActive?: boolean;
}

const Group = styled.div`
  display: flex;
  align-items: center;
`;

const MainIcon = styled(UntypedIcon)`
  margin-right: 0.5em;
`;

const Count = styled.span`
  ${hardcodedStyle.smallNumberIcon}
  color: ${({ theme }) => theme.colorSecondary[1]};
  background-color: ${({ theme }) => theme.colorSecondary[0]};
`;

const Underline = (props: {isActive?: boolean, className?: string}) =>
  (<div className={props.className}/>);

const StyledUnderline = styled(Underline)`
  border-bottom: ${(props) => props.isActive ? '2px solid' : '2px solid transparent'};
  height: 0;
  width: 100%;
  position: relative;
  top: 0.25rem;
`

export function makeTabLink<P extends {}>(Link: React.ComponentClass<P>) {

  // First create the component with the required API. This uses the Link
  // component as the outer wrapper.

  const A = (props: Props & P) => {
    // The no-any is a hack because of a TypeScript issue
    // (https://github.com/Microsoft/TypeScript/pull/13288)
    // tslint:disable-next-line:no-any
    const { title, icon, count, isActive, ...params } = props as any;
    const iconEl = icon ? <MainIcon name={icon} /> : null;
    const countEl = count !== undefined ? <Count>{count}</Count> : null;
    return (
      <Link {...params}>
        <Group>
          {iconEl}
          <Truncate>{title}</Truncate>
          {countEl}
        </Group>
        <StyledUnderline isActive={isActive}/>
      </Link>
    );
  };

  // Then style this (note that we're using isActive for conditional styling).
  const B: StyledComponentClass<Props & P, ThemeInterface, Props & P> =
    styled(A)`
    color: ${({theme}) => theme.colorPrimary[1]};
    margin-right: calc(7 * ${hardcodedStyle.actionBarElementMargin});
  `;

  // And return the styled version.
  return B;
};
