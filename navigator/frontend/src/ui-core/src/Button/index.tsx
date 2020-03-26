// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as Color from 'color';
import * as React from 'react';
import { default as styled, hardcodedStyle, ThemeInterface } from '../theme';
export { StyledComponentClass } from 'styled-components';
import { StyledComponentClass } from 'styled-components';

export function applyColors([bg, fg]: [string, string]) {
  return `
    color: ${fg};
    background: ${bg};
    &:hover { background: ${hover(bg)}; }
    &:active { background: ${active(bg)}; }
  `;
}
export function getColors(theme: ThemeInterface, type?: ButtonType): string {
  switch (type) {
    case 'danger': return applyColors(theme.colorDanger);
    case 'warning': return applyColors(theme.colorWarning);
    case 'nav-primary': return applyColors(theme.colorNavPrimary);
    case 'nav-secondary': return applyColors(theme.colorNavSecondary);
    case 'nav-transparent': return `
      ${applyColors(theme.colorNavPrimary)};
      background: transparent;
      &:hover { background: transparent; color: ${theme.colorPrimary[0]}; }
      &:active { background: transparent; color: ${theme.colorPrimary[0]}; }
    `;
    case 'transparent': return `
      color: ${theme.colorForeground};
      background: transparent;
      &:hover { background: ${theme.colorShade}; }
      &:active { background: ${theme.colorFaded}; }
    `;
    case 'inverted-primary': return `
      color: ${theme.colorPrimary[0]};
      background: ${theme.colorBackground};
      &:hover {
        box-shadow: ${hardcodedStyle.buttonShadow};
      }
    `;
    default: return applyColors(theme.colorPrimary);
  }
}
function getPadding(theme: ThemeInterface, type?: ButtonType): string {
  switch (type) {
    case 'minimal': return '0.5rem';
    default: return theme.buttonPadding.join(' ');
  }
}

function getBoxShadow(_: ThemeInterface, type?: ButtonType): string {
  switch (type) {
    case 'nav-primary': return 'none';
    case 'nav-transparent': return 'none';
    default: return hardcodedStyle.buttonShadow;
  }
}

function hover(color: string) {
  // tslint:disable-next-line:no-magic-numbers
  return Color(color).darken(20 / 100).hex().toString();
}

function active(color: string) {
  // tslint:disable-next-line:no-magic-numbers
  return Color(color).darken(50 / 100).hex().toString();
}

const TextNode = styled.span`
  padding-left: 0.25rem;
  padding-right: 0.25rem;
  &:first-child { padding-left: initial; }
  &:last-child { padding-right: initial; }
`;

// tslint:disable-next-line:no-any
const wrapStringInSpan = (children: any) =>
  React.Children.map(children, (child: JSX.Element | string) =>
    (typeof child === 'string' ? <TextNode>{child}</TextNode> : child));

export type ButtonType
  = 'main'
  | 'warning'
  | 'danger'
  | 'transparent'
  | 'minimal'
  | 'nav-transparent'
  | 'nav-primary'
  | 'nav-secondary'
  | 'inverted-primary';

export interface Props {
  onClick(e: React.MouseEvent<HTMLButtonElement>): void;
  type?: ButtonType;
  disabled?: boolean;
  className?: string;
  autoFocus?: boolean;
  tabIndex?: number;
  // tslint:disable-next-line:no-any
  children?: any;
  theme?: ThemeInterface;
}

const Button = ({ onClick, disabled, className, children, autoFocus, tabIndex }: Props) => (
  <button
    onClick={onClick}
    disabled={disabled}
    className={className}
    type="button"
    autoFocus={autoFocus}
    tabIndex={tabIndex}
  >
    {wrapStringInSpan(children)}
  </button>
);

// We wrap the functional button so we can style conditionally on type.

const B: StyledComponentClass<Props, ThemeInterface, Props> = styled(Button)`
  display: flex;
  flex-wrap: nowrap;
  white-space: nowrap;
  justify-content: center;
  align-items: center;
  padding: ${({ theme, type }) => getPadding(theme, type)};
  border-radius: ${({ theme }) => theme.buttonRadius};
  border: none;
  cursor: pointer;
  font-size: ${ hardcodedStyle.buttonFontSize };
  box-shadow: ${({ theme, type }) => getBoxShadow(theme, type)};
  ${({ theme, type }) => getColors(theme, type)}
  &:disabled {
    cursor: not-allowed;
    opacity: 0.5;
    color: ${({ theme }) => theme.colorFaded};
    &, &:hover {
      background: ${({ theme }) => theme.colorShade};
    }
  }
  &:focus { outline: none; }
  margin: 6px 2px;
`;

export default B;
