// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import styled, { ThemeInterface, withTheme } from "../theme";

/**
 * This Icon coomponent assumes an icon font is loaded and associated with
 * global, prefixed classnames. The default prefix is `icon-`, but can be
 * customised by setting `iconPrefix` in the theme. That is, this component
 * library does not ship with icons -- it is up to the app to provide the
 * necessary ones. The icons should be a superset of the names defined by the
 * IconName union type.
 *
 * The exported component is untyped (accepting any string) but can be
 * specialised with the exported `IconType<T>`.
 */

export type IconName =
  | "chevron-right"
  | "chevron-down"
  | "sort-asc"
  | "sort-desc";

const IconSpan = styled.i`
  color: inherit;
  line-height: 1;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
`;

export interface IconProps<T> {
  name: T;
  className?: string;
  theme?: ThemeInterface;
}

export type IconType<T> = React.FC<IconProps<T>>;

export const UntypedIcon: IconType<string> = withTheme(
  ({ theme, name, className }) => {
    const prefix = theme && theme.iconPrefix;
    const classNames = [`${prefix}${name}`, className].join(" ");
    return <IconSpan className={classNames} />;
  },
);

export default UntypedIcon;
