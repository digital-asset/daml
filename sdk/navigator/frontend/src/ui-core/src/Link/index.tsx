// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { default as styled } from "../theme";

const StyledA = styled.a`
  color: ${({ theme }) => theme.colorPrimary[0]};
  text-decoration: none;
`;

/**
 * This component represents a single-page application link. In particular it
 * handles modifier-key clicks and prevents default action in favour of an
 * onClick handler.
 */

export type HrefTarget = "_blank" | "_self" | "_parent" | "_top";

export interface Props {
  href: string;
  onClick(e: React.MouseEvent<HTMLAnchorElement>): void;
  className?: string;
  target?: HrefTarget;
  children?: React.ReactNode;
}

export default class Link extends React.Component<Props, {}> {
  constructor(props: Props) {
    super(props);
    this.click = this.click.bind(this);
  }

  click(e: React.MouseEvent<HTMLAnchorElement>): void {
    e.stopPropagation();
    if (e.metaKey || e.shiftKey || e.ctrlKey || e.altKey) {
      return;
    } else {
      e.preventDefault();
      this.props.onClick(e);
    }
  }

  render(): React.ReactElement<HTMLAnchorElement> {
    const { href, className, target, children } = this.props;

    return (
      <StyledA
        onClick={this.click}
        href={href}
        target={target}
        className={className}>
        {children}
      </StyledA>
    );
  }
}
