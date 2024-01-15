// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import Icon from "../Icon";
import styled from "../theme";

export interface Props {
  className?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  children?: any;
}

const StyledList = styled.ul`
  display: flex;
  flex-flow: row nowrap;
  align-items: center;
  align-content: flex-start;
  justify-content: flex-start;
  padding: 0;
`;

const Divider = styled(Icon)`
  display: flex;
  width: 2em;
  min-width: 2em;
  height: 2em;
  align-items: center;
  justify-content: center;
  padding-top: 0.2em;
  padding-left: 0.1em;
  font-size: 0.8em;
  border-radius: 50%;
  margin: 0 0.25em;
  color: ${({ theme }) => theme.colorFaded};
  background: transparent;
`;

const Breadcrumbs = (props: Props): JSX.Element => {
  const { className, children } = props;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const parts: any[] = [];
  React.Children.forEach(children, (child, idx) => {
    if (idx > 0) {
      parts.push(<Divider key={`divider-${idx}`} name="chevron-right" />);
    }
    parts.push(child);
  });
  return <StyledList className={className}>{parts}</StyledList>;
};

export default Breadcrumbs;
