// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import Button, { ButtonType } from "../Button";
import { Section } from "../Guide";
import UntypedIcon, { IconName, IconType } from "../Icon";
import styled from "../theme";

const Icon = UntypedIcon as IconType<IconName>;

function logClick() {
  console.log("CLICK!");
}

const ButtonRow = ({ type }: { type: ButtonType }) => (
  <tr>
    <td>
      <code>{type}</code>
    </td>
    <td>
      <Button type={type} onClick={logClick}>
        Button
      </Button>
    </td>
    <td>
      <Button type={type} onClick={logClick}>
        Button <i>with</i> icon <Icon name="chevron-right" />
      </Button>
    </td>
    <td>
      <Button type={type} disabled={true} onClick={logClick}>
        Disabled
      </Button>
    </td>
  </tr>
);

const NavTable = styled.table`
  background: ${({ theme }) => theme.documentBackground};
`;

export default (): JSX.Element => (
  <Section title="Button" description="Different types of buttons.">
    <table>
      <tbody>
        <ButtonRow type="main" />
        <ButtonRow type="warning" />
        <ButtonRow type="danger" />
        <ButtonRow type="transparent" />
      </tbody>
    </table>
    <NavTable>
      <tbody>
        <ButtonRow type="nav-primary" />
        <ButtonRow type="nav-secondary" />
        <ButtonRow type="nav-transparent" />
      </tbody>
    </NavTable>
  </Section>
);
