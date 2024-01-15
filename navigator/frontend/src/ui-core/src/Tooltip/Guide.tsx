// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import Button from "../Button";
import { Section } from "../Guide";
import styled from "../theme";
import Tooltip from "../Tooltip";

const description = `
A tooltip displays a box with a dropdown shadow,
with an optional arrow.
The position of the tooltip is static (it is positioned like any other element).
Use in combination with Popover to attach the tooltip to
a target element and display it above other content.
`;

const TooltipContentWrapper = styled.div`
  width: 200px;
  min-height: 30px;
  padding: 1px 1.33em;
`;

const CenteredDiv = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
`;

const TooltipContent = () => (
  <TooltipContentWrapper>
    <p>Tooltip</p>
  </TooltipContentWrapper>
);

const TooltipTarget = () => (
  <Button
    onClick={() => {
      return;
    }}>
    Button
  </Button>
);

export default class PopoverGuide extends React.Component {
  constructor() {
    super({});
  }

  render(): JSX.Element {
    return (
      <Section title="Tooltip" description={description}>
        <table>
          <tbody>
            <tr>
              <td>Bottom, no arrow</td>
              <td>
                <TooltipTarget />
                <Tooltip placement="bottom" arrow={false} margin={2}>
                  <TooltipContent />
                </Tooltip>
              </td>
            </tr>
            <tr>
              <td>Right, with arrow</td>
              <td>
                <CenteredDiv>
                  <TooltipTarget />
                  <Tooltip placement="right" arrow={true} margin={2}>
                    <TooltipContent />
                  </Tooltip>
                </CenteredDiv>
              </td>
            </tr>
          </tbody>
        </table>
      </Section>
    );
  }
}
