// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import Button from "../Button";
import { Section } from "../Guide";
import Popover, { placementStrings, PopoverPosition, Props } from "../Popover";
import styled from "../theme";

const description = `
A popover accepts a single child as the popover target
(i.e., the element relative to which the popover is positioned).
The content of the popover is specified using the 'popoverContent'
property.
The popover is a stateless (uncontrolled component), use the
'isOpen' property to specify whether the popover is visible.
`;

export type State = Props;

const PopoverContent = styled.div`
  width: 350px;
  min-height: 30px;
  padding: 1px 1.33em;
`;

const CenteredDiv = styled.div`
  display: flex;
  justify-content: center;
  overflow: hidden;
`;

const popoverContent = (
  <PopoverContent>
    <div>
      <h4>Popover title</h4>
      <p>
        Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod
        tempor incididunt ut labore et dolore magna aliqua.
      </p>
    </div>
  </PopoverContent>
);

const popoverTarget = (
  <Button
    onClick={() => {
      return;
    }}>
    Click to toggle
  </Button>
);

export default class PopoverGuide extends React.Component<{}, State> {
  constructor() {
    super({});
    this.state = {
      isOpen: false,
      arrow: true,
      content: popoverContent,
      target: popoverTarget,
      position: "right",
      onInteraction: (type, open) => {
        console.log(`Popover interaction: {type: ${type}, open: ${open}}`);
        this.setState({
          ...this.state,
          isOpen: open,
        });
      },
    };
  }

  setPlacement(value: string): void {
    this.setState({
      ...this.state,
      position: value as PopoverPosition,
    });
  }

  setArrow(value: boolean): void {
    this.setState({
      ...this.state,
      arrow: value,
    });
  }

  render(): JSX.Element {
    return (
      <Section title="Popover" description={description}>
        <label>
          <p>Option: Placement</p>
          <select
            value={this.state.position}
            onChange={e => {
              this.setPlacement(e.target.value);
            }}>
            {placementStrings.map((str, index) => (
              <option key={index} value={str}>
                {str}
              </option>
            ))}
          </select>
        </label>
        <br />
        <label>
          <p>Option: Arrow</p>
          <input
            type="checkbox"
            value="on"
            onChange={e => {
              this.setArrow(e.target.checked);
            }}
            defaultChecked={this.state.arrow}
          />
          &nbsp; Show arrow
        </label>
        <br />
        <label>
          <p>Result</p>
          <CenteredDiv>
            <Popover {...this.state} />
          </CenteredDiv>
        </label>
      </Section>
    );
  }
}
