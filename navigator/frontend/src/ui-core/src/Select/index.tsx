// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import Icon from "../Icon";
import Popover from "../Popover";
import styled, { hardcodedStyle } from "../theme";

export interface Option {
  label: string;
  value: string;
}

export interface Props {
  value: string;
  options: Option[];
  onChange(value: string): void;
  disabled?: boolean;
  minWidth?: string;
}

export interface State {
  open: boolean;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const DropdownContainer = styled("ul")<{ minWidth: string }>`
  min-width: ${({ minWidth }) => minWidth};
  padding: 0;
  margin: 0;
  display: flex;
  flex-direction: column;
  max-height: 15rem;
  overflow-y: auto;
  overflow-x: hidden;
  ::-webkit-scrollbar {
    display: none;
  }
`;

const DropdownItem = styled.li`
  display: flex;
  align-items: center;
  height: 2.5rem;
  padding-left: calc(15px - ${hardcodedStyle.tableHoverBorderWidth});
  padding-right: 15px;
  border-left: ${hardcodedStyle.tableHoverBorderWidth} solid transparent;
  border-color: transparent;
  border-radius: 0;
  &:hover {
    cursor: pointer;
    border-left: ${hardcodedStyle.tableHoverBorderWidth} solid
      ${({ theme }) => theme.colorPrimary[0]};
    background-color: ${hardcodedStyle.tableHoverBackgroundColor};
  }
  &:hover:first-child {
    border-top-left-radius: ${({ theme }) => theme.tooltipRadius};
  }
  &:hover:last-child {
    border-bottom-left-radius: ${({ theme }) => theme.tooltipRadius};
  }
`;

const SelectInput = styled.div`
  outline: none;
  display: block;
  border: 0;
  border-bottom: 1px ${({ theme }) => theme.colorFaded} solid;
  border-radius: ${({ theme }) => theme.radiusBorder};
  color: ${({ theme }) => theme.colorForeground};
  background: ${({ theme }) => theme.colorBackground};
  width: 100%;
  height: 2em;
  padding: 0 10px;
  vertical-align: middle;
  line-height: 2em;

  &:hover {
    cursor: pointer;
  }
`;

const RightIcon = styled(Icon)`
  margin-left: 0.5rem;
  font-size: 1rem;
  line-height: 2rem;
  float: right;
`;

/**
 * A <select>-like component.
 */
export default class Select extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);

    this.state = {
      open: false,
    };
  }

  render(): JSX.Element {
    const { value, disabled, options, onChange, minWidth } = this.props;

    return (
      <Popover
        position="bottom-start"
        isOpen={this.state.open}
        arrow={false}
        onInteraction={(type, isOpen) =>
          this.setState({
            open: type === "content" ? false : !disabled && isOpen,
          })
        }
        target={
          <SelectInput>
            {value}
            <RightIcon name="chevron-down" />
          </SelectInput>
        }
        content={
          <DropdownContainer minWidth={minWidth || "1rem"}>
            {options.map(({ label, value: val }) => (
              <DropdownItem key={val} onClick={() => onChange(val)}>
                {label}
              </DropdownItem>
            ))}
          </DropdownContainer>
        }
      />
    );
  }
}
