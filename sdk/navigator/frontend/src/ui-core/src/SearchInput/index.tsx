// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { StyledTextInput } from "../Input";

const DEBOUNCE_TIME_DEFAULT = 500;

export interface Props {
  onChange(value: string): void;
  className?: string;
  initialValue?: string;
  debounceTime?: number;
  placeholder?: string;
  disabled?: boolean;
  focusAfterMount?: boolean;
}

export interface State {
  value: string;
}

export default class SearchInput extends React.Component<Props, State> {
  private searchField: HTMLInputElement;
  private delayTimer: number;

  constructor(props: Props) {
    super(props);
    this.immediateChange = this.immediateChange.bind(this);
    this.state = { value: this.props.initialValue || "" };
  }

  immediateChange(event: React.ChangeEvent<HTMLInputElement>): void {
    event.preventDefault();
    const value = event.target.value;
    this.setState({ value });
    clearTimeout(this.delayTimer);
    this.delayTimer = window.setTimeout(() => {
      this.props.onChange(value);
    }, this.props.debounceTime || DEBOUNCE_TIME_DEFAULT);
  }

  render(): JSX.Element {
    const { className, placeholder = "", disabled = false } = this.props;

    return (
      <StyledTextInput
        innerRef={input => {
          this.searchField = input;
        }}
        className={className}
        type="search"
        disabled={disabled}
        placeholder={placeholder}
        value={this.state.value}
        onChange={this.immediateChange}
      />
    );
  }

  componentDidMount(): void {
    if (this.props.focusAfterMount) {
      this.searchField.focus();
    }
  }
}
