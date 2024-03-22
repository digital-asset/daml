// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import Input from "react-autosuggest";
import { StyledTextInput } from "../Input";
import styled, { hardcodedStyle } from "../theme";

const DEBOUNCE_TIME_DEFAULT = 500;

export interface Props<R> {
  className?: string;
  disabled?: boolean;
  onFetchSuggestions(query: string, onResult: (result: R[]) => void): void;
  getSuggestionValue(suggestion: R): string;
  renderSuggestion(suggestion: R): JSX.Element;
  placeholder?: string;
  initialValue?: string;
  debounceTime?: number;
  onChange?(value: string): void;
}

export interface State<R> {
  value: string;
  suggestions: R[];
}

const InputContainer = styled.div`
  display: flex;
`;

const SuggestionsContainer = styled.div`
  position: absolute;
  z-index: 2;
  width: 100%;
  max-height: 200px;
  overflow-x: hidden;
  overflow-y: auto;
  background-color: ${({ theme }) => theme.colorBackground};
  ul {
    list-style-type: none;
    margin: 0;
    padding: 0.25rem 0 0 0;
  }
  li {
    background-color: ${({ theme }) => theme.colorInputBackground};
    padding: 0.375rem ${hardcodedStyle.tableCellHorizontalMargin};
    border-bottom: 1px solid ${({ theme }) => theme.colorFaded};
  }
  li:last-child {
    border-bottom: none;
  }
  li:hover {
    cursor: pointer;
  }
  li.react-autosuggest__suggestion--highlighted {
    border-left: ${hardcodedStyle.tableHoverBorderWidth} solid
      ${({ theme }) => theme.colorPrimary[0]};
    padding-left: calc(
      ${hardcodedStyle.tableCellHorizontalMargin} -
        ${hardcodedStyle.tableHoverBorderWidth}
    );
    background-color: ${hardcodedStyle.tableHoverBackgroundColor};
  }
`;

export default class Autosuggest<R> extends React.Component<
  Props<R>,
  State<R>
> {
  private delayTimer: number;

  constructor(props: Props<R>) {
    super(props);
    const { initialValue = "" } = props;
    this.state = {
      value: initialValue,
      suggestions: [],
    };
    this.onSuggestionFetchRequested =
      this.onSuggestionFetchRequested.bind(this);
    this.onSuggestionClearRequested =
      this.onSuggestionClearRequested.bind(this);
    this.onChange = this.onChange.bind(this);
  }

  componentWillUnmount(): void {
    clearTimeout(this.delayTimer);
  }

  onSuggestionClearRequested(): void {
    this.setState({ suggestions: [] });
  }

  onSuggestionFetchRequested({ value }: { value: string }): void {
    clearTimeout(this.delayTimer);
    this.delayTimer = window.setTimeout(() => {
      this.props.onFetchSuggestions(value, (suggestions: R[]) => {
        this.setState({ suggestions });
      });
    }, this.props.debounceTime || DEBOUNCE_TIME_DEFAULT);
  }

  onChange(_event: {}, { newValue }: { newValue: string }): void {
    this.setState({ value: newValue });
    if (this.props.onChange) {
      this.props.onChange(newValue);
    }
  }

  render(): JSX.Element {
    const { disabled, placeholder } = this.props;
    const { value, suggestions } = this.state;
    const inputProps = {
      placeholder,
      value,
      onChange: this.onChange,
    };
    return (
      <Input
        suggestions={suggestions}
        onSuggestionsFetchRequested={this.onSuggestionFetchRequested}
        onSuggestionsClearRequested={this.onSuggestionClearRequested}
        getSuggestionValue={this.props.getSuggestionValue}
        renderSuggestion={this.props.renderSuggestion}
        inputProps={inputProps}
        shouldRenderSuggestions={() => true}
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        renderInputComponent={(inputComponentProps: any) => {
          const { ref, ...otherInputComponentProps } = inputComponentProps;
          const refCallback = (input: HTMLInputElement) => ref(input);
          return (
            <InputContainer>
              <StyledTextInput
                disabled={disabled}
                innerRef={refCallback}
                {...otherInputComponentProps}
              />
            </InputContainer>
          );
        }}
        renderSuggestionsContainer={({ containerProps, children }) => (
          <SuggestionsContainer {...containerProps}>
            {children}
          </SuggestionsContainer>
        )}
        highlightFirstSuggestion={true}
      />
    );
  }
}
