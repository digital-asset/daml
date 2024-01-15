// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { Section } from "../Guide";
import SearchInput from "../SearchInput";

export interface State {
  value: string;
}

const description = `
These inputs are styled as search inputs and include an optional
debounce delay before the \`onChange\` callback is called.
`;

export default class SearchInputGuide extends React.Component<{}, State> {
  constructor() {
    super({});
    this.state = { value: "Mockingbird" };
  }
  render(): JSX.Element {
    return (
      <Section title="Search and filter inputs" description={description}>
        <SearchInput
          onChange={value => {
            this.setState({ value });
          }}
          placeholder="Placeholder"
          initialValue={this.state.value}
        />
        <p>
          <span>Value seen by parent:</span> {this.state.value}
        </p>
        <SearchInput
          onChange={() => {
            return;
          }}
          placeholder="Placeholder"
          disabled={true}
          initialValue="Hello, world"
        />
      </Section>
    );
  }
}
