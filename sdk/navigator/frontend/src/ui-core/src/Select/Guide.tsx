// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { Section } from "../Guide";
import Select, { Option, Props } from "../Select";

const description = `
An input where the user can select one value out of a given list.
`;

export type State = Props;

const options: Option[] = [
  { label: "Apple", value: "Apple" },
  { label: "Banana", value: "Banana" },
  { label: "Orange", value: "Orange" },
];

export default class SelectGuide extends React.Component<{}, State> {
  constructor() {
    super({});
    this.state = {
      value: options[0].label,
      options,
      onChange: (value: string) =>
        this.setState({
          ...this.state,
          value,
        }),
      minWidth: "200px",
    };
  }

  render(): JSX.Element {
    return (
      <Section title="Select" description={description}>
        <Select {...this.state} />
      </Section>
    );
  }
}
