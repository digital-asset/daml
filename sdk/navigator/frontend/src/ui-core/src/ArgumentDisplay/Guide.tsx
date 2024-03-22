// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { DamlLfValue } from "../api/DamlLfValue";
import * as DamlLfValueF from "../api/DamlLfValue";
import ArgumentDisplay from "../ArgumentDisplay";
import { Section } from "../Guide";

const exampleRecordId = {
  name: "exampleRecord",
  module: "module",
  package: "package",
};
const exampleArgument: DamlLfValue = DamlLfValueF.record(exampleRecordId, [
  { label: "text parameter", value: DamlLfValueF.text("text") },
  { label: "party parameter", value: DamlLfValueF.party("party") },
  { label: "contractId parameter", value: DamlLfValueF.contractid("1:23") },
  { label: "numeric parameter", value: DamlLfValueF.numeric("0") },
  { label: "integer parameter", value: DamlLfValueF.int64("0") },
  {
    label: "time parameter",
    value: DamlLfValueF.timestamp("2017-01-01T11:30:10.001Z"),
  },
  { label: "date parameter", value: DamlLfValueF.date("2017-01-01") },
  { label: "bool parameter", value: DamlLfValueF.bool(false) },
]);

const description = `
The ArgumentDisplay component displays a Daml-LF value.
The argument is supplied by the client of the component.
`;

export default (): JSX.Element => (
  <Section title="Argument display" description={description}>
    <ArgumentDisplay argument={exampleArgument} />
  </Section>
);
