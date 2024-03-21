// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import {
  DamlLfDefDataType,
  DamlLfIdentifier,
  DamlLfRecord,
  DamlLfTypeCon,
} from "../api/DamlLfType";
import * as DamlLfTypeF from "../api/DamlLfType";
import { DamlLfValue } from "../api/DamlLfValue";
import * as DamlLfValueF from "../api/DamlLfValue";
import { Section } from "../Guide";
import {
  ContractIdProvider,
  default as ParameterForm,
  ParameterFormContract,
  TypeProvider,
} from "./index";

const contracts: ParameterFormContract[] = [
  {
    id: "c1230001",
    createEvent: {
      transaction: {
        effectiveAt: "2017-06-05T11:34:21Z",
      },
    },
    archiveEvent: null,
    template: {
      id: "t90001111",
    },
  },
  {
    id: "c1230002",
    createEvent: {
      transaction: {
        effectiveAt: "2017-06-05T11:35:26Z",
      },
    },
    archiveEvent: null,
    template: {
      id: "t90002222",
    },
  },
  {
    id: "c1230003",
    createEvent: {
      transaction: {
        effectiveAt: "2017-06-05T11:35:26Z",
      },
    },
    archiveEvent: {
      transaction: {
        effectiveAt: "2017-06-05T11:35:26Z",
      },
    },
    template: {
      id: "t90003333",
    },
  },
] as ParameterFormContract[];

const contractIdProvider: ContractIdProvider = {
  fetchContracts(
    filter: string,
    onResult: (result: ParameterFormContract[]) => void,
  ): void {
    onResult(
      contracts.filter(
        c => c.id.indexOf(filter) >= 0 || c.template.id.indexOf(filter) >= 0,
      ),
    );
  },
};

const exampleRecordId: DamlLfIdentifier = {
  name: "exampleRecord",
  module: "module",
  package: "package",
};
const exampleRecord: DamlLfRecord = {
  type: "record",
  fields: [
    { name: "text parameter", value: DamlLfTypeF.text() },
    { name: "party parameter", value: DamlLfTypeF.party() },
    { name: "contractId parameter", value: DamlLfTypeF.contractid() },
    { name: "numeric parameter", value: DamlLfTypeF.numeric(10) },
    { name: "integer parameter", value: DamlLfTypeF.int64() },
    { name: "time parameter", value: DamlLfTypeF.timestamp() },
    { name: "date parameter", value: DamlLfTypeF.date() },
    { name: "bool parameter", value: DamlLfTypeF.bool() },
  ],
};
const exampleRecordDef: DamlLfDefDataType = {
  dataType: exampleRecord,
  typeVars: [],
};
const exampleRecordTc: DamlLfTypeCon = {
  type: "typecon",
  name: exampleRecordId,
  args: [],
};

const typeProvider: TypeProvider = {
  fetchType(
    id: DamlLfIdentifier,
    onResult: (
      id: DamlLfIdentifier,
      result: DamlLfDefDataType | undefined,
    ) => void,
  ): void {
    if (id.name === exampleRecordId.name) {
      onResult(id, exampleRecordDef);
    } else {
      onResult(id, undefined);
    }
  },
};

export interface State {
  value: DamlLfValue;
}

const description = `
The ParameterForm component displays a form for inputting a Daml-LF value
corresponding to a given Daml-LF type. The argument is supplied by the
client of the component which provides and onChange callback to change
it. To handle submission, the component provides an \`onSubmit\` callback.
`;

export default class ParameterFormGuide extends React.Component<{}, State> {
  constructor() {
    super({});
    this.state = {
      value: DamlLfValueF.record(exampleRecordId, [
        { label: "text parameter", value: DamlLfValueF.text("") },
        { label: "party parameter", value: DamlLfValueF.party("") },
        { label: "contractId parameter", value: DamlLfValueF.contractid("") },
        { label: "numeric parameter", value: DamlLfValueF.numeric("0") },
        { label: "integer parameter", value: DamlLfValueF.int64("0") },
        { label: "time parameter", value: DamlLfValueF.timestamp("") },
        { label: "bool parameter", value: DamlLfValueF.bool(false) },
      ]),
    };
  }

  render(): JSX.Element {
    return (
      <Section title="Parameter form" description={description}>
        <ParameterForm
          disabled={false}
          onChange={(value: DamlLfValue) => {
            this.setState({ value });
          }}
          onSubmit={e => {
            e.preventDefault();
          }}
          parameter={exampleRecordTc}
          argument={this.state.value}
          contractIdProvider={contractIdProvider}
          typeProvider={typeProvider}
        />
      </Section>
    );
  }
}
