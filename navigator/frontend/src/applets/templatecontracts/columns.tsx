// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ChoicesButton, ContractColumn } from "@da/ui-core";
import {
  DamlLFFieldWithType,
  DamlLfRecord,
} from "@da/ui-core/lib/api/DamlLfType";
import * as DamlLfValueF from "@da/ui-core/lib/api/DamlLfValue";
import * as React from "react";
import Link from "../../components/Link";
import * as Routes from "../../routes";
import { Contract } from "./data";

function formatField(
  field: DamlLFFieldWithType,
  argument: DamlLfValueF.DamlLfValueRecord,
): string {
  const valueField = argument.fields.filter(f => f.label === field.name)[0];
  return valueField
    ? JSON.stringify(DamlLfValueF.toJSON(valueField.value))
    : "???";
}

function makeColumns(param: DamlLfRecord): ContractColumn<Contract>[] {
  return param.fields.map(
    field =>
      ({
        key: `argument.${field.name}`,
        sortable: true,
        title: field.name,
        extractCellData: contract => formatField(field, contract.argument),
        createCell: ({ cellData }) => <span>{cellData}</span>,
        width: 200,
        weight: 1,
        alignment: "left",
      } as ContractColumn<Contract>),
  );
}

export default (param: DamlLfRecord): ContractColumn<Contract>[] => [
  {
    key: "id",
    title: "ID",
    extractCellData: ({ id }: Contract) => id,
    createCell: ({ cellData }) => <span>{cellData}</span>,
    sortable: true,
    width: 80,
    weight: 0,
    alignment: "left",
  },
  ...makeColumns(param),
  {
    key: "choice",
    title: "Choice",
    extractCellData: (contract: Contract) => contract,
    createCell: ({ cellData }) => (
      <ChoicesButton
        contract={cellData}
        renderLink={(id, name, inheritedInterface) => (
          <Link
            route={Routes.contract}
            params={{
              id: encodeURIComponent(id),
              choice: name,
              ifc: inheritedInterface && encodeURIComponent(inheritedInterface),
            }}>
            <div>{name}</div>
          </Link>
        )}
      />
    ),
    sortable: true,
    width: 120,
    weight: 0,
    alignment: "right",
  } as ContractColumn<Contract, Contract>,
];
