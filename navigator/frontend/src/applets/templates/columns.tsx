// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { DataColumnConfig, UntypedIcon } from "@da/ui-core";
import * as React from "react";
import Link from "../../components/Link";
import * as Routes from "../../routes";
import { Template } from "./data";

export const columns: DataColumnConfig<Template, {}>[] = [
  {
    key: "id",
    title: "Template ID",
    extractCellData: ({ id }: Template) => id,
    createCell: ({ cellData }) => <span>{cellData}</span>,
    sortable: true,
    width: 200,
    weight: 3,
    alignment: "left",
  },
  {
    key: "template.id",
    title: "# Contracts",
    extractCellData: (template: Template) => template,
    createCell: ({ cellData }) => (
      <Link
        route={Routes.templateContracts}
        params={{ id: cellData.id }}
        key={cellData.id}>
        <span>
          <UntypedIcon name="contract" />
          &nbsp;
          {cellData.contracts.totalCount > 0
            ? `${cellData.contracts.totalCount}`
            : "-"}
        </span>
      </Link>
    ),
    sortable: false,
    width: 120,
    weight: 0,
    alignment: "right",
  } as DataColumnConfig<Template, Template>,
];

export default columns;
