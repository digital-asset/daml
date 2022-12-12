// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ChoicesButton, ContractColumn } from "@da/ui-core";
import * as React from "react";
import Link from "../../components/Link";
import * as Routes from "../../routes";
import { Contract } from "./data";

import { removePkgIdFromContractTypeId } from "@da/ui-core/lib/api/IdentifierShortening";

export const columns: ContractColumn<Contract>[] = [
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
  {
    key: "template.id",
    title: "Template ID",
    extractCellData: ({ template }: Contract) => template.id,
    createCell: ({ cellData }) => <span>{cellData}</span>,
    sortable: true,
    width: 200,
    weight: 3,
    alignment: "left",
  },
  {
    key: "time",
    title: "Time",
    extractCellData: (contract: Contract) =>
      contract.createEvent.transaction.effectiveAt,
    createCell: ({ cellData }) => <span>{cellData}</span>,
    sortable: true,
    width: 180,
    weight: 0,
    alignment: "left",
  },
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
            <div>
              {inheritedInterface ? (
                <>
                  <strong>
                    {removePkgIdFromContractTypeId(inheritedInterface) + ":"}
                  </strong>{" "}
                  {name}{" "}
                </>
              ) : (
                name
              )}
            </div>
          </Link>
        )}
      />
    ),
    sortable: false,
    width: 120,
    weight: 0,
    alignment: "right",
  },
];

export default columns;
