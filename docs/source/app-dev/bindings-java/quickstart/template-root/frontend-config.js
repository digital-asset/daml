// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { DamlLfValue } from '@da/ui-core';

export const version = {
  schema: 'navigator-config',
  major: 2,
  minor: 0,
};

export const customViews = (userId, party, role) => ({
  issued_ious: {
    type: "table-view",
    title: "Issued Ious",
    source: {
      type: "contracts",
      filter: [
        {
          field: "argument.issuer",
          value: party,
        },
        {
          field: "template.id",
          value: "Iou:Iou",
        }
      ],
      search: "",
      sort: [
        {
          field: "id",
          direction: "ASCENDING"
        }
      ]
    },
    columns: [
      {
        key: "id",
        title: "Contract ID",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.id
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.owner",
        title: "Owner",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).owner
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.currency",
        title: "Currency",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).currency
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.amount",
        title: "Amount",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).amount
        }),
        sortable: true,
        width: 80,
        weight: 3,
        alignment: "left"
      }
    ]
  },
  owned_ious: {
    type: "table-view",
    title: "Owned Ious",
    source: {
      type: "contracts",
      filter: [
        {
          field: "argument.owner",
          value: party,
        },
        {
          field: "template.id",
          value: "Iou:Iou",
        }
      ],
      search: "",
      sort: [
        {
          field: "id",
          direction: "ASCENDING"
        }
      ]
    },
    columns: [
      {
        key: "id",
        title: "Contract ID",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.id
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.issuer",
        title: "Issuer",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).issuer
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.currency",
        title: "Currency",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).currency
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.amount",
        title: "Amount",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).amount
        }),
        sortable: true,
        width: 80,
        weight: 3,
        alignment: "left"
      }
    ]
  },
  transfers: {
    type: "table-view",
    title: "Iou Transfers",
    source: {
      type: "contracts",
      filter: [
        {
          field: "template.id",
          value: "Iou:IouTransfer",
        }
      ],
      search: "",
      sort: [
        {
          field: "id",
          direction: "ASCENDING"
        }
      ]
    },
    columns: [
      {
        key: "id",
        title: "Contract ID",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.id
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.iou.owner",
        title: "Sender",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).iou.owner
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.iou.newOwner",
        title: "Receiver",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).newOwner
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.iou.issuer",
        title: "Issuer",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).iou.issuer
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.iou.currency",
        title: "Currency",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).iou.currency
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.iou.amount",
        title: "Amount",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).iou.amount
        }),
        sortable: true,
        width: 80,
        weight: 3,
        alignment: "left"
      }
    ]
  },
  trades: {
    type: "table-view",
    title: "Trades",
    source: {
      type: "contracts",
      filter: [
        {
          field: "template.id",
          value: "IouTrade:IouTrade@",
        }
      ],
      search: "",
      sort: [
        {
          field: "id",
          direction: "ASCENDING"
        }
      ]
    },
    columns: [
      {
        key: "id",
        title: "Contract ID",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.id
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.buyer",
        title: "Buyer",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).buyer
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.seller",
        title: "Seller",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).seller
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.baseIssuer",
        title: "Base Issuer",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).baseIssuer
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.baseCurrency",
        title: "Base Currency",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).baseCurrency
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.baseAmount",
        title: "Base Amount",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).baseAmount
        }),
        sortable: true,
        width: 80,
        weight: 3,
        alignment: "left"
      },
      {
        key: "argument.quoteIssuer",
        title: "Quote Issuer",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).quoteIssuer
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.quoteCurrency",
        title: "Quote Currency",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).quoteCurrency
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "argument.quoteAmount",
        title: "Quote Amount",
        createCell: ({rowData}) => ({
          type: "text",
          value: DamlLfValue.toJSON(rowData.argument).quoteAmount
        }),
        sortable: true,
        width: 80,
        weight: 3,
        alignment: "left"
      },
    ]
  }
})
