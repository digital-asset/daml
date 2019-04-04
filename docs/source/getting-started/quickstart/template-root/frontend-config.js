// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

export const version = {
  schema: 'navigator-config',
  major: 1,
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
          value: "Iou.Iou",
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
        key: "owner",
        title: "Owner",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.owner
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "ccy",
        title: "Currency",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.currency
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "amount",
        title: "Amount",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.amount
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
          value: "Iou.Iou",
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
        key: "issuer",
        title: "Issuer",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.issuer
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "ccy",
        title: "Currency",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.currency
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "amount",
        title: "Amount",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.amount
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
          value: "Iou.IouTransfer",
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
        key: "sender",
        title: "Sender",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.iou.owner
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "receiver",
        title: "Receiver",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.newOwner
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "issuer",
        title: "Issuer",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.iou.issuer
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "ccy",
        title: "Currency",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.iou.currency
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "amount",
        title: "Amount",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.iou.amount
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
          value: "IouTrade.IouTrade@",
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
        key: "buyer",
        title: "Buyer",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.buyer
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "seller",
        title: "Seller",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.seller
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "baseIssuer",
        title: "Base Issuer",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.baseIssuer
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "baseccy",
        title: "Base Currency",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.baseCurrency
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "baseamount",
        title: "Base Amount",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.baseAmount
        }),
        sortable: true,
        width: 80,
        weight: 3,
        alignment: "left"
      },
      {
        key: "quoteIssuer",
        title: "Quote Issuer",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.quoteIssuer
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "quoteccy",
        title: "Quote Currency",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.quoteCurrency
        }),
        sortable: true,
        width: 80,
        weight: 0,
        alignment: "left"
      },
      {
        key: "quoteamount",
        title: "Quote Amount",
        createCell: ({rowData}) => ({
          type: "text",
          value: rowData.argument.quoteAmount
        }),
        sortable: true,
        width: 80,
        weight: 3,
        alignment: "left"
      },
    ]
  }
})