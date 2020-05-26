// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

module.exports = {
  client: {
    addTypename: true,
    service: {
      name: 'navigator-backend',
      localSchemaFile: '../backend/src/test/resources/schema.graphql',
    },
  },
};
