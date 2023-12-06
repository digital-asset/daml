// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.testing.postgresql

final case class PostgresServer(
    hostName: String,
    port: Int,
    userName: String,
    password: String,
    baseDatabase: String,
)
