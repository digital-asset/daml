// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.postgresql

import com.daml.ports.Port

final case class PostgresServer(
    hostName: String,
    port: Port,
    userName: String,
    password: String,
)
