// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services

import com.daml.platform.store.DbType

final case class DbInfo(
    jdbcUrl: String,
    dbType: DbType,
)
