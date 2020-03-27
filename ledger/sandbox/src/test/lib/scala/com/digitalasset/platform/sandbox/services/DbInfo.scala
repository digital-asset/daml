// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import com.digitalasset.platform.store.DbType

final case class DbInfo(
    jdbcUrl: String,
    dbType: DbType,
)
