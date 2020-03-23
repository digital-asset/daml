// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import com.digitalasset.platform.store.DbType

final case class DbInfo(
    jdbcUrl: String,
    dbType: DbType,
)
