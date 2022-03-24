// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection

import com.daml.platform.store.backend.ResetStorageBackend

object MResetStorageBackend extends ResetStorageBackend {
  override def resetAll(connection: Connection): Unit = MStore.update(connection)(_ => MStore())
}
