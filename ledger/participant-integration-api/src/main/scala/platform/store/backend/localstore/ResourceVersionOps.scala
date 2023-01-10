// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.localstore

import java.sql.Connection

trait ResourceVersionOps {
  def compareAndIncreaseResourceVersion(
      internalId: Int,
      expectedResourceVersion: Long,
  )(connection: Connection): Boolean

  def increaseResourceVersion(
      internalId: Int
  )(connection: Connection): Boolean
}
