// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

trait Table[FROM] {
  def prepareData(in: Vector[FROM]): Array[Array[_]]
  def executeUpdate: Array[Array[_]] => Connection => Unit
}

abstract class BaseTable[FROM](fields: Seq[(String, Field[FROM, _, _])]) extends Table[FROM] {
  override def prepareData(in: Vector[FROM]): Array[Array[_]] =
    fields.view.map(_._2.toArray(in)).toArray
}
