// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.config

sealed trait PostgresStartupMode extends Product with Serializable

object PostgresStartupMode {
  def fromString(value: String): Option[PostgresStartupMode] = {
    Vector(MigrateOnly, MigrateAndStart).find(_.toString == value)
  }
  case object MigrateOnly extends PostgresStartupMode {
    override def toString = "migrate-only"
  }

  case object MigrateAndStart extends PostgresStartupMode {
    override def toString = "migrate-and-start"
  }

}
