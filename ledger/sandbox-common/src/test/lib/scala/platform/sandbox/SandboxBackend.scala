// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import java.util.UUID

import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.platform.sandbox.services.DbInfo
import com.daml.platform.store.DbType
import com.daml.testing.postgresql.PostgresResource

object SandboxBackend {

  trait Postgresql {
    this: AbstractSandboxFixture =>

    override protected final def database: Option[ResourceOwner[DbInfo]] =
      Some(Postgresql.owner)
  }

  object Postgresql {

    def owner: ResourceOwner[DbInfo] =
      PostgresResource
        .owner[ResourceContext]()
        .map(database => DbInfo(database.url, DbType.Postgres))

  }

  trait H2Database {
    this: AbstractSandboxFixture =>

    override protected final def database: Option[ResourceOwner[DbInfo]] =
      Some(H2Database.owner)
  }

  object H2Database {

    def owner: ResourceOwner[DbInfo] =
      ResourceOwner.forValue { () =>
        val jdbcUrl = s"jdbc:h2:mem:${UUID.randomUUID().toString};db_close_delay=-1"
        DbInfo(jdbcUrl, DbType.H2Database)
      }

  }

}
