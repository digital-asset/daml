// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox
import java.util.UUID

import com.daml.platform.sandbox.services.DbInfo
import com.daml.platform.store.DbType
import com.daml.resources.{Resource, ResourceOwner}
import com.daml.testing.postgresql.PostgresResource

import scala.concurrent.ExecutionContext

object SandboxBackend {

  trait Postgresql {
    this: AbstractSandboxFixture =>
    override protected final def database: Option[ResourceOwner[DbInfo]] =
      Some(PostgresResource.owner().map(database => DbInfo(database.url, DbType.Postgres)))
  }

  trait H2Database {
    this: AbstractSandboxFixture =>
    override protected final def database: Option[ResourceOwner[DbInfo]] =
      Some(H2Database.Owner)
  }

  object H2Database {

    object Owner extends ResourceOwner[DbInfo] {
      override def acquire()(implicit executionContext: ExecutionContext): Resource[DbInfo] = {
        val jdbcUrl = s"jdbc:h2:mem:${UUID.randomUUID().toString};db_close_delay=-1"
        Resource.successful(DbInfo(jdbcUrl, DbType.H2Database))
      }
    }

  }

}
