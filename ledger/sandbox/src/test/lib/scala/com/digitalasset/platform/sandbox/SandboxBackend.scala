// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox
import java.util.UUID

import com.daml.platform.sandbox.services.DbInfo
import com.daml.platform.store.DbType
import com.daml.resources.ResourceOwner
import com.daml.testing.postgresql.PostgresResource

import scala.util.Success

object SandboxBackend {

  trait Postgresql { this: AbstractSandboxFixture =>
    override protected final def database: Option[ResourceOwner[DbInfo]] =
      Some(PostgresResource.owner().map(database => DbInfo(database.url, DbType.Postgres)))
  }

  trait H2Database { this: AbstractSandboxFixture =>
    private def randomDatabaseName = UUID.randomUUID().toString
    private[this] def jdbcUrl = s"jdbc:h2:mem:$randomDatabaseName;db_close_delay=-1"
    override protected final def database: Option[ResourceOwner[DbInfo]] =
      Some(ResourceOwner.forTry(() => Success(DbInfo(jdbcUrl, DbType.H2Database))))
  }

}
