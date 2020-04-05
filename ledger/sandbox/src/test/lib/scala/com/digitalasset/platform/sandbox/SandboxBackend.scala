// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox
import com.daml.platform.sandbox.services.DbInfo
import com.daml.platform.store.DbType
import com.daml.resources.ResourceOwner
import com.daml.testing.postgresql.PostgresResource

object SandboxBackend {

  trait Postgresql { this: AbstractSandboxFixture =>
    override protected final def database: Option[ResourceOwner[DbInfo]] =
      Some(PostgresResource.owner().map(resource => DbInfo(resource.jdbcUrl, DbType.Postgres)))
  }

  trait H2Database { this: AbstractSandboxFixture =>
    private[this] lazy val jdbcUrl = s"jdbc:h2:mem:${getClass.getSimpleName};db_close_delay=-1"
    override protected final def database: Option[ResourceOwner[DbInfo]] =
      Some(ResourceOwner.successful(DbInfo(jdbcUrl, DbType.H2Database)))
  }

}
