// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandboxnext.services.reset

import com.digitalasset.platform.sandbox.services.reset.ResetServiceITBase
import com.digitalasset.platform.sandboxnext.SandboxNextFixture
import com.digitalasset.resources.ResourceOwner
import com.digitalasset.testing.postgresql.PostgresResource
import org.scalatest.Ignore

@Ignore
final class ResetServiceOnPostgresqlIT extends ResetServiceITBase with SandboxNextFixture {
  override def spanScaleFactor: Double = super.spanScaleFactor * 4

  override protected def database: Option[ResourceOwner[String]] =
    Some(PostgresResource.owner().map(_.jdbcUrl))
}
