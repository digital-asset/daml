// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import com.digitalasset.resources.ResourceOwner
import com.digitalasset.testing.postgresql.PostgresResource

class ScenarioLoadingITPostgres extends ScenarioLoadingITBase {
  override protected def database: Option[ResourceOwner[String]] =
    Some(PostgresResource.owner().map(_.jdbcUrl))
}
