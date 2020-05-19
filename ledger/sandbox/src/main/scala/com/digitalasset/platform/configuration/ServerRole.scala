// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

/**
  * Used to disambiguate thread pool names.
  *
  * This is necessary because Hikari connection pools use the pool name when registering metrics.
  * If we were to register two connection pools with the same names with a single metrics registry,
  * the second would fail with an exception.
  */
sealed trait ServerRole {
  val threadPoolSuffix: String
}

object ServerRole {

  object ApiServer extends ServerRole {
    override val threadPoolSuffix: String = "api-server"
  }

  object Indexer extends ServerRole {
    override val threadPoolSuffix: String = "indexer"
  }

  object IndexMigrations extends ServerRole {
    override val threadPoolSuffix: String = "migrations"
  }

  object Sandbox extends ServerRole {
    override val threadPoolSuffix: String = "sandbox"
  }

  final case class Testing(testClass: Class[_]) extends ServerRole {
    override val threadPoolSuffix: String = testClass.getSimpleName.toLowerCase
  }

}
