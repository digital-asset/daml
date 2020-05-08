// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.postgresql

import com.daml.resources.{Resource, ResourceOwner}

import scala.concurrent.{ExecutionContext, Future}

object PostgresResource {
  def owner(): ResourceOwner[JdbcUrl] =
    new ResourceOwner[JdbcUrl] with PostgresAround {
      override def acquire()(
          implicit executionContext: ExecutionContext
      ): Resource[JdbcUrl] =
        Resource(Future {
          startEphemeralPostgres()
          createNewRandomDatabase()
        })(_ => Future(stopAndCleanUpPostgres()))
    }
}
