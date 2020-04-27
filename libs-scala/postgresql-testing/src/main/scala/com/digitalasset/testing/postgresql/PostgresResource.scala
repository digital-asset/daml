// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.postgresql

import com.daml.resources.{Resource, ResourceOwner}

import scala.concurrent.{ExecutionContext, Future}

object PostgresResource {
  def owner(): ResourceOwner[PostgresFixture] =
    new ResourceOwner[PostgresFixture] with PostgresAround {
      override def acquire()(
          implicit executionContext: ExecutionContext
      ): Resource[PostgresFixture] =
        Resource(Future {
          startEphemeralPostgres()
          postgresFixture
        })(_ => Future(stopAndCleanUpPostgres()))
    }
}
