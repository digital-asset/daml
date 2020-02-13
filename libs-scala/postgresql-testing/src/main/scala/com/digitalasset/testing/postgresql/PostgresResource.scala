// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.testing.postgresql

import com.digitalasset.resources.{Resource, ResourceOwner}

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
