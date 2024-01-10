// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.testing.postgresql

import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ReleasableResource, Resource}

import scala.concurrent.Future

object PostgresResource {
  def owner[Context: HasExecutionContext](): AbstractResourceOwner[Context, PostgresDatabase] =
    new AbstractResourceOwner[Context, PostgresDatabase] with PostgresAround {
      override def acquire()(implicit context: Context): Resource[Context, PostgresDatabase] =
        ReleasableResource(Future {
          connectToPostgresqlServer()
          createNewRandomDatabase()
        })(_ => Future(disconnectFromPostgresqlServer()))
    }
}
