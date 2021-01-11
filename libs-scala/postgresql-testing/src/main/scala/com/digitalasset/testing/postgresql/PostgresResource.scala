// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.postgresql

import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, Resource}

import scala.concurrent.Future

object PostgresResource {
  def owner[Context: HasExecutionContext](): AbstractResourceOwner[Context, PostgresDatabase] =
    new AbstractResourceOwner[Context, PostgresDatabase] with PostgresAround {
      override def acquire()(implicit context: Context): Resource[Context, PostgresDatabase] =
        Resource[Context].apply(Future {
          connectToPostgresqlServer()
          createNewRandomDatabase()
        })(_ => Future(disconnectFromPostgresqlServer()))
    }
}
