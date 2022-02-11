// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fs

import java.nio.file.{Files, Path}

import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ReleasableResource, Resource}

import scala.concurrent.Future

final class TemporaryDirectory[Context: HasExecutionContext](prefix: String)
    extends AbstractResourceOwner[Context, Path] {
  override def acquire()(implicit context: Context): Resource[Context, Path] =
    ReleasableResource(Future {
      Files.createTempDirectory(prefix)
    })(directory =>
      Future {
        Utils.deleteRecursively(directory)
      }
    )
}
