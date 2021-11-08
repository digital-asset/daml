// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer
import com.daml.platform.store.DbType

object AsyncCommitMode {
  import DbType.{AsyncCommitMode, AsynchronousCommit, LocalSynchronousCommit, SynchronousCommit}

  def fromString(mode: String): Either[String, AsyncCommitMode] = mode.toUpperCase() match {
    case SynchronousCommit.setting => Right(SynchronousCommit)
    case LocalSynchronousCommit.setting => Right(LocalSynchronousCommit)
    case AsynchronousCommit.setting => Right(AsynchronousCommit)
    case unknownMode => Left(s"Unsupported synchronous_commit mode: $unknownMode")
  }
}
