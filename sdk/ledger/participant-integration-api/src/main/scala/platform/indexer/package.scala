// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.ledger.resources.ResourceOwner

import scala.concurrent.Future

package object indexer {

  /** Indexer is a factory for indexing. Future[Unit] is the completion Future, as it completes indexing is completed with results accordingly (Success/Failure)
    * TODO append-only: as doing cleanup scala-doc for the final types around Indexer needs to be improved
    */
  type Indexer = ResourceOwner[Future[Unit]]
}
