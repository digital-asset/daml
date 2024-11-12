// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.util.PekkoUtil.{Commit, FutureQueueConsumer}

import scala.concurrent.Future

package object indexer {

  /** Indexer is a factory for indexing. Future[Unit] is the completion Future, as it completes indexing is completed with results accordingly (Success/Failure)
    */
  type Indexer = Boolean => Commit => Future[FutureQueueConsumer[Update]]
}
