// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import com.daml.ledger.participant.state.v1.ReadService
import com.daml.resources.ResourceOwner

import scala.concurrent.Future

/**
  * Establishes a feed for an IndexService implementation
  */
trait Indexer {

  /**
    * A resource owner that, when acquired, subscribes to an instance of ReadService.
    *
    * @param readService the ReadService to subscribe to
    * @return a handle of IndexFeedHandle or a failed Future
    */
  def subscription(readService: ReadService): ResourceOwner[IndexFeedHandle]

}

/** A handle with which one can stop a running indexing feed. */
trait IndexFeedHandle {

  /**
    * A future that completes when the feed terminates.
    *
    * @return Nothing if the feed terminates normally, or a failed future in case of an error during
    *         feed processing.
    */
  def completed(): Future[Unit]
}
