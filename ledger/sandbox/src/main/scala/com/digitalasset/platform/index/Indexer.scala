// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import akka.Done
import com.daml.ledger.participant.state.v1.ReadService

import scala.concurrent.Future

/**
  * Establishes a feed for an IndexService implementation
  */
trait Indexer {

  /**
    * Subscribes to an instance of ReadService.
    *
    * @param readService the ReadService to subscribe to
    * @return a handle of IndexFeedHandle or a failed Future
    */
  def subscribe(readService: ReadService): Future[IndexFeedHandle]

}

/** A handle with which one can stop a running indexing feed. */
trait IndexFeedHandle {

  /**
    * Asynchronously stops the running index feed.
    *
    * @return Done if success or a failed future in case of an error.
    */
  def stop(): Future[Done]

  /**
    * A future that completes when the feed terminates.
    *
    * @return Done if the feed terminates normally or a failed future in case of an error during feed processing.
    */
  def completed(): Future[Done]
}
