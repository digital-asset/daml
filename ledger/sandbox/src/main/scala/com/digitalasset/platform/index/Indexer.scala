// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import akka.Done
import com.daml.ledger.participant.state.v2.ReadService

import scala.concurrent.Future

/**
  * Establishes a feed for an IndexService implementation
  */
trait Indexer {

  /**
    * Subscribes to an instance of ReadService.
    *
    * @param readService the ReadService to subscribe to
    * @param onError     callback to signal error during feed processing
    * @param onComplete  callback fired only once at normal feed termination.
    * @return a handle of IndexFeedHandle or a failed Future
    */
  def subscribe(
      readService: ReadService,
      onError: Throwable => Unit,
      onComplete: () => Unit): Future[IndexFeedHandle]

}

/** A handle with which one can stop a running indexing feed. */
trait IndexFeedHandle {

  /**
    * Asynchronously stops the running index feed.
    *
    * @return Done if success or a failed future in case of an error.
    */
  def stop(): Future[Done]
}
