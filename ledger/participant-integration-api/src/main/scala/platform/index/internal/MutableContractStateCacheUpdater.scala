// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index.internal

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{Materializer, RestartSettings}
import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.LoggingContext
import com.daml.platform.store.appendonlydao.events.ContractStateEvent
import com.daml.platform.store.cache.MutableCacheBackedContractStore
import com.daml.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId

import scala.concurrent.duration.{FiniteDuration, _}

private[platform] object MutableContractStateCacheUpdater {
  // Subscribe to the contract state events stream starting at a specific event_offset and event_sequential_id
  type SubscribeToContractStateEvents =
    ((Offset, EventSequentialId)) => Source[ContractStateEvent, NotUsed]

  /** Creates a managed, restartable subscription and updates the mutable contract store cache with
    * contract state events.
    */
  def owner(
      contractStore: MutableCacheBackedContractStore,
      subscribeToContractStateEvents: SubscribeToContractStateEvents,
      minBackoffStreamRestart: FiniteDuration = 100.millis,
  )(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[RestartableManagedStream[ContractStateEvent]] =
    RestartableManagedStream.owner(
      name = "contract state events",
      streamBuilder = () => subscribeToContractStateEvents(contractStore.cacheIndex()),
      restartSettings = RestartSettings(
        minBackoff = minBackoffStreamRestart,
        maxBackoff = 10.seconds,
        randomFactor = 0.2,
      ),
      sink = Sink.foreach(contractStore.push),
      teardown = System.exit,
    )
}
