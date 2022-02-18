// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.stream.{Materializer, RestartSettings}
import com.daml.logging.LoggingContext
import com.daml.platform.store.appendonlydao.events.ContractStateEvent
import com.daml.platform.store.cache.MutableCacheBackedContractStore
import com.daml.platform.store.cache.MutableCacheBackedContractStore.SubscribeToContractStateEvents

import scala.concurrent.duration.{FiniteDuration, _}

object MutableContractStateCacheUpdater {

  /** Creates a managed, restartable subscription and updates the mutable contract store cache with
    * contract state events.
    */
  def apply(
      contractStore: MutableCacheBackedContractStore,
      subscribeToContractStateEvents: SubscribeToContractStateEvents,
      minBackoffStreamRestart: FiniteDuration = 100.millis,
  )(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): RestartableManagedStream[ContractStateEvent] =
    RestartableManagedStream.withSyncConsumer(
      name = "contract state events",
      streamBuilder = () => subscribeToContractStateEvents(contractStore.cacheIndex()),
      consumeSync = contractStore.push,
      restartSettings = RestartSettings(
        minBackoff = minBackoffStreamRestart,
        maxBackoff = 10.seconds,
        randomFactor = 0.2,
      ),
      teardown = System.exit,
    )
}
