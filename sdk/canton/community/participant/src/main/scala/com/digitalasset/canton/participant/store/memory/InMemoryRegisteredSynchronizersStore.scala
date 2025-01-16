// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.RegisteredSynchronizersStore
import com.digitalasset.canton.participant.store.SynchronizerAliasAndIdStore.{
  Error,
  SynchronizerAliasAlreadyAdded,
  SynchronizerIdAlreadyAdded,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.collect.{BiMap, HashBiMap}

import scala.concurrent.{ExecutionContext, blocking}

class InMemoryRegisteredSynchronizersStore(override protected val loggerFactory: NamedLoggerFactory)
    extends RegisteredSynchronizersStore
    with NamedLogging {

  private val synchronizerAliasToId: BiMap[SynchronizerAlias, SynchronizerId] =
    HashBiMap.create[SynchronizerAlias, SynchronizerId]()

  private val lock = new Object()

  private implicit val ec: ExecutionContext = DirectExecutionContext(noTracingLogger)

  override def addMapping(alias: SynchronizerAlias, synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit] = {
    val swapped = blocking(lock.synchronized {
      for {
        _ <- Option(synchronizerAliasToId.get(alias))
          .fold(Either.right[Either[Error, Unit], Unit](())) { oldSynchronizerId =>
            Left(
              Either.cond(
                oldSynchronizerId == synchronizerId,
                (),
                SynchronizerAliasAlreadyAdded(alias, oldSynchronizerId),
              )
            )
          }
        _ <- Option(synchronizerAliasToId.inverse.get(synchronizerId))
          .fold(Either.right[Either[Error, Unit], Unit](())) { oldAlias =>
            Left(
              Either.cond(
                oldAlias == alias,
                (),
                SynchronizerIdAlreadyAdded(synchronizerId, oldAlias),
              )
            )
          }
      } yield {
        val _ = synchronizerAliasToId.put(alias, synchronizerId)
      }
    })
    EitherT.fromEither[FutureUnlessShutdown](swapped.swap.getOrElse(Either.unit))
  }

  override def aliasToSynchronizerIdMap(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SynchronizerAlias, SynchronizerId]] = {
    val map = blocking {
      lock.synchronized {
        import scala.jdk.CollectionConverters.*
        Map(synchronizerAliasToId.asScala.toSeq*)
      }
    }
    FutureUnlessShutdown.pure(map)
  }

  override def close(): Unit = ()
}
