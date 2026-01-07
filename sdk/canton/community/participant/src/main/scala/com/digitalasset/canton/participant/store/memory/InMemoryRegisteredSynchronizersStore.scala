// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.RegisteredSynchronizersStore
import com.digitalasset.canton.participant.store.SynchronizerAliasAndIdStore.{
  Error,
  InconsistentLogicalSynchronizerIds,
  SynchronizerIdAlreadyAdded,
}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Mutex

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class InMemoryRegisteredSynchronizersStore(
    override protected val loggerFactory: NamedLoggerFactory
)(implicit ec: ExecutionContext)
    extends RegisteredSynchronizersStore
    with NamedLogging {

  private val lock = new Mutex()
  private val synchronizerAliasToIds
      : TrieMap[SynchronizerAlias, NonEmpty[Set[PhysicalSynchronizerId]]] =
    new TrieMap()

  override def addMapping(alias: SynchronizerAlias, psid: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Unit] = {

    val result: Either[Error, Unit] =
      lock.exclusive {
        val inconsistentIdO = synchronizerAliasToIds
          .get(alias)
          .flatMap(_.find(_.logical != psid.logical))

        val existingAliasO = synchronizerAliasToIds.collectFirst {
          case (existingAlias, psids) if psids.contains(psid) => existingAlias
        }

        for {
          _ <- inconsistentIdO
            .map(InconsistentLogicalSynchronizerIds(alias, psid, _))
            .toLeft(())

          _ <- existingAliasO.fold(().asRight[Error])(existingAlias =>
            Either.cond(
              existingAlias == alias,
              (),
              SynchronizerIdAlreadyAdded(psid, existingAlias),
            )
          )
        } yield {
          synchronizerAliasToIds
            .updateWith(alias) {
              case Some(ids) => Some(ids ++ Seq(psid))
              case None => Some(NonEmpty.mk(Set, psid))
            }
            .discard
        }
      }

    EitherT.fromEither[FutureUnlessShutdown](result)
  }

  override def aliasToSynchronizerIdMap(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SynchronizerAlias, NonEmpty[Set[PhysicalSynchronizerId]]]] =
    FutureUnlessShutdown.pure(synchronizerAliasToIds.snapshot().toMap)

  override def close(): Unit = ()
}
