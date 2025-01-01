// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.AcsCounterParticipantConfigStore
import com.digitalasset.canton.pruning.{
  ConfigForDomainThresholds,
  ConfigForNoWaitCounterParticipants,
  ConfigForSlowCounterParticipants,
}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

class InMemoryAcsCommitmentConfigStore(implicit val ec: ExecutionContext)
    extends AcsCounterParticipantConfigStore {

  private val thresholdForDomainConfigs: AtomicReference[Seq[ConfigForDomainThresholds]] =
    new AtomicReference(Seq.empty)
  private val slowCounterParticipantConfigs
      : AtomicReference[Seq[ConfigForSlowCounterParticipants]] = new AtomicReference(Seq.empty)

  private val noWaitCounterParticipantConfigs
      : AtomicReference[Set[ConfigForNoWaitCounterParticipants]] = new AtomicReference(Set.empty)

  override def fetchAllSlowCounterParticipantConfig()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[(Seq[ConfigForSlowCounterParticipants], Seq[ConfigForDomainThresholds])] =
    FutureUnlessShutdown.pure(
      (slowCounterParticipantConfigs.get(), thresholdForDomainConfigs.get())
    )

  override def createOrUpdateCounterParticipantConfigs(
      configs: Seq[ConfigForSlowCounterParticipants],
      thresholds: Seq[ConfigForDomainThresholds],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    slowCounterParticipantConfigs.updateAndGet { x =>
      val filteredConfigs = x.filterNot { config =>
        configs.exists(_.synchronizerId == config.synchronizerId) || configs.isEmpty
      }
      filteredConfigs ++ configs
    }

    thresholdForDomainConfigs.updateAndGet { x =>
      val filteredConfigs = x.filterNot(config =>
        thresholds.exists(_.synchronizerId == config.synchronizerId) || thresholds.isEmpty
      )

      filteredConfigs ++ thresholds
    }

    FutureUnlessShutdown.unit
  }

  override def clearSlowCounterParticipants(
      synchronizerIds: Seq[SynchronizerId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    slowCounterParticipantConfigs.updateAndGet(x =>
      x.filter(config =>
        synchronizerIds.nonEmpty || !synchronizerIds.contains(config.synchronizerId)
      )
    )
    thresholdForDomainConfigs.updateAndGet(x =>
      x.filter(config =>
        synchronizerIds.nonEmpty || !synchronizerIds.contains(config.synchronizerId)
      )
    )
    FutureUnlessShutdown.unit
  }
  override def close(): Unit = ()

  override def addNoWaitCounterParticipant(
      configs: Seq[ConfigForNoWaitCounterParticipants]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    noWaitCounterParticipantConfigs.updateAndGet(x => x ++ configs)
    FutureUnlessShutdown.unit
  }

  override def removeNoWaitCounterParticipant(
      domains: Seq[SynchronizerId],
      participants: Seq[ParticipantId],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val crossProduct = for {
      domain <- domains
      participant <- participants
    } yield (domain, participant)
    noWaitCounterParticipantConfigs.updateAndGet(conf =>
      conf.filter(config =>
        !crossProduct.contains(
          (config.synchronizerId, config.participantId)
        ) || crossProduct.isEmpty
      )
    )
    FutureUnlessShutdown.unit
  }

  override def getAllActiveNoWaitCounterParticipants(
      filterDomains: Seq[SynchronizerId],
      filterParticipants: Seq[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[ConfigForNoWaitCounterParticipants]] =
    FutureUnlessShutdown.pure(
      noWaitCounterParticipantConfigs
        .get()
        .filter(c =>
          (filterDomains.contains(c.synchronizerId) || filterDomains.isEmpty) && (filterParticipants
            .contains(c.participantId) || filterParticipants.isEmpty)
        )
        .toSeq
    )

}
