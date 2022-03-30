// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.api.v1.experimental_features.{
  CommandDeduplicationFeatures,
  CommandDeduplicationPeriodSupport,
  CommandDeduplicationType,
  ExperimentalContractIds,
}
import com.daml.ledger.runner.common.{Config, ConfigProvider}
import com.daml.ledger.participant.state.kvutils.app.Runner
import com.daml.ledger.resources.ResourceOwner
import com.daml.platform.apiserver.LedgerFeatures
import com.daml.platform.services.time.TimeProviderType

object Owner {
  // Utily if you want to spin this up as a library.
  def apply(config: Config[Unit]): ResourceOwner[Unit] = {
    val features = LedgerFeatures(
      staticTime = config.timeProviderType match {
        case TimeProviderType.Static => true
        case TimeProviderType.WallClock => false
      },
      commandDeduplicationFeatures = CommandDeduplicationFeatures.of(
        deduplicationPeriodSupport = Some(
          CommandDeduplicationPeriodSupport.of(
            offsetSupport =
              CommandDeduplicationPeriodSupport.OffsetSupport.OFFSET_CONVERT_TO_DURATION,
            durationSupport =
              CommandDeduplicationPeriodSupport.DurationSupport.DURATION_NATIVE_SUPPORT,
          )
        ),
        deduplicationType = CommandDeduplicationType.ASYNC_ONLY,
        maxDeduplicationDurationEnforced = true,
      ),
      contractIdFeatures = ExperimentalContractIds.of(
        v1 = ExperimentalContractIds.ContractIdV1Support.NON_SUFFIXED
      ),
    )

    for {
      dispatcher <- dispatcherOwner
      sharedState = InMemoryState.empty
      factory = new InMemoryLedgerFactory(dispatcher, sharedState)
      runner <- new Runner(RunnerName, factory, ConfigProvider.ForUnit, features).owner(config)
    } yield runner
  }
}
