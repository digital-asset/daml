// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.ledger.api.v1.experimental_features.ExperimentalCommitterEventLog.CommitterEventLogType.CENTRALIZED
import com.daml.ledger.api.v1.experimental_features.{
  CommandDeduplicationFeatures,
  ExperimentalCommitterEventLog,
  ExperimentalContractIds,
  ExperimentalExplicitDisclosure,
}

final case class LedgerFeatures(
    staticTime: Boolean = false,
    commandDeduplicationFeatures: CommandDeduplicationFeatures =
      CommandDeduplicationFeatures.defaultInstance,
    contractIdFeatures: ExperimentalContractIds = ExperimentalContractIds.defaultInstance,
    committerEventLog: ExperimentalCommitterEventLog =
      ExperimentalCommitterEventLog.of(eventLogType = CENTRALIZED),
    explicitDisclosure: ExperimentalExplicitDisclosure =
      ExperimentalExplicitDisclosure.of(supported = false),
)
