// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.ledger.api.v2.experimental_features.ExperimentalCommandInspectionService

final case class LedgerFeatures(
    staticTime: Boolean = false,
    commandInspectionService: ExperimentalCommandInspectionService =
      ExperimentalCommandInspectionService(supported = true),
)
