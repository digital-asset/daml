// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveDouble
import com.digitalasset.canton.environment.CantonNodeParameters

// TODO(#15161): Move to SequencerNodeParameters.scala
trait SequencerParameters {
  def maxBurstFactor: PositiveDouble

  def processingTimeouts: ProcessingTimeout
}

trait CantonNodeWithSequencerParameters extends CantonNodeParameters with SequencerParameters
