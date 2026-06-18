// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.Errors.MismatchError.{
  CommitmentsMismatch,
  NoSharedContracts,
}
import org.scalatest

object LoggerSuppressionHelpers {
  // Useful for tests that involve party replication (and thus copying the ACS to another participant)
  def suppressOptionalAcsCmtMismatchAndNoSharedContracts()
      : Seq[(LogEntryOptionality, LogEntry => scalatest.Assertion)] =
    Seq(
      LogEntryOptionality.Optional -> (_.shouldBeCantonErrorCode(NoSharedContracts)),
      LogEntryOptionality.Optional -> (_.shouldBeCantonErrorCode(CommitmentsMismatch)),
    )
}
