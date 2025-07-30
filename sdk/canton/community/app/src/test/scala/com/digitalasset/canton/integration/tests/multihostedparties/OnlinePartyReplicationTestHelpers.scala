// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.admin.api.client.data.AddPartyStatus
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.ParticipantReference

import scala.concurrent.duration.*

/** Utilities for testing online party replication.
  */
private[multihostedparties] trait OnlinePartyReplicationTestHelpers {
  this: BaseTest =>

  /** Wait until online party replication completes on the source and target participants with the
    * expected number of replicated contracts on the specified request.
    */
  protected def eventuallyOnPRCompletes(
      sourceParticipant: ParticipantReference,
      targetParticipant: ParticipantReference,
      addPartyRequestId: String,
      expectedNumContracts: NonNegativeInt,
  ): Unit =
    eventually(retryOnTestFailuresOnly = false, maxPollInterval = 10.millis) {
      val spStatus = sourceParticipant.parties.get_add_party_status(addPartyRequestId)
      val tpStatus = targetParticipant.parties.get_add_party_status(addPartyRequestId)
      (spStatus.status, tpStatus.status) match {
        case (
              AddPartyStatus.Completed(_, _, `expectedNumContracts`),
              AddPartyStatus.Completed(_, _, `expectedNumContracts`),
            ) =>
          logger.info(
            s"SP and TP completed party replication with status $spStatus and $tpStatus"
          )
        case (
              AddPartyStatus.Completed(_, _, numSpContracts),
              AddPartyStatus.Completed(_, _, numTpContracts),
            ) =>
          logger.warn(
            s"SP and TP completed party replication but had unexpected number of contracts: $numSpContracts and $numTpContracts, expected $expectedNumContracts"
          )
        case (sourceStatus, targetStatus) =>
          fail(
            s"TP and SP did not complete party replication. SP and TP status: $sourceStatus and $targetStatus"
          )
      }
    }
}
