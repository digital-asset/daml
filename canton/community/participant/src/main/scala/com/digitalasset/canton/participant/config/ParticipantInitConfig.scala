// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import com.digitalasset.canton.config.{InitConfigBase, NonNegativeFiniteDuration}
import com.digitalasset.canton.participant.config.ParticipantInitConfig.ParticipantLedgerApiInitConfig

/** Init configuration specific to participant nodes
  * @param ledgerApi ledgerApi related init config
  */
final case class ParticipantInitConfig(
    identity: Option[InitConfigBase.Identity] = Some(InitConfigBase.Identity()),
    ledgerApi: ParticipantLedgerApiInitConfig = ParticipantLedgerApiInitConfig(),
) extends InitConfigBase

object ParticipantInitConfig {

  /** Init configuration of the ledger API for participant nodes
    * @param maxDeduplicationDuration The max deduplication duration reported by the participant's ledger configuration service.
    *                                 The participant's command and command submission services accept all command deduplication durations up to this duration. Acceptance of longer ones is at the discretion of the participant and may vary over time.
    */
  final case class ParticipantLedgerApiInitConfig(
      maxDeduplicationDuration: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofDays(7L)
  )
}
