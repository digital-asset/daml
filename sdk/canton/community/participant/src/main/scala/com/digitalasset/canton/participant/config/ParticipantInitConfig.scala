// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.config.{
  CantonConfigValidator,
  IdentityConfig,
  InitConfigBase,
  NonNegativeFiniteDuration,
  UniformCantonConfigValidation,
}
import com.digitalasset.canton.participant.config.ParticipantInitConfig.ParticipantLedgerApiInitConfig

/** Init configuration specific to participant nodes
  * @param ledgerApi
  *   ledgerApi related init config
  * @param state
  *   optional state config, pointing to a state file which will be applied to the node whenever it
  *   changes
  */
final case class ParticipantInitConfig(
    identity: IdentityConfig = IdentityConfig.Auto(),
    ledgerApi: ParticipantLedgerApiInitConfig = ParticipantLedgerApiInitConfig(),
    generateIntermediateKey: Boolean = false,
    generateTopologyTransactionsAndKeys: Boolean = true,
) extends InitConfigBase
    with UniformCantonConfigValidation

object ParticipantInitConfig {

  implicit val participantInitConfigCantonConfigValidator
      : CantonConfigValidator[ParticipantInitConfig] =
    CantonConfigValidatorDerivation[ParticipantInitConfig]

  /** Init configuration of the ledger API for participant nodes
    * @param maxDeduplicationDuration
    *   The max deduplication duration reported by the participant's ledger configuration service.
    *   The participant's command and command submission services accept all command deduplication
    *   durations up to this duration. Acceptance of longer ones is at the discretion of the
    *   participant and may vary over time.
    */
  final case class ParticipantLedgerApiInitConfig(
      maxDeduplicationDuration: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofDays(7L)
  ) extends UniformCantonConfigValidation

  object ParticipantLedgerApiInitConfig {
    implicit val participantLedgerApiInitConfigCantonConfigValidator
        : CantonConfigValidator[ParticipantLedgerApiInitConfig] =
      CantonConfigValidatorDerivation[ParticipantLedgerApiInitConfig]
  }

}
