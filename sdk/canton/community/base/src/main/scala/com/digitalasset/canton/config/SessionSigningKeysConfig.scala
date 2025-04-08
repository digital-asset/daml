// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.crypto.{SigningAlgorithmSpec, SigningKeySpec}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

/** Configuration for enabling session signing keys with a specified validity period. This setting
  * is applicable only when using a KMS provider with externally stored keys.
  *
  * @param enabled
  *   Enables the usage of session signing keys in the protocol.
  * @param keyValidityDuration
  *   Specifies the validity duration for each session signing key. Its lifespan should be
  *   configured to be at least as long as the participant's response timeout. This ensures that, in
  *   the event of a crash, the participant can generate a new session signing key while still being
  *   able to serve confirmation responses for requests received before the crash. Since the
  *   response's signature will rely on the topology snapshot from the request's original timestamp
  *   (i.e., pre-crash), the key should remain valid at least until that request has timed out.
  * @param cutOffDuration
  *   A cut-off duration that defines how long before the session key expires we stop using it. This
  *   is important because a participant uses this key to sign submission requests, but the
  *   timestamp assigned by the sequencer is unknown in advance. Since the sequencer and other
  *   protocol participants use this timestamp to verify the delegation’s validity, if a new session
  *   signing key is only created after the previous key's validity period expires, multiple
  *   submissions may fail signature verification because their sequencing timestamps exceed the
  *   validity period. The configured baseline is based on the maximum expected time to generate and
  *   sign a new delegation.
  * @param keyEvictionPeriod
  *   This defines how long the private session signing key remains in memory. This is distinct from
  *   the validity period in the sense that we can be asked to sign arbitrarily old timestamps, and
  *   so we want to persist the key for longer times so we can re-use it. The eviction period should
  *   be longer than [[keyValidityDuration]] and at least as long as the majority of confirmation
  *   request decision latencies (for the mediator) or confirmation request response latencies (for
  *   participants).
  * @param signingAlgorithmSpec
  *   Defines the signing algorithm when using session signing keys. It defaults to Ed25519.
  * @param signingKeySpec
  *   Defines the key scheme to use for the session signing keys. It defaults to EcCurve25519. Both
  *   algorithm and key scheme must be supported and allowed by the node.
  */
final case class SessionSigningKeysConfig(
    enabled: Boolean,
    keyValidityDuration: PositiveDurationSeconds = PositiveDurationSeconds.ofMinutes(5),
    cutOffDuration: PositiveDurationSeconds = PositiveDurationSeconds.ofSeconds(30),
    keyEvictionPeriod: PositiveDurationSeconds = PositiveDurationSeconds.ofMinutes(10),
    // TODO(#13649): be sure these are supported by all the synchronizers the participant will connect to
    signingAlgorithmSpec: SigningAlgorithmSpec = SigningAlgorithmSpec.Ed25519,
    signingKeySpec: SigningKeySpec = SigningKeySpec.EcCurve25519,
) extends PrettyPrinting
    with UniformCantonConfigValidation {

  override protected def pretty: Pretty[SessionSigningKeysConfig] =
    prettyOfClass(
      param("enabled", _.enabled),
      param("keyValidityDuration", _.keyValidityDuration),
      param("cutOffDuration", _.cutOffDuration),
      param("keyEvictionPeriod", _.keyEvictionPeriod),
      param("signingAlgorithmSpec", _.signingAlgorithmSpec),
      param("signingKeySpec", _.signingKeySpec),
    )

}

object SessionSigningKeysConfig {
  implicit val sessionSigningKeysConfigCantonConfigValidator
      : CantonConfigValidator[SessionSigningKeysConfig] =
    CantonConfigValidatorDerivation[SessionSigningKeysConfig]

  val disabled: SessionSigningKeysConfig = SessionSigningKeysConfig(enabled = false)
  val default: SessionSigningKeysConfig = SessionSigningKeysConfig(enabled = true)
}
