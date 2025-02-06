// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.crypto.{SigningAlgorithmSpec, SigningKeySpec}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

/** Configuration for enabling session signing keys with a specified validity period.
  * This setting is applicable only when using a KMS provider with externally stored keys.
  *
  * @param enabled Enables the usage of session signing keys in the protocol.
  * @param keyValidityDuration Specifies the validity duration for each session signing key.
  *                            To ensure that, in the event of a crash, the participant can generate a new session
  *                            signing key that can still serve confirmation responses for requests received before
  *                            the crash, its lifespan must be at least as long as the participant's response timeout.
  *                            The response's signature will rely on the topology snapshot from the request's original
  *                            timestamp (i.e., pre-crash), so the key must remain valid at least until that request
  *                            has timed out. Since this timeout is a dynamic synchronizer parameter, it can only be
  *                            evaluated when a request is sent. If the initially configured validity duration is too
  *                            short, we will use the participant's response timeout instead (and issue a warning).
  * @param signingAlgorithmSpec Defines the signing algorithm when using session signing keys. It defaults to Ed25519.
  * @param signingKeySpec Defines the key scheme to use for the session signing keys. It defaults to EcCurve25519.
  *                             Both algorithm and key scheme must be supported and allowed by the node.
  */
final case class SessionSigningKeysConfig(
    enabled: Boolean,
    keyValidityDuration: PositiveDurationSeconds = PositiveDurationSeconds.ofMinutes(5),
    // TODO(#13649): be sure these are supported by all the synchronizers the participant will connect to
    signingAlgorithmSpec: SigningAlgorithmSpec = SigningAlgorithmSpec.Ed25519,
    signingKeySpec: SigningKeySpec = SigningKeySpec.EcCurve25519,
) extends PrettyPrinting {

  override protected def pretty: Pretty[SessionSigningKeysConfig] =
    prettyOfClass(
      param("enabled", _.enabled),
      param("keyValidityDuration", _.keyValidityDuration),
      param("signingAlgorithmSpec", _.signingAlgorithmSpec),
      param("signingKeySpec", _.signingKeySpec),
    )

}

object SessionSigningKeysConfig {
  val disabled: SessionSigningKeysConfig = SessionSigningKeysConfig(enabled = false)
  val default: SessionSigningKeysConfig = SessionSigningKeysConfig(enabled = true)
}
