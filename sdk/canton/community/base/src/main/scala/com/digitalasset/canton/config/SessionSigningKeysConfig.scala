// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

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
  */
final case class SessionSigningKeysConfig(
    enabled: Boolean,
    keyValidityDuration: PositiveFiniteDuration = PositiveFiniteDuration.ofMinutes(5),
) extends PrettyPrinting {

  override protected def pretty: Pretty[SessionSigningKeysConfig] =
    prettyOfClass(
      param("enabled", _.enabled),
      param("keyValidityDuration", _.keyValidityDuration),
    )

}

object SessionSigningKeysConfig {
  val disabled: SessionSigningKeysConfig = SessionSigningKeysConfig(enabled = false)
}
