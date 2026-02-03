// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.crypto.{SigningAlgorithmSpec, SigningKeySpec}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.google.common.annotations.VisibleForTesting

/** Configuration for enabling session signing keys with a specified validity period. This setting
  * is applicable only when using a KMS provider with externally stored keys.
  *
  * @param enabled
  *   Enables the usage of session signing keys in the protocol.
  * @param keyValidityDuration
  *   Specifies the validity duration for each session signing key. Its lifespan MUST be configured
  *   to be: keyValidityDuration > 2 * cutoffDuration. The validity duration should also not be too
  *   short, so that a session signing key can be reused across, for example, multiple submission
  *   requests.
  * @param toleranceShiftDuration
  *   This defines a value that that is used to shift the validity interval from '[ts,
  *   ts+keyValidityDuration]' to '[ts-tolerance, ts+keyValidityDuration-tolerance]'. It MUST
  *   respect the following constraints: (1) keyValidityDuration - toleranceShiftDuration >=
  *   cutoffDuration; (2) toleranceShiftDuration >= cutoffDuration. What we are trying to solve with
  *   this shift is the following: Given a sequence of timestamps `tss`, find a set of intervals of
  *   fixed length `keyValidityDuration` such that each timestamp in `tss` falls within at least one
  *   interval. The session signing key selection algorithm picks intervals as it goes through the
  *   list of timestamps from left to right, without any lookahead. As such, the goal is to minimize
  *   the number of intervals needed. For example, consider the sequence of timestamps: `ts0`,
  *   `ts0-1us`, `ts0-2us`, `ts0-3us`. Without applying this shift, a new interval (i.e., a new
  *   session signing key) would be created for each of these slightly different timestamps.
  *   Shifting the interval allows us to cover multiple timestamps with a single interval, reducing
  *   the number of session keys created. This value can be adjusted: reduced if timestamps are
  *   mostly increasing, or increased if timestamps are mostly decreasing.
  * @param cutOffDuration
  *   A cut-off duration is applied only when we need to sign something without knowing the exact
  *   topology timestamp that will later be used for verification (e.g., when using a
  *   currentSnapshotApproximation). The cutoff measures the level of this uncertainty. The
  *   participant makes a guess t0 at the topology timestamp for verification and will use an
  *   existing session signing key k only if the interval [t0 - cutoff, t0 + cutoff] lies fully
  *   within k’s validity period. As a result, newly created session signing keys must have a
  *   validity period that starts at least cutoffDuration before ts, which is ensured if
  *   toleranceShiftDuration ≥ cutoffDuration. A typical example of this uncertainty is signing
  *   submission requests, where the sequencer-assigned timestamp is unknown in advance. If a new
  *   session key is created only after the previous key’s validity interval ends, multiple
  *   submissions may fail verification because their sequencing timestamps fall outside that
  *   interval.
  * @param keyEvictionPeriod
  *   This defines how long the private session signing key remains in memory. This is distinct from
  *   the validity period in the sense that we can be asked to sign arbitrarily old timestamps, and
  *   so we want to persist the key for longer times so we can re-use it. The eviction period should
  *   be longer than [[keyValidityDuration]] and at least as long as the majority of confirmation
  *   request decision latencies (for the mediator) or confirmation request response latencies (for
  *   participants). It should also be longer than a participant's response timeout. For example, a
  *   participant may create a submission at time ts0, which is sequenced at ts1. Later, it may need
  *   to send a response at ts2 using the original timestamp ts1. Under normal operation, ts2 − ts0
  *   should not exceed the participant response timeout. Therefore, if the eviction period is
  *   longer than this timeout, the participant won’t need to create a new session signing key just
  *   to send the confirmation response.
  * @param signingAlgorithmSpec
  *   Defines the signing algorithm when using session signing keys. It defaults to Ed25519.
  * @param signingKeySpec
  *   Defines the key scheme to use for the session signing keys. It defaults to EcCurve25519. Both
  *   algorithm and key scheme must be supported and allowed by the node.
  */
final case class SessionSigningKeysConfig(
    enabled: Boolean,
    keyValidityDuration: PositiveFiniteDuration = PositiveFiniteDuration.ofMinutes(6),
    toleranceShiftDuration: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMinutes(3),
    cutOffDuration: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMinutes(1),
    keyEvictionPeriod: PositiveFiniteDuration = PositiveFiniteDuration.ofMinutes(10),
    // TODO(#13649): be sure these are supported by all the synchronizers the participant will connect to
    signingAlgorithmSpec: SigningAlgorithmSpec = SigningAlgorithmSpec.Ed25519,
    signingKeySpec: SigningKeySpec = SigningKeySpec.EcCurve25519,
) extends PrettyPrinting {

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
  val disabled: SessionSigningKeysConfig = SessionSigningKeysConfig(enabled = false)
  val default: SessionSigningKeysConfig = SessionSigningKeysConfig(enabled = true)

  /** Short test-only configuration: durations are small enough to trigger key rotation and validity
    * edge cases within a test, but not so small that normal scheduling jitter would make tests
    * flaky.
    */
  @VisibleForTesting
  val short: SessionSigningKeysConfig = SessionSigningKeysConfig(
    enabled = true,
    keyValidityDuration = PositiveFiniteDuration.ofSeconds(10),
    toleranceShiftDuration = NonNegativeFiniteDuration.ofSeconds(5),
    cutOffDuration = NonNegativeFiniteDuration.ofSeconds(2),
    keyEvictionPeriod = PositiveFiniteDuration.ofMinutes(1),
  )
}
