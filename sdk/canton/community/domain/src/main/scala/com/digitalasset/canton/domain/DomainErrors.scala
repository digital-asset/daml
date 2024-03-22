// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain

import com.daml.error.*
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.sequencing.client.{grpc as _, *}
import com.digitalasset.canton.topology.TopologyManagerError.DomainErrorGroup
import com.digitalasset.canton.topology.*
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference

object Domain extends DomainErrorGroup {

  /** If the function maps `member` to `recordConfig`,
    * the sequencer client for `member` will record all sends requested and events received to the directory specified
    * by the recording config.
    * A new recording starts whenever the domain is restarted.
    */
  @VisibleForTesting
  val recordSequencerInteractions: AtomicReference[PartialFunction[Member, RecordingConfig]] =
    new AtomicReference(PartialFunction.empty)

  /** If the function maps `member` to `path`,
    * the sequencer client for `member` will replay events from `path` instead of pulling them from the sequencer.
    * A new replay starts whenever the domain is restarted.
    */
  @VisibleForTesting
  val replaySequencerConfig: AtomicReference[PartialFunction[Member, ReplayConfig]] =
    new AtomicReference(PartialFunction.empty)

  def setMemberRecordingPath(member: Member)(config: RecordingConfig): RecordingConfig = {
    val namePrefix = member.show.stripSuffix("...")
    config.setFilename(namePrefix)
  }

  def defaultReplayPath(member: Member)(config: ReplayConfig): ReplayConfig =
    config.copy(recordingConfig = setMemberRecordingPath(member)(config.recordingConfig))

  abstract class GrpcSequencerAuthenticationErrorGroup extends ErrorGroup

  @Explanation(
    """This error indicates that the initialisation of a domain node failed due to invalid arguments."""
  )
  @Resolution("""Consult the error details.""")
  object FailedToInitialiseDomainNode
      extends ErrorCode(
        id = "DOMAIN_NODE_INITIALISATION_FAILED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(override val cause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause)

    final case class Shutdown()(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = "Node is being shutdown")
  }

}
