// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer

import com.digitalasset.base.error.{ErrorCategory, ErrorCode, ErrorGroup, Explanation, Resolution}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.sequencing.client.{grpc as _, *}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.TopologyManagerError.SynchronizerErrorGroup

object Synchronizer extends SynchronizerErrorGroup {

  def setMemberRecordingPath(member: Member)(config: RecordingConfig): RecordingConfig = {
    val namePrefix = member.show.stripSuffix("...")
    config.setFilename(namePrefix)
  }

  def defaultReplayPath(member: Member)(config: ReplayConfig): ReplayConfig =
    config.copy(recordingConfig = setMemberRecordingPath(member)(config.recordingConfig))

  abstract class GrpcSequencerAuthenticationErrorGroup extends ErrorGroup

  @Explanation(
    """This error indicates that the initialisation of a synchronizer node failed due to invalid arguments."""
  )
  @Resolution("""Consult the error details.""")
  object FailedToInitialiseSynchronizerNode
      extends ErrorCode(
        id = "SYNCHRONIZER_NODE_INITIALISATION_FAILED",
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
