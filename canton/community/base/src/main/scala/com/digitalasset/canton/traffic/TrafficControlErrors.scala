// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.traffic

import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.TrafficControlErrorGroup
import com.digitalasset.canton.logging.ErrorLoggingContext

object TrafficControlErrors extends TrafficControlErrorGroup {
  sealed trait TrafficControlError extends Product with Serializable with CantonError

  @Explanation(
    """This error indicates that the participant does not have a traffic state."""
  )
  @Resolution(
    """Ensure that the the participant is connected to a domain with traffic control enabled,
        and that it has received at least one event from the domain since its connection."""
  )
  object TrafficStateNotFound
      extends ErrorCode(
        id = "TRAFFIC_CONTROL_STATE_NOT_FOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Error()(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Traffic state not found"
        )
        with TrafficControlError
  }

  @Explanation(
    """Traffic control is not active on the domain."""
  )
  @Resolution(
    """Enable traffic control by setting the traffic control dynamic domain parameter."""
  )
  object TrafficControlDisabled
      extends ErrorCode(
        id = "TRAFFIC_CONTROL_DISABLED",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Error()(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "TrafficControlDisabled"
        )
        with TrafficControlError
  }

  @Explanation(
    """Received an unexpected error when sending a top up submission request for sequencing."""
  )
  @Resolution(
    """Re-submit the top up request with an exponential backoff strategy."""
  )
  object TrafficBalanceRequestAsyncSendFailed
      extends ErrorCode(
        id = "TRAFFIC_CONTROL_TOP_UP_SUBMISSION_FAILED",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Error(failureCause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = failureCause
        )
        with TrafficControlError
  }
}
