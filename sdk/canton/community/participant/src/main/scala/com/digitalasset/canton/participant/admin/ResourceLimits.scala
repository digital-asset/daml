// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveDouble, PositiveNumeric}

/** Encapsulated resource limits for a participant.
  *
  * @param maxInflightValidationRequests the maximum number of requests that are currently being validated.
  *                         This also covers requests submitted by other participants.
  * @param maxSubmissionRate the maximum submission rate at which commands may be submitted through the ledger api.
  * @param maxSubmissionBurstFactor to ratio of the max submission rate, describing the maximum acceptable initial burst before the steady
  *                      submission rate limiting kicks in. example: if maxSubmissionRate is 100 and the burst ratio is 0.3, then the first
  *                      30 commands can submitted in the same instant, while thereafter, only one command every 10ms
  *                      is accepted.
  */
final case class ResourceLimits(
    maxInflightValidationRequests: Option[NonNegativeInt],
    maxSubmissionRate: Option[NonNegativeInt],
    maxSubmissionBurstFactor: PositiveDouble = ResourceLimits.defaultMaxSubmissionBurstFactor,
) {

  def toProtoV30: v30.ResourceLimits =
    v30.ResourceLimits(
      maxInflightValidationRequests = maxInflightValidationRequests.fold(-1)(_.unwrap),
      maxSubmissionRate = maxSubmissionRate.fold(-1)(_.unwrap),
      maxSubmissionBurstFactor = maxSubmissionBurstFactor.value,
    )
}

object ResourceLimits {
  def fromProtoV30(resourceLimitsP: v30.ResourceLimits): ResourceLimits = {
    val v30.ResourceLimits(
      maxInflightValidationRequestsP,
      maxSubmissionRateP,
      maxSubmissionBurstFactorP,
    ) = resourceLimitsP

    val maxInflightValidationRequests =
      if (maxInflightValidationRequestsP >= 0)
        Some(NonNegativeInt.tryCreate(maxInflightValidationRequestsP))
      else None
    val maxSubmissionRate =
      if (maxSubmissionRateP >= 0) Some(NonNegativeInt.tryCreate(maxSubmissionRateP)) else None
    // backwards compatible: use 0.5 as safe default value
    val maxBurstRatio = PositiveNumeric.tryCreate(
      if (maxSubmissionBurstFactorP > 0) maxSubmissionBurstFactorP else 0.5
    )
    ResourceLimits(maxInflightValidationRequests, maxSubmissionRate, maxBurstRatio)
  }

  def noLimit: ResourceLimits = ResourceLimits(None, None)

  /** Default resource limits to protect Canton from being overloaded by applications that send excessively many commands.
    * The default settings allow for processing an average of 100 commands/s with a latency of 5s,
    * with bursts of up to 200 commands/s.
    */
  def default: ResourceLimits = ResourceLimits(
    maxInflightValidationRequests = Some(NonNegativeInt.tryCreate(500)),
    maxSubmissionRate = Some(NonNegativeInt.tryCreate(200)),
    maxSubmissionBurstFactor = defaultMaxSubmissionBurstFactor,
  )

  def community: ResourceLimits =
    ResourceLimits(Some(NonNegativeInt.tryCreate(100)), None, defaultMaxSubmissionBurstFactor)

  private lazy val defaultMaxSubmissionBurstFactor = PositiveNumeric.tryCreate(0.5)

}
