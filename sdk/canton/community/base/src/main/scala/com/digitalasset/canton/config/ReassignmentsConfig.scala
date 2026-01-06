// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

/** Configuration relating to reassignments.
  *
  * @param targetTimestampForwardTolerance
  *   Defines how far into the future a target timestamp on an unassignment request may be, before
  *   this participant will determine the request unavalidatable and abstain from the decision.
  *
  * In order to validate an unassignment request, we need to refer to the topology of the target
  * synchronizer. Which snapshot of the topology we use is defined by the target timestamp in the
  * unassignment request. However the processing of events from each synchronizer can go at its own
  * pace on each participant, so at the time a participant receives the unassignment request to
  * validate, its own topology snapshot on the target synchronizer may lag behind the target
  * timestamp chosen by the submitting participant. In such cases, the validating participant can
  * simply wait until it has processed topology events indicating that it has caught up to the
  * required timestamp. However if the target timestamp is too far in the future, we do not want to
  * tie up resources waiting for the relevant topology, and this participant will simply abstain
  * from the decision, deferring to other participants to make the decision.
  *
  * The tuning of this parameter should be understood as a trade-off. Higher values mean that the
  * participant may consume more resources waiting to catch up before validating an unassignment.
  * Lower values mean that the participant is more likely to opt-out of validating unassignments if
  * other participants are running ahead of this one.
  */
final case class ReassignmentsConfig(
    targetTimestampForwardTolerance: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.ofSeconds(5)
)
