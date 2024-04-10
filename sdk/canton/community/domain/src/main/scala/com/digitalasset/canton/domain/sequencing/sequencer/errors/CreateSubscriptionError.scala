// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.errors

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.topology.Member

/** Possible error from creating a subscription */
sealed trait CreateSubscriptionError

object CreateSubscriptionError {

  /** The member is not registered with the sequencer */
  final case class UnknownMember(member: Member) extends CreateSubscriptionError

  /** Problem registering an unauthenticated member */
  final case class RegisterUnauthenticatedMemberError(e: SequencerWriteError[RegisterMemberError])
      extends CreateSubscriptionError

  /** The member has been disabled and can no longer read. */
  final case class MemberDisabled(member: Member) extends CreateSubscriptionError

  /** The provided sequencer counter cannot be used for a subscription (currently will only occur if counter <0) */
  final case class InvalidCounter(counter: SequencerCounter) extends CreateSubscriptionError

  /** Returned if the counter is valid but this sequencer instance does not hold sufficient data to fulfill the request
    * from this point. Potentially because this data has been pruned or the sequencer was initialized at a later point.
    */
  final case class EventsUnavailable(counter: SequencerCounter, message: String)
      extends CreateSubscriptionError

  /** In case the sequencer is being shut down as we are creating the subscription */
  case object ShutdownError extends CreateSubscriptionError
}
