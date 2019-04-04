// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.backend.api.v1

sealed trait RejectionReason {
  val description: String
}

object RejectionReason {

  /** The transaction relied on contracts being active that were no longer
    * active at the point where it was sequenced.
    */
  final case class Inconsistent(description: String) extends RejectionReason

  /** The Participant node did not have sufficient resource quota with the
    * to submit the transactoin.
    */
  final case class OutOfQuota(description: String) extends RejectionReason

  /** The transaction submission timed out.
    *
    * This means the 'maximumRecordTime' was smaller than the recordTime seen
    * in an event in the Participant node.
    */
  final case class TimedOut(description: String) extends RejectionReason

  /** The transaction submission was disputed.
    *
    * This means that the underlying ledger and its validation logic
    * considered the transaction potentially invalid. This can be due to a bug
    * in the submission or validiation logic, or due to malicious behaviour.
    */
  final case class Disputed(description: String) extends RejectionReason

  /** The participant node has already seen a command with the same commandId
    * during its implementation specific deduplication window.
    *
    * TODO (SM): explain in more detail how command de-duplication should
    * work.
    */
  final case class DuplicateCommandId(description: String) extends RejectionReason
}
