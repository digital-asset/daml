// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

sealed abstract class UploadDarRejectionReason extends Product with Serializable {
  def description: String
}

object UploadDarRejectionReason {

  /** One of the uploaded packages is not valid */
  final case class InvalidPackage(reason: String) extends UploadDarRejectionReason {
    override def description: String = "Uploaded packages were invalid: " + reason
  }

  /** The participant was not authorized to submit the upload request */
  final case object ParticipantNotAuthorized extends UploadDarRejectionReason {
    override def description: String = "Participant is not authorized to upload packages"
  }
}