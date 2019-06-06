// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

sealed abstract class PackageUploadRejectionReason extends Product with Serializable {
  def description: String
}

object PackageUploadRejectionReason {

  /** One of the uploaded packages is not valid */
  final case object InvalidPackage extends PackageUploadRejectionReason {
    override def description: String = "Uploaded packages were invalid"
  }

  /** The participant was not authorized to submit the upload request */
  final case object ParticipantNotAuthorized extends PackageUploadRejectionReason {
    override def description: String = "Participant is not authorized to upload packages"
  }
}
