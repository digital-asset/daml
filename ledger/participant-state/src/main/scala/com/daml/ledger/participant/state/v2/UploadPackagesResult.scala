// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

sealed abstract class UploadPackagesResult extends Product with Serializable {
  def description: String
}

object UploadPackagesResult {

  /** The package was successfully uploaded */
  final case object Ok extends UploadPackagesResult {
    override def description: String = "Packages successfully uploaded"
  }

  /** One of the uploaded packages is not valid */
  final case class InvalidPackage(reason: String) extends UploadPackagesResult {
    override def description: String = "Uploaded packages were invalid: " + reason
  }

  /** The participant was not authorized to submit the upload request */
  final case object ParticipantNotAuthorized extends UploadPackagesResult {
    override def description: String = "Participant is not authorized to upload packages"
  }
}
