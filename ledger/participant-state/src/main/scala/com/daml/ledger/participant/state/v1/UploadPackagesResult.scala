// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

sealed abstract class UploadPackagesResult extends Product with Serializable {
  def description: String
}

object UploadPackagesResult {

  /** The packages were successfully uploaded */
  case object Ok extends UploadPackagesResult {
    override val description: String = "Packages successfully uploaded"
  }

  /** The system is overloaded, clients should back off exponentially */
  case object Overloaded extends UploadPackagesResult {
    override val description: String = "System is overloaded, please try again later"
  }

  /** Synchronous package upload is not supported */
  case object NotSupported extends UploadPackagesResult {
    override val description: String = "Packages upload not supported"
  }

  /** Submission ended up with internal error */
  final case class InternalError(reason: String) extends UploadPackagesResult {
    override def description: String =
      s"Party allocation failed with an internal error, reason=$reason"
  }

  /** One of the uploaded packages is not valid */
  final case class InvalidPackage(reason: String) extends UploadPackagesResult {
    override def description: String = s"Uploaded packages were invalid, details=$reason"
  }

  /** The participant was not authorized to submit the upload request */
  case object ParticipantNotAuthorized extends UploadPackagesResult {
    override val description: String = "Participant is not authorized to upload packages"
  }
}
