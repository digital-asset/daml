// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

sealed abstract class UploadDarResult extends Product with Serializable

object UploadDarResult {

  /** The package was successfully uploaded */
  final case object Ok extends UploadDarResult

  /** The package was rejected for some reason */
  final case class Rejected(reason: UploadDarRejectionReason) extends UploadDarResult
}
