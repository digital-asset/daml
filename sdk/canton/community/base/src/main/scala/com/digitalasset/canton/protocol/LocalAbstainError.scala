// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.base.error.{ErrorCategory, ErrorClass, ErrorCode, ErrorResource}
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.LocalRejectionGroup
import com.digitalasset.canton.protocol.messages.LocalAbstain
import com.digitalasset.canton.version.ProtocolVersion

abstract class LocalAbstainErrorCode(
    id: String,
    category: ErrorCategory,
)(implicit parent: ErrorClass)
    extends ErrorCode(id, category) {
  override implicit val code: LocalAbstainErrorCode = this
}

sealed trait LocalAbstainError extends LocalError {

  def toLocalAbstain(protocolVersion: ProtocolVersion): LocalAbstain =
    LocalAbstain(reason(), protocolVersion)
}

sealed abstract class LocalAbstainErrorImpl(
    override val _causePrefix: String,
    override val _details: String = "",
    override val throwableO: Option[Throwable] = None,
    override val _resourcesType: Option[ErrorResource] = None,
    override val _resources: Seq[String] = Seq.empty,
)(implicit override val code: LocalAbstainErrorCode)
    extends LocalAbstainError {
  override def isMalformed: Boolean = false
}

object LocalAbstainError extends LocalRejectionGroup {

  object CannotPerformAllValidations
      extends LocalAbstainErrorCode(
        id = "CANNOT_PERFORM_ALL_VALIDATIONS",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Abstain(override val _details: String)
        extends LocalAbstainErrorImpl(_causePrefix = "Cannot perform all validations: ")
  }
}
