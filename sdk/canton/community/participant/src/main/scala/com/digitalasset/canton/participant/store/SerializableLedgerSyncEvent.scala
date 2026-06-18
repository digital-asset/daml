// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.traverse.*
import com.digitalasset.canton.checkedToByteString
import com.digitalasset.canton.ledger.participant.state.*
import com.digitalasset.canton.participant.protocol.v30
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.*
import com.google.rpc.status.Status as RpcStatus

final case class SerializableRejectionReasonTemplate(
    rejectionReasonStatus: RpcStatus
) {
  def toProtoV30: v30.CommandRejected.GrpcRejectionReasonTemplate =
    v30.CommandRejected.GrpcRejectionReasonTemplate(checkedToByteString(rejectionReasonStatus))
}

object SerializableRejectionReasonTemplate {
  def fromProtoV30(
      reasonP: v30.CommandRejected.GrpcRejectionReasonTemplate
  ): ParsingResult[RpcStatus] =
    ProtoConverter.protoParser(RpcStatus.parseFrom)(reasonP.status)
}

final case class SerializableCompletionInfo(completionInfo: CompletionInfo) {
  def toProtoV30: v30.CompletionInfo = {
    val CompletionInfo(
      actAs,
      userId,
      commandId,
      deduplicateUntil,
      submissionId,
    ) =
      completionInfo
    v30.CompletionInfo(
      actAs,
      userId,
      commandId,
      deduplicateUntil.map(SerializableDeduplicationPeriod(_).toProtoV30),
      submissionId.getOrElse(""),
    )
  }
}

object SerializableCompletionInfo {
  def fromProtoV30(
      completionInfoP: v30.CompletionInfo
  ): ParsingResult[CompletionInfo] = {
    val v30.CompletionInfo(actAsP, userIdP, commandIdP, deduplicateUntilP, submissionIdP) =
      completionInfoP
    for {
      actAs <- actAsP.toList.traverse(ProtoConverter.parseLfPartyId(_, "act_as"))
      userId <- ProtoConverter.parseLFUserId(userIdP)
      commandId <- ProtoConverter.parseCommandId(commandIdP)
      deduplicateUntil <- deduplicateUntilP.traverse(SerializableDeduplicationPeriod.fromProtoV30)
      submissionId <- ProtoConverter.parseLFSubmissionIdO(submissionIdP)
    } yield CompletionInfo(
      actAs,
      userId,
      commandId,
      deduplicateUntil,
      submissionId,
    )
  }
}
