// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.syntax.either.*
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.{LedgerSubmissionId, checked}
import slick.jdbc.{GetResult, SetParameter}

final case class SerializableSubmissionId(submissionId: LedgerSubmissionId) {
  def toProtoPrimitive: String = submissionId
  def toLengthLimitedString: String255 =
    checked(String255.tryCreate(submissionId)) // LedgerSubmissionId is limited to 255 chars
}
object SerializableSubmissionId {
  implicit val setParameterSubmissionId: SetParameter[SerializableSubmissionId] = (v, pp) =>
    pp >> v.toLengthLimitedString

  implicit val getResultSubmissionId: GetResult[SerializableSubmissionId] = GetResult { r =>
    deserializeFromPrimitive(r.nextString())
  }

  implicit val getResultOptionSubmissionId: GetResult[Option[SerializableSubmissionId]] =
    GetResult { r =>
      r.nextStringOption().map(deserializeFromPrimitive)
    }

  implicit val setParameterOptionSubmissionId: SetParameter[Option[SerializableSubmissionId]] =
    (v, pp) => pp >> v.map(_.toLengthLimitedString)
  private def deserializeFromPrimitive(serialized: String): SerializableSubmissionId = {
    val submissionId = ProtoConverter
      .parseLFSubmissionId(serialized)
      .valueOr(err =>
        throw new DbDeserializationException(s"Failed to deserialize submission id: $err")
      )
    SerializableSubmissionId(submissionId)
  }
}
