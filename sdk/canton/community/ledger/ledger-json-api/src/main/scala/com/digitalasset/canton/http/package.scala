// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
import com.digitalasset.daml.lf
import com.google.protobuf.ByteString
import org.apache.pekko.http.scaladsl.model.StatusCode

package object http {

  import scalaz.{@@, Tag}

  type LfValue = lf.value.Value
  type Party = lar.Party
  final val Party = lar.Party
  type PartySet = NonEmpty[Set[Party]]

  type UserId = lar.UserId
  val UserId = lar.UserId

  type RetryInfoDetailDuration = scala.concurrent.duration.Duration @@ RetryInfoDetailDurationTag
  val RetryInfoDetailDuration = Tag.of[RetryInfoDetailDurationTag]

  type Base64 = ByteString @@ Base64Tag
  val Base64 = Tag.of[Base64Tag]

}

package http {

  sealed trait Base64Tag

  sealed abstract class SyncResponse[+R] extends Product with Serializable {
    def status: StatusCode
  }

  sealed trait RetryInfoDetailDurationTag

  // Important note: when changing this ADT, adapt the custom associated JsonFormat codec in JsonProtocol
  sealed trait ErrorDetail extends Product with Serializable
  final case class ResourceInfoDetail(name: String, typ: String) extends ErrorDetail
  final case class ErrorInfoDetail(errorCodeId: String, metadata: Map[String, String])
      extends ErrorDetail
  final case class RetryInfoDetail(duration: RetryInfoDetailDuration) extends ErrorDetail
  final case class RequestInfoDetail(correlationId: String) extends ErrorDetail

  object ErrorDetail {

    import com.digitalasset.base.error.utils.ErrorDetails
    def fromErrorUtils(errorDetail: ErrorDetails.ErrorDetail): http.ErrorDetail =
      errorDetail match {
        case ErrorDetails.ResourceInfoDetail(name, typ) => http.ResourceInfoDetail(name, typ)
        case ErrorDetails.ErrorInfoDetail(errorCodeId, metadata) =>
          http.ErrorInfoDetail(errorCodeId, metadata)
        case ErrorDetails.RetryInfoDetail(duration) =>
          http.RetryInfoDetail(http.RetryInfoDetailDuration(duration))
        case ErrorDetails.RequestInfoDetail(correlationId) =>
          http.RequestInfoDetail(correlationId)
      }
  }

  final case class LedgerApiError(
      code: Int,
      message: String,
      details: Seq[ErrorDetail],
  )

  final case class ErrorResponse(
      errors: List[String],
      status: StatusCode,
      ledgerApiError: Option[LedgerApiError] = None,
  ) extends SyncResponse[Nothing]
}
