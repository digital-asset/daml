// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.data.EitherT
import com.digitalasset.canton.common.domain.ServiceAgreement
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLogging}
import com.digitalasset.canton.sequencing.GrpcSequencerConnection
import com.digitalasset.canton.topology.DomainId

import scala.concurrent.{ExecutionContextExecutor, Future}

private[domain] trait DomainServiceClient extends NamedLogging {
  implicit def ec: ExecutionContextExecutor

  def getAgreement(domainId: DomainId, sequencerConnection: GrpcSequencerConnection)(implicit
      loggingContext: ErrorLoggingContext
  ): EitherT[Future, DomainServiceClient.Error.GetServiceAgreementError, Option[ServiceAgreement]]
}

private[domain] object DomainServiceClient {
  sealed trait Error {
    def message: String
  }
  object Error {
    final case class DeserializationFailure(err: String) extends Error {
      def message: String = s"Unable to deserialize proto: $err"
    }
    final case class HandshakeFailure(message: String) extends Error
    final case class GetServiceAgreementError(message: String) extends Error
    final case class InvalidResponse(message: String) extends Error
    final case class Transport(message: String) extends Error
  }
}
