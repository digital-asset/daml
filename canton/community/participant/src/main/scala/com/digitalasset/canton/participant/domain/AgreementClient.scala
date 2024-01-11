// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.data.EitherT
import com.digitalasset.canton.common.domain.ServiceAgreement
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.AgreementService.AgreementServiceError
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnections}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContextExecutor, Future}

/** Simple wrapper around [[AgreementService]] which calls the wrapped service if the given sequencer connection is a GRPC one,
  * otherwise it defaults to a noop, since the HTTP CCF sequencer does not yet implement an agreement service.
  */
class AgreementClient(
    service: AgreementService,
    sequencerConnections: SequencerConnections,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    val ec: ExecutionContextExecutor
) extends NamedLogging {

  def isRequiredAgreementAccepted(domainId: DomainId, protocolVersion: ProtocolVersion)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AgreementServiceError, Option[ServiceAgreement]] =
    if (sequencerConnections.nonBftSetup)
      sequencerConnections.default match {
        case grpcSequencerConnection: GrpcSequencerConnection =>
          service.isRequiredAgreementAccepted(grpcSequencerConnection, domainId, protocolVersion)
      }
    else
      EitherT.rightT[Future, AgreementServiceError](None)

}
