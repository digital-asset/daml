// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.admin.gprc

import com.digitalasset.canton.mediator.admin.v30
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.{SequencerConnectionValidation, SequencerConnections}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId

final case class InitializeMediatorRequest(
    domainId: DomainId,
    domainParameters: StaticDomainParameters,
    sequencerConnections: SequencerConnections,
    sequencerConnectionValidation: SequencerConnectionValidation,
) {
  def toProtoV30: v30.InitializeMediatorRequest =
    v30.InitializeMediatorRequest(
      domainId.toProtoPrimitive,
      Some(domainParameters.toProtoV30),
      Some(sequencerConnections.toProtoV30),
      sequencerConnectionValidation.toProtoV30,
    )
}

object InitializeMediatorRequest {
  def fromProtoV30(
      requestP: v30.InitializeMediatorRequest
  ): ParsingResult[InitializeMediatorRequest] = {
    val v30.InitializeMediatorRequest(
      domainIdP,
      domainParametersP,
      sequencerConnectionsPO,
      sequencerConnectionValidationPO,
    ) = requestP
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      domainParameters <- ProtoConverter
        .required("domain_parameters", domainParametersP)
        .flatMap(StaticDomainParameters.fromProtoV30)
      sequencerConnections <- ProtoConverter
        .required("sequencerConnections", sequencerConnectionsPO)
        .flatMap(SequencerConnections.fromProtoV30)
      sequencerConnectionValidation <- SequencerConnectionValidation.fromProtoV30(
        sequencerConnectionValidationPO
      )

    } yield InitializeMediatorRequest(
      domainId,
      domainParameters,
      sequencerConnections,
      sequencerConnectionValidation,
    )
  }
}
