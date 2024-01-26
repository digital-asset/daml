// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.admin.gprc

import com.digitalasset.canton.domain.admin.v30
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId

final case class InitializeMediatorRequestX(
    domainId: DomainId,
    domainParameters: StaticDomainParameters,
    sequencerConnections: SequencerConnections,
) {
  def toProtoV30: v30.InitializeMediatorRequest =
    v30.InitializeMediatorRequest(
      domainId.toProtoPrimitive,
      Some(domainParameters.toProtoV30),
      Some(sequencerConnections.toProtoV30),
    )
}

object InitializeMediatorRequestX {
  def fromProtoV30(
      requestP: v30.InitializeMediatorRequest
  ): ParsingResult[InitializeMediatorRequestX] = {
    val v30.InitializeMediatorRequest(
      domainIdP,
      domainParametersP,
      sequencerConnectionsPO,
    ) = requestP
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      domainParameters <- ProtoConverter
        .required("domain_parameters", domainParametersP)
        .flatMap(StaticDomainParameters.fromProtoV30)
      sequencerConnections <- ProtoConverter
        .required("sequencerConnections", sequencerConnectionsPO)
        .flatMap(SequencerConnections.fromProtoV30)
    } yield InitializeMediatorRequestX(
      domainId,
      domainParameters,
      sequencerConnections,
    )
  }
}
