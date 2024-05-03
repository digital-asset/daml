// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.admin.gprc

import com.digitalasset.canton.mediator.admin.v30
import com.digitalasset.canton.sequencing.{SequencerConnectionValidation, SequencerConnections}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId

final case class InitializeMediatorRequest(
    domainId: DomainId,
    sequencerConnections: SequencerConnections,
    sequencerConnectionValidation: SequencerConnectionValidation,
) {
  def toProtoV30: v30.InitializeMediatorRequest =
    v30.InitializeMediatorRequest(
      domainId.toProtoPrimitive,
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
      sequencerConnectionsPO,
      sequencerConnectionValidationPO,
    ) = requestP
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      sequencerConnections <- ProtoConverter
        .required("sequencerConnections", sequencerConnectionsPO)
        .flatMap(SequencerConnections.fromProtoV30)
      sequencerConnectionValidation <- SequencerConnectionValidation.fromProtoV30(
        sequencerConnectionValidationPO
      )

    } yield InitializeMediatorRequest(
      domainId,
      sequencerConnections,
      sequencerConnectionValidation,
    )
  }
}
