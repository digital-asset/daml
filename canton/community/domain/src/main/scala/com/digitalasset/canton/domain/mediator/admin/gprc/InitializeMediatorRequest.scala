// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.admin.gprc

import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.domain.admin.{v30, v30old}
import com.digitalasset.canton.domain.sequencing.admin.grpc.InitializeSequencerRequest
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.{SequencerConnection, SequencerConnections}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import com.digitalasset.canton.topology.{DomainId, MediatorId, UniqueIdentifier}

final case class InitializeMediatorRequest(
    domainId: DomainId,
    mediatorId: MediatorId,
    topologyState: Option[StoredTopologyTransactions[TopologyChangeOp.Positive]],
    domainParameters: StaticDomainParameters,
    sequencerConnections: SequencerConnections,
    signingKeyFingerprint: Option[Fingerprint],
) {
  def toProtoV30Old: v30old.InitializeMediatorRequest =
    v30old.InitializeMediatorRequest(
      domainId.toProtoPrimitive,
      mediatorId.uid.toProtoPrimitive,
      topologyState.map(_.toProtoV30),
      Some(domainParameters.toProtoV30),
      // Non-BFT domain is only supporting a single sequencer connection
      Some(sequencerConnections.default.toProtoV30),
      signingKeyFingerprint.map(_.toProtoPrimitive),
    )
}

object InitializeMediatorRequest {
  def fromProtoV30Old(
      requestP: v30old.InitializeMediatorRequest
  ): ParsingResult[InitializeMediatorRequest] = {
    val v30old.InitializeMediatorRequest(
      domainIdP,
      mediatorIdP,
      topologyStateP,
      domainParametersP,
      sequencerConnectionP,
      signingKeyFingerprintP,
    ) = requestP
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      mediatorId <- UniqueIdentifier
        .fromProtoPrimitive(mediatorIdP, "mediator_id")
        .map(MediatorId(_))
      topologyState <- topologyStateP.traverse(InitializeSequencerRequest.convertTopologySnapshot)
      domainParameters <- ProtoConverter
        .required("domain_parameters", domainParametersP)
        .flatMap(StaticDomainParameters.fromProtoV30)
      sequencerConnection <- ProtoConverter.parseRequired(
        SequencerConnection.fromProtoV0,
        "sequencer_connection",
        sequencerConnectionP,
      )
      signingKeyFingerprint <- signingKeyFingerprintP.traverse(Fingerprint.fromProtoPrimitive)
    } yield InitializeMediatorRequest(
      domainId,
      mediatorId,
      topologyState,
      domainParameters,
      SequencerConnections.single(sequencerConnection),
      signingKeyFingerprint,
    )
  }
}

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
