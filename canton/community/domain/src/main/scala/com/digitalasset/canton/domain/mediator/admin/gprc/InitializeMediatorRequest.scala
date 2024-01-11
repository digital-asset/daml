// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.admin.gprc

import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.domain.admin.{v0, v2}
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
  def toProtoV0: v0.InitializeMediatorRequest =
    v0.InitializeMediatorRequest(
      domainId.toProtoPrimitive,
      mediatorId.uid.toProtoPrimitive,
      topologyState.map(_.toProtoV0),
      Some(domainParameters.toProtoV0),
      // Non-BFT domain is only supporting a single sequencer connection
      Some(sequencerConnections.default.toProtoV0),
      signingKeyFingerprint.map(_.toProtoPrimitive),
    )
}

object InitializeMediatorRequest {
  def fromProtoV0(
      requestP: v0.InitializeMediatorRequest
  ): ParsingResult[InitializeMediatorRequest] = {
    val v0.InitializeMediatorRequest(
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
        .flatMap(StaticDomainParameters.fromProtoV0)
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
  def toProtoV2: v2.InitializeMediatorRequest =
    v2.InitializeMediatorRequest(
      domainId.toProtoPrimitive,
      Some(domainParameters.toProtoV1),
      sequencerConnections.toProtoV0,
      sequencerConnections.sequencerTrustThreshold.unwrap,
    )
}

object InitializeMediatorRequestX {
  def fromProtoV2(
      requestP: v2.InitializeMediatorRequest
  ): ParsingResult[InitializeMediatorRequestX] = {
    val v2.InitializeMediatorRequest(
      domainIdP,
      domainParametersP,
      sequencerConnectionP,
      sequencerTrustThreshold,
    ) = requestP
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      domainParameters <- ProtoConverter
        .required("domain_parameters", domainParametersP)
        .flatMap(StaticDomainParameters.fromProtoV1)
      sequencerConnections <- SequencerConnections.fromProtoV0(
        sequencerConnectionP,
        sequencerTrustThreshold,
      )
    } yield InitializeMediatorRequestX(
      domainId,
      domainParameters,
      sequencerConnections,
    )
  }
}
