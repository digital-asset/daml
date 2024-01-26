// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.admin.grpc

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.domain.admin.{v30 as adminProtoV30, v30old as adminProtoV30Old}
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot
import com.digitalasset.canton.protocol.{StaticDomainParameters, v30}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactions,
  StoredTopologyTransactionsX,
}
import com.digitalasset.canton.topology.transaction.{TopologyChangeOp, TopologyChangeOpX}
import com.digitalasset.canton.topology.{DomainId, UniqueIdentifier}
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

final case class InitializeSequencerRequest(
    domainId: DomainId,
    topologySnapshot: StoredTopologyTransactions[TopologyChangeOp.Positive],
    domainParameters: StaticDomainParameters,
    sequencerSnapshot: Option[SequencerSnapshot] = None,
) extends HasProtocolVersionedWrapper[InitializeSequencerRequest] {

  override val representativeProtocolVersion
      : RepresentativeProtocolVersion[InitializeSequencerRequest.type] =
    InitializeSequencerRequest.protocolVersionRepresentativeFor(domainParameters.protocolVersion)

  @transient override protected lazy val companionObj: InitializeSequencerRequest.type =
    InitializeSequencerRequest

  def toProtoV30Old: adminProtoV30Old.InitRequest = adminProtoV30Old.InitRequest(
    domainId.toProtoPrimitive,
    Some(topologySnapshot.toProtoV30),
    Some(domainParameters.toProtoV30),
    sequencerSnapshot.fold(ByteString.EMPTY)(_.toProtoVersioned.toByteString),
  )
}

object InitializeSequencerRequest
    extends HasProtocolVersionedCompanion[InitializeSequencerRequest] {
  override val name: String = "InitializeSequencerRequest"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.v30)(adminProtoV30Old.InitRequest)(
      supportedProtoVersion(_)(fromProtoV30Old),
      _.toProtoV30Old.toByteString,
    )
  )

  def convertTopologySnapshot(
      transactionsP: v30.TopologyTransactions
  ): ParsingResult[StoredTopologyTransactions[TopologyChangeOp.Positive]] = {
    StoredTopologyTransactions.fromProtoV30(transactionsP).flatMap { topologySnapshot =>
      val topologySnapshotPositive = topologySnapshot.collectOfType[TopologyChangeOp.Positive]
      if (topologySnapshot.result.sizeCompare(topologySnapshotPositive.result) == 0)
        Right(topologySnapshotPositive)
      else
        Left(
          ProtoDeserializationError.InvariantViolation(
            "InitRequest should contain only positive transactions"
          )
        )
    }
  }

  private[sequencing] def fromProtoV30Old(
      request: adminProtoV30Old.InitRequest
  ): ParsingResult[InitializeSequencerRequest] = {
    for {
      domainId <- UniqueIdentifier
        .fromProtoPrimitive(request.domainId, "domain_id")
        .map(DomainId(_))
      domainParameters <- ProtoConverter.parseRequired(
        StaticDomainParameters.fromProtoV30,
        "domain_parameters",
        request.domainParameters,
      )
      topologySnapshotAddO <- request.topologySnapshot.traverse(convertTopologySnapshot)
      topologySnapshotAdd <- topologySnapshotAddO.toRight(
        ProtoDeserializationError.FieldNotSet("topology_snapshot")
      )
      snapshotO <- Option
        .when(!request.snapshot.isEmpty)(
          SequencerSnapshot.fromByteString(domainParameters.protocolVersion)(
            request.snapshot
          )
        )
        .sequence
    } yield InitializeSequencerRequest(
      domainId,
      topologySnapshotAdd,
      domainParameters,
      snapshotO,
    )
  }
}

final case class InitializeSequencerRequestX(
    topologySnapshot: GenericStoredTopologyTransactionsX,
    domainParameters: StaticDomainParameters,
    sequencerSnapshot: Option[SequencerSnapshot] =
      None, // this will likely be a different type for X nodes
) {
  def toProtoV30: adminProtoV30.InitializeSequencerRequest = {
    adminProtoV30.InitializeSequencerRequest(
      Some(topologySnapshot.toProtoV30),
      Some(domainParameters.toProtoV30),
      sequencerSnapshot.fold(ByteString.EMPTY)(_.toProtoVersioned.toByteString),
    )
  }
}

object InitializeSequencerRequestX {

  private[sequencing] def fromProtoV2(
      request: adminProtoV30.InitializeSequencerRequest
  ): ParsingResult[InitializeSequencerRequestX] =
    for {
      domainParameters <- ProtoConverter.parseRequired(
        StaticDomainParameters.fromProtoV30,
        "domain_parameters",
        request.domainParameters,
      )
      topologySnapshotAddO <- request.topologySnapshot.traverse(
        StoredTopologyTransactionsX.fromProtoV30
      )
      snapshotO <- Option
        .when(!request.snapshot.isEmpty)(
          SequencerSnapshot.fromByteString(domainParameters.protocolVersion)(
            request.snapshot
          )
        )
        .sequence
    } yield InitializeSequencerRequestX(
      topologySnapshotAddO
        .getOrElse(StoredTopologyTransactionsX.empty)
        .collectOfType[TopologyChangeOpX.Replace],
      domainParameters,
      snapshotO,
    )
}
