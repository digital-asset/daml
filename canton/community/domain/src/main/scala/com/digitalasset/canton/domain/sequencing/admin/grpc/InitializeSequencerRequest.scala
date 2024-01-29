// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.admin.grpc

import cats.syntax.traverse.*
import com.digitalasset.canton.domain.admin.v30
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.topology.transaction.TopologyChangeOpX
import com.google.protobuf.ByteString

final case class InitializeSequencerRequestX(
    topologySnapshot: GenericStoredTopologyTransactionsX,
    domainParameters: StaticDomainParameters,
    sequencerSnapshot: Option[SequencerSnapshot] =
      None, // this will likely be a different type for X nodes
) {
  def toProtoV30: v30.InitializeSequencerRequest = {
    v30.InitializeSequencerRequest(
      Some(topologySnapshot.toProtoV30),
      Some(domainParameters.toProtoV30),
      sequencerSnapshot.fold(ByteString.EMPTY)(_.toProtoVersioned.toByteString),
    )
  }
}

object InitializeSequencerRequestX {

  private[sequencing] def fromProtoV2(
      request: v30.InitializeSequencerRequest
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
