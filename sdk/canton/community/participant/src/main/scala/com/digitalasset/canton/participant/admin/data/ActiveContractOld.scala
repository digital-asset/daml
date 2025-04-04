// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import better.files.File
import cats.syntax.either.*
import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.protocol.messages.HasSynchronizerId
import com.digitalasset.canton.protocol.{HasSerializableContract, SerializableContract}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.{ByteStringUtil, GrpcStreamingUtils, ResourceUtil}
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

import java.io.{ByteArrayInputStream, InputStream}

// TODO(#24610) – Remove; replaced by the new ActiveContract that uses LAPI active contract
final case class ActiveContractOld(
    synchronizerId: SynchronizerId,
    contract: SerializableContract,
    reassignmentCounter: ReassignmentCounter,
)(override val representativeProtocolVersion: RepresentativeProtocolVersion[ActiveContractOld.type])
    extends HasProtocolVersionedWrapper[ActiveContractOld]
    with HasSynchronizerId
    with HasSerializableContract {
  def toProtoV30: v30.ActiveContractOld =
    v30.ActiveContractOld(
      synchronizerId.toProtoPrimitive,
      Some(contract.toAdminProtoV30),
      reassignmentCounter.toProtoPrimitive,
    )

  override protected lazy val companionObj: ActiveContractOld.type = ActiveContractOld

  private[canton] def withSerializableContract(
      contract: SerializableContract
  ): ActiveContractOld =
    copy(contract = contract)(representativeProtocolVersion)

  private[admin] def toRepairContract: RepairContract =
    RepairContract(synchronizerId, contract, reassignmentCounter)

}

private[canton] object ActiveContractOld extends VersioningCompanion[ActiveContractOld] {

  override def name: String = "ActiveContractOld"

  override def versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v33)(v30.ActiveContractOld)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def fromProtoV30(
      proto: v30.ActiveContractOld
  ): ParsingResult[ActiveContractOld] =
    for {
      synchronizerId <- SynchronizerId.fromProtoPrimitive(proto.synchronizerId, "synchronizer_id")
      contract <- ProtoConverter.parseRequired(
        SerializableContract.fromAdminProtoV30,
        "contract",
        proto.contract,
      )
      reassignmentCounter = proto.reassignmentCounter
      reprProtocolVersion <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield {
      ActiveContractOld(synchronizerId, contract, ReassignmentCounter(reassignmentCounter))(
        reprProtocolVersion
      )
    }

  def create(
      synchronizerId: SynchronizerId,
      contract: SerializableContract,
      reassignmentCounter: ReassignmentCounter,
  )(protocolVersion: ProtocolVersion): ActiveContractOld =
    ActiveContractOld(synchronizerId, contract, reassignmentCounter)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  // TODO(#24728) - Remove, do not depend on reading ACS from file directly
  private[canton] def fromFile(fileInput: File): Iterator[ActiveContractOld] =
    ResourceUtil.withResource(fileInput.newGzipInputStream(8192)) { fileInput =>
      loadFromSource(fileInput) match {
        case Left(error) => throw new Exception(error)
        case Right(value) => value.iterator
      }
    }

  private[admin] def loadFromByteString(
      bytes: ByteString
  ): Either[String, List[ActiveContractOld]] =
    for {
      decompressedBytes <-
        ByteStringUtil
          .decompressGzip(bytes, None)
          .leftMap(err => s"Failed to decompress bytes: $err")
      contracts <- ResourceUtil.withResource(
        new ByteArrayInputStream(decompressedBytes.toByteArray)
      ) { inputSource =>
        loadFromSource(inputSource)
      }
    } yield contracts

  private def loadFromSource(
      source: InputStream
  ): Either[String, List[ActiveContractOld]] =
    GrpcStreamingUtils
      .parseDelimitedFromTrusted[ActiveContractOld](
        source,
        ActiveContractOld,
      )
      .map(_.toList)
}
