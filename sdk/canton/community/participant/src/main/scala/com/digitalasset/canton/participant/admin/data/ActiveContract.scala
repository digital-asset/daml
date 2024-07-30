// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import better.files.File
import cats.syntax.either.*
import com.digitalasset.canton.TransferCounter
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.protocol.messages.HasDomainId
import com.digitalasset.canton.protocol.{HasSerializableContract, SerializableContract}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.{ByteStringUtil, ResourceUtil}
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

import java.io.{ByteArrayInputStream, InputStream}

final case class ActiveContract(
    domainId: DomainId,
    contract: SerializableContract,
    transferCounter: TransferCounter,
)(override val representativeProtocolVersion: RepresentativeProtocolVersion[ActiveContract.type])
    extends HasProtocolVersionedWrapper[ActiveContract]
    with HasDomainId
    with HasSerializableContract {
  private def toProtoV30: v30.ActiveContract =
    v30.ActiveContract(
      domainId.toProtoPrimitive,
      Some(contract.toAdminProtoV30),
      transferCounter.toProtoPrimitive,
    )

  override protected lazy val companionObj: ActiveContract.type = ActiveContract

  private[canton] def withSerializableContract(
      contract: SerializableContract
  ): ActiveContract =
    copy(contract = contract)(representativeProtocolVersion)

}

private[canton] object ActiveContract extends HasProtocolVersionedCompanion[ActiveContract] {

  override def name: String = "ActiveContract"

  override def supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(v30.ActiveContract)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  private def fromProtoV30(
      proto: v30.ActiveContract
  ): ParsingResult[ActiveContract] =
    for {
      domainId <- DomainId.fromProtoPrimitive(proto.domainId, "domain_id")
      contract <- ProtoConverter.parseRequired(
        SerializableContract.fromAdminProtoV30,
        "contract",
        proto.contract,
      )
      transferCounter = proto.reassignmentCounter
      reprProtocolVersion <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield {
      ActiveContract(domainId, contract, TransferCounter(transferCounter))(reprProtocolVersion)
    }

  def create(
      domainId: DomainId,
      contract: SerializableContract,
      transferCounter: TransferCounter,
  )(protocolVersion: ProtocolVersion): ActiveContract =
    ActiveContract(domainId, contract, transferCounter)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  private[canton] def fromFile(fileInput: File): Iterator[ActiveContract] =
    ResourceUtil.withResource(fileInput.newGzipInputStream(8192)) { fileInput =>
      loadFromSource(fileInput) match {
        case Left(error) => throw new Exception(error)
        case Right(value) => value.iterator
      }
    }

  private[admin] def loadFromByteString(
      bytes: ByteString
  ): Either[String, List[ActiveContract]] =
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

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.While"))
  private def loadFromSource(
      source: InputStream
  ): Either[String, List[ActiveContract]] = {
    // assume we can load everything into memory
    val buf = scala.collection.mutable.ListBuffer.empty[ActiveContract]

    var hasDataInStream = true
    var errorMessageO: Option[String] = None

    while (hasDataInStream && errorMessageO.isEmpty) {
      ActiveContract.parseDelimitedFromTrusted(source) match {
        case None =>
          // parseDelimitedFrom returns None to indicate that there is no more data to read from the input stream
          hasDataInStream = false
        case Some(activeContractE) =>
          activeContractE match {
            case Left(parsingError) =>
              // if there is a deserialization error, let's stop processing and return the error message
              errorMessageO = Some(parsingError.message)
            case Right(activeContract) =>
              buf.addOne(activeContract)
          }
      }
    }

    errorMessageO.toLeft(buf.toList)
  }

}
