// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.crypto.Salt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{LfTimestamp, admin, crypto, protocol}
import com.digitalasset.daml.lf.transaction.{FatContractInstance, Versioned}
import com.digitalasset.daml.lf.value.ValueCoder
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import io.scalaland.chimney.dsl.*

import java.time.Instant

/** Represents a serializable contract.
  *
  * @param contractId
  *   The ID of the contract.
  * @param rawContractInstance
  *   The raw instance of the contract.
  * @param metadata
  *   The metadata with stakeholders and signatories; can be computed from contract instance
  * @param ledgerCreateTime
  *   The ledger time of the transaction '''creating''' the contract
  */
// This class is a reference example of serialization best practices, demonstrating:
// - use of an UntypedVersionedMessage wrapper when serializing to an anonymous binary format. For a more extensive example of this,
// please also see the writeup under `Backwards-incompatible Protobuf changes` in `CONTRIBUTING.md`.

// Please consult the team if you intend to change the design of serialization.
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class SerializableContract(
    contractId: LfContractId,
    rawContractInstance: SerializableRawContractInstance,
    metadata: ContractMetadata,
    ledgerCreateTime: LedgerCreateTime,
    contractSalt: Salt,
)
// The class implements `HasVersionedWrapper` because we serialize it to an anonymous binary format (ByteString/Array[Byte]) when
// writing to the ReassignmentStore and thus need to encode the version of the serialized Protobuf message
    extends HasVersionedWrapper[SerializableContract]
    with PrettyPrinting {

  def contractInstance: LfContractInst = rawContractInstance.contractInstance

  override protected def companionObj: HasVersionedMessageCompanionCommon[SerializableContract] =
    SerializableContract

  def toProtoV30: protocol.v30.SerializableContract =
    protocol.v30.SerializableContract(
      contractId = contractId.toProtoPrimitive,
      rawContractInstance = rawContractInstance.getCryptographicEvidence,
      // Even though [[ContractMetadata]] also implements `HasVersionedWrapper`, we explicitly use Protobuf V30
      // -> we only use `UntypedVersionedMessage` when required and not for 'regularly' nested Protobuf messages
      metadata = Some(metadata.toProtoV30),
      ledgerCreateTime = ledgerCreateTime.ts.toProtoPrimitive,
      // Contract salt can be empty for contracts created in protocol versions < 4.
      contractSalt = Some(contractSalt.toProtoV30),
    )

  def toAdminProtoV30: admin.participant.v30.Contract =
    admin.participant.v30.Contract(
      contractId = contractId.toProtoPrimitive,
      rawContractInstance = rawContractInstance.getCryptographicEvidence,
      // Even though [[ContractMetadata]] also implements `HasVersionedWrapper`, we explicitly use Protobuf V30
      // -> we only use `UntypedVersionedMessage` when required and not for 'regularly' nested Protobuf messages
      metadata = Some(metadata.toProtoV30.transformInto[admin.participant.v30.Contract.Metadata]),
      ledgerCreateTime = Some(ledgerCreateTime.ts.toProtoTimestamp),
      // Contract salt can be empty for contracts created in protocol versions < 4.
      contractSalt = Some(contractSalt.toProtoV30.transformInto[admin.crypto.v30.Salt]),
    )

  override protected def pretty: Pretty[SerializableContract] = prettyOfClass(
    param("contractId", _.contractId),
    paramWithoutValue("instance"), // Do not leak confidential data (such as PII) to the log file!
    param("metadata", _.metadata),
    param("create time", _.ledgerCreateTime.ts),
    param("contract salt", _.contractSalt),
  )

  def toLf: LfNodeCreate = LfNodeCreate(
    coid = contractId,
    packageName = rawContractInstance.contractInstance.unversioned.packageName,
    templateId = rawContractInstance.contractInstance.unversioned.template,
    arg = rawContractInstance.contractInstance.unversioned.arg,
    signatories = metadata.signatories,
    stakeholders = metadata.stakeholders,
    keyOpt = metadata.maybeKeyWithMaintainers,
    version = rawContractInstance.contractInstance.version,
  )

}

object SerializableContract
    extends HasVersionedMessageCompanion[SerializableContract]
    with HasVersionedMessageCompanionDbHelpers[SerializableContract] {
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v33,
      supportedProtoVersion(protocol.v30.SerializableContract)(fromProtoV30),
      _.toProtoV30,
    )
  )

  override def name: String = "serializable contract"

  // Ledger time of the "repair transaction" creating the contract
  final case class LedgerCreateTime(ts: CantonTimestamp) extends AnyVal {
    def toProtoPrimitive: Timestamp = ts.toProtoTimestamp
    def toInstant: Instant = ts.toInstant
    def toLf: LfTimestamp = ts.toLf
  }

  object LedgerCreateTime extends PrettyUtil {
    implicit val ledgerCreateTimeOrdering: Ordering[LedgerCreateTime] = Ordering.by(_.ts)
    implicit val prettyLedgerCreateTime: Pretty[LedgerCreateTime] =
      prettyOfClass[LedgerCreateTime](param("ts", _.ts))
  }

  def apply(
      contractId: LfContractId,
      contractInstance: LfContractInst,
      metadata: ContractMetadata,
      ledgerTime: CantonTimestamp,
      contractSalt: Salt,
  ): Either[ValueCoder.EncodeError, SerializableContract] =
    SerializableRawContractInstance
      .create(contractInstance)
      .map(
        SerializableContract(contractId, _, metadata, LedgerCreateTime(ledgerTime), contractSalt)
      )

  def fromDisclosedContract(
      fat: FatContractInstance
  ): Either[String, SerializableContract] = {
    val ledgerTime = CantonTimestamp(fat.createdAt)
    val driverContractMetadataBytes = fat.cantonData.toByteArray

    for {
      _disclosedContractIdVersion <- CantonContractIdVersion
        .extractCantonContractIdVersion(fat.contractId)
        .leftMap(err => s"Invalid disclosed contract id: ${err.toString}")
      salt <- {
        if (driverContractMetadataBytes.isEmpty)
          Left[String, Salt](
            value = "Missing driver contract metadata in provided disclosed contract"
          )
        else
          DriverContractMetadata
            .fromTrustedByteArray(driverContractMetadataBytes)
            .leftMap(err => s"Failed parsing disclosed contract driver contract metadata: $err")
            .map(_.salt)
      }
      contractInstance = Versioned(fat.version, fat.toCreateNode.coinst)
      cantonContractMetadata <- ContractMetadata.create(
        signatories = fat.signatories,
        stakeholders = fat.stakeholders,
        maybeKeyWithMaintainersVersioned =
          fat.contractKeyWithMaintainers.map(Versioned(fat.version, _)),
      )
      contract <- SerializableContract(
        contractId = fat.contractId,
        contractInstance = contractInstance,
        metadata = cantonContractMetadata,
        ledgerTime = ledgerTime,
        contractSalt = salt,
      ).leftMap(err => s"Failed creating serializable contract from disclosed contract: $err")
    } yield contract
  }

  def fromProtoV30(
      serializableContractInstanceP: protocol.v30.SerializableContract
  ): ParsingResult[SerializableContract] = {
    val protocol.v30.SerializableContract(
      contractIdP,
      rawP,
      metadataP,
      ledgerCreateTimeP,
      contractSaltP,
    ) =
      serializableContractInstanceP

    for {
      ledgerCreateTime <- CantonTimestamp.fromProtoPrimitive(ledgerCreateTimeP)
      contract <- toSerializableContract(
        contractIdP,
        rawP,
        metadataP,
        ledgerCreateTime,
        contractSaltP,
      )
    } yield contract
  }

  def fromAdminProtoV30(
      contractP: admin.participant.v30.Contract
  ): ParsingResult[SerializableContract] = {
    val admin.participant.v30.Contract(
      contractIdP,
      rawP,
      metadataP,
      ledgerCreateTimeP,
      contractSaltP,
    ) =
      contractP

    for {
      ledgerCreateTime <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoTimestamp,
        "ledger_create_time",
        ledgerCreateTimeP,
      )
      contract <- toSerializableContract(
        contractIdP,
        rawP,
        metadataP.transformInto[Option[protocol.v30.SerializableContract.Metadata]],
        ledgerCreateTime,
        contractSaltP.transformInto[Option[crypto.v30.Salt]],
      )
    } yield contract
  }

  private def toSerializableContract(
      contractIdP: String,
      rawP: ByteString,
      metadataP: Option[protocol.v30.SerializableContract.Metadata],
      ledgerCreateTime: CantonTimestamp,
      contractSaltO: Option[crypto.v30.Salt],
  ): ParsingResult[SerializableContract] =
    for {
      contractId <- ProtoConverter.parseLfContractId(contractIdP)
      raw <- SerializableRawContractInstance
        .fromByteString(rawP)
        .leftMap(error => ValueConversionError("raw_contract_instance", error.toString))
      metadata <- ProtoConverter
        .required("metadata", metadataP)
        .flatMap(ContractMetadata.fromProtoV30)
      contractSalt <- ProtoConverter.required("salt", contractSaltO).flatMap(Salt.fromProtoV30)
    } yield SerializableContract(
      contractId,
      raw,
      metadata,
      LedgerCreateTime(ledgerCreateTime),
      contractSalt,
    )
}
