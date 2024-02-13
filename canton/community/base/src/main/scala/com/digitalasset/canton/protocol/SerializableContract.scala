// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.implicits.toTraverseOps
import cats.syntax.either.*
import com.daml.lf.value.ValueCoder
import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.crypto.Salt
import com.digitalasset.canton.data.{CantonTimestamp, ProcessedDisclosedContract}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{LfTimestamp, crypto}
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp

import java.time.Instant

/** Represents a serializable contract.
  *
  * @param contractId The ID of the contract.
  * @param rawContractInstance The raw instance of the contract.
  * @param metadata The metadata with stakeholders and signatories; can be computed from contract instance
  * @param ledgerCreateTime The ledger time of the transaction '''creating''' the contract
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
    contractSalt: Option[Salt],
)
// The class implements `HasVersionedWrapper` because we serialize it to an anonymous binary format (ByteString/Array[Byte]) when
// writing to the TransferStore and thus need to encode the version of the serialized Protobuf message
    extends HasVersionedWrapper[SerializableContract]
    with PrettyPrinting {

  def contractInstance: LfContractInst = rawContractInstance.contractInstance

  override protected def companionObj: HasVersionedMessageCompanionCommon[SerializableContract] =
    SerializableContract

  def toProtoV30: v30.SerializableContract =
    v30.SerializableContract(
      contractId = contractId.toProtoPrimitive,
      rawContractInstance = rawContractInstance.getCryptographicEvidence,
      // Even though [[ContractMetadata]] also implements `HasVersionedWrapper`, we explicitly use Protobuf V30
      // -> we only use `UntypedVersionedMessage` when required and not for 'regularly' nested Protobuf messages
      metadata = Some(metadata.toProtoV30),
      ledgerCreateTime = Some(ledgerCreateTime.toProtoPrimitive),
      // Contract salt can be empty for contracts created in protocol versions < 4.
      contractSalt = contractSalt.map(_.toProtoV30),
    )

  override def pretty: Pretty[SerializableContract] = prettyOfClass(
    param("contractId", _.contractId),
    paramWithoutValue("instance"), // Do not leak confidential data (such as PII) to the log file!
    param("metadata", _.metadata),
    param("create time", _.ledgerCreateTime.ts),
    paramIfDefined("contract salt", _.contractSalt),
  )

  def toLf: LfNodeCreate = LfNodeCreate(
    coid = contractId,
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
      ProtocolVersion.v30,
      supportedProtoVersion(v30.SerializableContract)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  override def name: String = "serializable contract"

  // Ledger time of the "repair transaction" creating the contract
  final case class LedgerCreateTime(ts: CantonTimestamp) extends AnyVal {
    def toProtoPrimitive: Timestamp = ts.toProtoPrimitive
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
      contractSalt: Option[Salt],
  ): Either[ValueCoder.EncodeError, SerializableContract] =
    SerializableRawContractInstance
      .create(contractInstance)
      .map(
        SerializableContract(contractId, _, metadata, LedgerCreateTime(ledgerTime), contractSalt)
      )

  def fromDisclosedContract(
      disclosedContract: ProcessedDisclosedContract
  ): Either[String, SerializableContract] = {
    val create = disclosedContract.create
    val ledgerTime = CantonTimestamp(disclosedContract.createdAt)
    val driverContractMetadataBytes = disclosedContract.driverMetadata.toByteArray

    for {
      _disclosedContractIdVersion <- CantonContractIdVersion
        .ensureCantonContractId(disclosedContract.contractId)
        .leftMap(err => s"Invalid disclosed contract id: ${err.toString}")
      salt <- {
        if (driverContractMetadataBytes.isEmpty)
          Left[String, Option[Salt]](
            value = "Missing driver contract metadata in provided disclosed contract"
          )
        else
          DriverContractMetadata
            .fromByteArray(driverContractMetadataBytes)
            .leftMap(err => s"Failed parsing disclosed contract driver contract metadata: $err")
            .map(m => Some(m.salt))
      }
      contractInstance = create.versionedCoinst
      cantonContractMetadata <- ContractMetadata.create(
        signatories = create.signatories,
        stakeholders = create.stakeholders,
        maybeKeyWithMaintainers = create.versionedKeyOpt,
      )
      contract <- SerializableContract(
        contractId = disclosedContract.contractId,
        contractInstance = contractInstance,
        metadata = cantonContractMetadata,
        ledgerTime = ledgerTime,
        contractSalt = salt,
      ).leftMap(err => s"Failed creating serializable contract from disclosed contract: $err")
    } yield contract
  }

  def fromProtoV30(
      serializableContractInstanceP: v30.SerializableContract
  ): ParsingResult[SerializableContract] = {
    val v30.SerializableContract(contractIdP, rawP, metadataP, ledgerCreateTime, contractSaltP) =
      serializableContractInstanceP

    toSerializableContract(contractIdP, rawP, metadataP, ledgerCreateTime, contractSaltP)
  }

  private def toSerializableContract(
      contractIdP: String,
      rawP: ByteString,
      metadataP: Option[v30.SerializableContract.Metadata],
      ledgerCreateTime: Option[Timestamp],
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
      ledgerTime <- ProtoConverter
        .required("ledger_create_time", ledgerCreateTime)
        .flatMap(CantonTimestamp.fromProtoPrimitive)
      contractSalt <- contractSaltO.traverse(Salt.fromProtoV30)
    } yield SerializableContract(
      contractId,
      raw,
      metadata,
      LedgerCreateTime(ledgerTime),
      contractSalt,
    )
}
