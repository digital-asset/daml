// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    // Even if implementing HasVersionedWrapper, we should still implement HasProtoV0
    with PrettyPrinting {

  def contractInstance: LfContractInst = rawContractInstance.contractInstance

  override protected def companionObj = SerializableContract

  def toProtoV0: v0.SerializableContract =
    v0.SerializableContract(
      contractId = contractId.toProtoPrimitive,
      rawContractInstance = rawContractInstance.getCryptographicEvidence,
      // Even though [[ContractMetadata]] also implements `HasVersionedWrapper`, we explicitly use Protobuf V0
      // -> we only use `UntypedVersionedMessage` when required and not for 'regularly' nested Protobuf messages
      metadata = Some(metadata.toProtoV0),
      ledgerCreateTime = Some(ledgerCreateTime.toProtoPrimitive),
    )

  def toProtoV1: v1.SerializableContract =
    v1.SerializableContract(
      contractId = contractId.toProtoPrimitive,
      rawContractInstance = rawContractInstance.getCryptographicEvidence,
      // Even though [[ContractMetadata]] also implements `HasVersionedWrapper`, we explicitly use Protobuf V0
      // -> we only use `UntypedVersionedMessage` when required and not for 'regularly' nested Protobuf messages
      metadata = Some(metadata.toProtoV0),
      ledgerCreateTime = Some(ledgerCreateTime.toProtoPrimitive),
      // Contract salt can be empty for contracts created in protocol versions < 4.
      contractSalt = contractSalt.map(_.toProtoV0),
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
    agreementText = rawContractInstance.unvalidatedAgreementText.v,
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
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v3,
      supportedProtoVersion(v0.SerializableContract)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> ProtoCodec(
      ProtocolVersion.v4,
      supportedProtoVersion(v1.SerializableContract)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
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
      unvalidatedAgreementText: AgreementText,
  ): Either[ValueCoder.EncodeError, SerializableContract] =
    SerializableRawContractInstance
      .create(contractInstance, unvalidatedAgreementText)
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
      disclosedContractIdVersion <- CantonContractIdVersion
        .ensureCantonContractId(disclosedContract.contractId)
        .leftMap(err => s"Invalid disclosed contract id: ${err.toString}")
      _ <- disclosedContractIdVersion match {
        case NonAuthenticatedContractIdVersion =>
          Left(
            s"Disclosed contract with non-authenticated contract id: ${disclosedContract.contractId.toString}"
          )
        case AuthenticatedContractIdVersion | AuthenticatedContractIdVersionV2 => Right(())
      }
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
        unvalidatedAgreementText = AgreementText(create.agreementText),
      ).leftMap(err => s"Failed creating serializable contract from disclosed contract: $err")
    } yield contract
  }

  def fromProtoV0(
      serializableContractInstanceP: v0.SerializableContract
  ): ParsingResult[SerializableContract] = {
    val v0.SerializableContract(contractIdP, rawP, metadataP, ledgerCreateTime) =
      serializableContractInstanceP

    toSerializableContract(contractIdP, rawP, metadataP, ledgerCreateTime, None)
  }

  def fromProtoV1(
      serializableContractInstanceP: v1.SerializableContract
  ): ParsingResult[SerializableContract] = {
    val v1.SerializableContract(contractIdP, rawP, metadataP, ledgerCreateTime, contractSaltP) =
      serializableContractInstanceP

    toSerializableContract(contractIdP, rawP, metadataP, ledgerCreateTime, contractSaltP)
  }

  private def toSerializableContract(
      contractIdP: String,
      rawP: ByteString,
      metadataP: Option[v0.SerializableContract.Metadata],
      ledgerCreateTime: Option[Timestamp],
      contractSaltO: Option[crypto.v0.Salt],
  ): ParsingResult[SerializableContract] =
    for {
      contractId <- ProtoConverter.parseLfContractId(contractIdP)
      raw <- SerializableRawContractInstance
        .fromByteString(rawP)
        .leftMap(error => ValueConversionError("raw_contract_instance", error.toString))
      metadata <- ProtoConverter
        .required("metadata", metadataP)
        .flatMap(ContractMetadata.fromProtoV0)
      ledgerTime <- ProtoConverter
        .required("ledger_create_time", ledgerCreateTime)
        .flatMap(CantonTimestamp.fromProtoPrimitive)
      contractSalt <- contractSaltO.traverse(Salt.fromProtoV0)
    } yield SerializableContract(
      contractId,
      raw,
      metadata,
      LedgerCreateTime(ledgerTime),
      contractSalt,
    )
}
