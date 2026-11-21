// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.ContractInstance.contractAuthenticationData
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{admin, protocol}
import com.digitalasset.daml.lf.transaction.{CreationTime, Versioned}
import com.digitalasset.daml.lf.value.ValueCoder
import com.google.protobuf.ByteString
import io.scalaland.chimney.dsl.*
import org.jetbrains.annotations.TestOnly

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
    ledgerCreateTime: CreationTime.CreatedAt,
    authenticationData: ContractAuthenticationData,
)
// The class implements `HasVersionedWrapper` because we serialize it to an anonymous binary format (ByteString/Array[Byte]) when
// writing to the ReassignmentStore and thus need to encode the version of the serialized Protobuf message
    extends HasVersionedWrapper[SerializableContract]
    with PrettyPrinting {

  def contractInstance: LfThinContractInst = rawContractInstance.contractInstance

  override protected def companionObj: HasVersionedMessageCompanionCommon[SerializableContract] =
    SerializableContract

  def toProtoV30: protocol.v30.SerializableContract =
    protocol.v30.SerializableContract(
      contractId = contractId.toProtoPrimitive,
      rawContractInstance = rawContractInstance.getCryptographicEvidence,
      // Even though [[ContractMetadata]] also implements `HasVersionedWrapper`, we explicitly use Protobuf V30
      // -> we only use `UntypedVersionedMessage` when required and not for 'regularly' nested Protobuf messages
      metadata = Some(metadata.toProtoV30),
      ledgerCreateTime = CantonTimestamp(ledgerCreateTime.time).toProtoPrimitive,
      authenticationData = authenticationData.toSerializableContractProtoV30,
    )

  def toAdminProtoV30: admin.participant.v30.Contract =
    admin.participant.v30.Contract(
      contractId = contractId.toProtoPrimitive,
      rawContractInstance = rawContractInstance.getCryptographicEvidence,
      // Even though [[ContractMetadata]] also implements `HasVersionedWrapper`, we explicitly use Protobuf V30
      // -> we only use `UntypedVersionedMessage` when required and not for 'regularly' nested Protobuf messages
      metadata = Some(metadata.toProtoV30.transformInto[admin.participant.v30.Contract.Metadata]),
      ledgerCreateTime = Some(CantonTimestamp(ledgerCreateTime.time).toProtoTimestamp),
      authenticationData = authenticationData.toSerializableContractAdminProtoV30,
    )

  override protected def pretty: Pretty[SerializableContract] = prettyOfClass(
    param("contractId", _.contractId),
    paramWithoutValue("instance"), // Do not leak confidential data (such as PII) to the log file!
    param("metadata", _.metadata),
    param("create time", _.ledgerCreateTime),
    param("authentication data", _.authenticationData),
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

  @TestOnly
  def tryToContractInstance(): ContractInstance = ContractInstance
    .fromSerializable(this)
    .valueOr(err =>
      throw new IllegalArgumentException(s"Failed to convert to contract instance: $err")
    )

}

object SerializableContract
    extends HasVersionedMessageCompanion[SerializableContract]
    with HasVersionedMessageCompanionDbHelpers[SerializableContract] {
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v34,
      supportedProtoVersion(protocol.v30.SerializableContract)(fromProtoV30),
      _.toProtoV30,
    )
  )

  override def name: String = "serializable contract"

  def apply(
      contractId: LfContractId,
      contractInstance: LfThinContractInst,
      metadata: ContractMetadata,
      ledgerTime: CantonTimestamp,
      authenticationData: ContractAuthenticationData,
  ): Either[ValueCoder.EncodeError, SerializableContract] =
    SerializableRawContractInstance
      .create(contractInstance)
      .map(
        SerializableContract(
          contractId,
          _,
          metadata,
          CreationTime.CreatedAt(ledgerTime.toLf),
          authenticationData,
        )
      )

  def fromProtoV30(
      serializableContractInstanceP: protocol.v30.SerializableContract
  ): ParsingResult[SerializableContract] = {
    val protocol.v30.SerializableContract(
      contractIdP,
      rawP,
      metadataP,
      ledgerCreateTimeP,
      authenticationDataP,
    ) = serializableContractInstanceP

    for {
      ledgerCreateTime <- CantonTimestamp.fromProtoPrimitive(ledgerCreateTimeP)
      contractId <- ProtoConverter.parseLfContractId(contractIdP)
      contractIdVersion <- CantonContractIdVersion
        .extractCantonContractIdVersion(contractId)
        .leftMap(err => ValueConversionError("contract_id", err.toString))
      authenticationData <- ContractAuthenticationData
        .fromSerializableContractProtoV30(contractIdVersion, authenticationDataP)
      contract <- toSerializableContract(
        contractId,
        rawP,
        metadataP,
        CreationTime.CreatedAt(ledgerCreateTime.toLf),
        authenticationData,
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
      authenticationDataP,
    ) = contractP

    for {
      ledgerCreateTime <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoTimestamp,
        "ledger_create_time",
        ledgerCreateTimeP,
      )
      contractId <- ProtoConverter.parseLfContractId(contractIdP)
      contractIdVersion <- CantonContractIdVersion
        .extractCantonContractIdVersion(contractId)
        .leftMap(err => ValueConversionError("contract_id", err.toString))
      authenticationData <- ContractAuthenticationData
        .fromSerializableContractAdminProtoV30(contractIdVersion, authenticationDataP)
      contract <- toSerializableContract(
        contractId,
        rawP,
        metadataP.transformInto[Option[protocol.v30.SerializableContract.Metadata]],
        CreationTime.CreatedAt(ledgerCreateTime.toLf),
        authenticationData,
      )
    } yield contract
  }

  private def toSerializableContract(
      contractId: LfContractId,
      rawP: ByteString,
      metadataP: Option[protocol.v30.SerializableContract.Metadata],
      ledgerCreateTime: CreationTime.CreatedAt,
      authenticationData: ContractAuthenticationData,
  ): ParsingResult[SerializableContract] =
    for {
      raw <- SerializableRawContractInstance
        .fromByteString(rawP)
        .leftMap(error => ValueConversionError("raw_contract_instance", error.toString))
      metadata <- ProtoConverter
        .required("metadata", metadataP)
        .flatMap(ContractMetadata.fromProtoV30)
    } yield SerializableContract(
      contractId,
      raw,
      metadata,
      ledgerCreateTime,
      authenticationData,
    )

  def fromLfFatContractInst(inst: LfFatContractInst): Either[String, SerializableContract] =
    for {
      contractIdVersion <- CantonContractIdVersion
        .extractCantonContractIdVersion(inst.contractId)
        .leftMap(err => s"Invalid disclosed contract id: $err")
      authenticationData <- contractAuthenticationData(contractIdVersion, inst)
      metadata <- ContractMetadata.create(
        signatories = inst.signatories,
        stakeholders = inst.stakeholders,
        maybeKeyWithMaintainersVersioned =
          inst.contractKeyWithMaintainers.map(Versioned(inst.version, _)),
      )
      serializable <- SerializableContract(
        contractId = inst.contractId,
        contractInstance = inst.toCreateNode.versionedCoinst,
        metadata = metadata,
        ledgerTime = CantonTimestamp(inst.createdAt.time),
        authenticationData = authenticationData,
      ).leftMap(err => s"Failed creating serializable contract from disclosed contract: $err")

    } yield serializable

}
