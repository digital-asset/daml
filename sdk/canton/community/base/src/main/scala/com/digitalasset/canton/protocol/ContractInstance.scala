// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.daml.lf.transaction.{
  CreationTime,
  FatContractInstance,
  TransactionCoder,
  Versioned,
}
import com.google.protobuf.ByteString

/** Wraps a [[com.digitalasset.daml.lf.transaction.FatContractInstance]] and ensures the following
  * invariants via smart constructors:
  *   - The contract instance can be serialized
  *   - The contract ID format is known
  */
sealed trait GenContractInstance extends PrettyPrinting {
  type InstCreatedAtTime <: CreationTime

  val inst: FatContractInstance { type CreatedAtTime = InstCreatedAtTime }
  def metadata: ContractMetadata
  def serialization: ByteString

  def contractId: LfContractId = inst.contractId
  def templateId: LfTemplateId = inst.templateId
  def stakeholders: Set[LfPartyId] = inst.stakeholders
  def signatories: Set[LfPartyId] = inst.signatories
  def contractKeyWithMaintainers: Option[LfGlobalKeyWithMaintainers] =
    inst.contractKeyWithMaintainers
  def toLf: LfNodeCreate = inst.toCreateNode

  override protected def pretty: Pretty[GenContractInstance] =
    ContractInstance.prettyGenContractInstance

  def encoded: ByteString = serialization

  def contractAuthenticationData: Either[String, ContractAuthenticationData] =
    ContractInstance.contractAuthenticationData(inst)

  def traverseCreatedAt[NewCreatedAtTime <: CreationTime](
      f: InstCreatedAtTime => Either[String, NewCreatedAtTime]
  ): Either[String, GenContractInstance { type InstCreatedAtTime <: NewCreatedAtTime }]
}

object ContractInstance {
  // TODO(#28382) revert removal of private access modifier
  final case class ContractInstanceImpl[Time <: CreationTime](
      override val inst: FatContractInstance { type CreatedAtTime = Time },
      override val metadata: ContractMetadata,
      override val serialization: ByteString,
  ) extends GenContractInstance {
    override type InstCreatedAtTime = Time

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    override def traverseCreatedAt[NewCreatedAtTime <: CreationTime](
        f: Time => Either[String, NewCreatedAtTime]
    ): Either[String, GenContractInstance { type InstCreatedAtTime <: NewCreatedAtTime }] =
      inst.traverseCreateAt(f).flatMap { fci =>
        if (fci eq inst)
          Right(
            this.asInstanceOf[GenContractInstance { type InstCreatedAtTime <: NewCreatedAtTime }]
          )
        else
          ContractInstance.create[NewCreatedAtTime](fci)
      }
  }

  def unapply[Time <: CreationTime](
      contractInstance: GenContractInstance { type InstCreatedAtTime = Time }
  ): Some[(FatContractInstance { type CreatedAtTime = Time }, ContractMetadata, ByteString)] =
    Some((contractInstance.inst, contractInstance.metadata, contractInstance.serialization))

  def contractAuthenticationData(
      inst: FatContractInstance
  ): Either[String, ContractAuthenticationData] =
    CantonContractIdVersion
      .extractCantonContractIdVersion(inst.contractId)
      .flatMap(contractAuthenticationData(_, inst))

  private[protocol] def contractAuthenticationData(
      contractIdVersion: CantonContractIdVersion,
      inst: FatContractInstance,
  ): Either[String, contractIdVersion.AuthenticationData] =
    if (inst.authenticationData.toByteArray.nonEmpty)
      ContractAuthenticationData
        .fromLfBytes(contractIdVersion, inst.authenticationData)
        .leftMap(err => s"Failed parsing disclosed contract authentication data: $err")
    else Left("Missing authentication data in provided disclosed contract")

  def toSerializableContract(inst: LfFatContractInst): Either[String, SerializableContract] =
    for {
      contractIdVersion <- CantonContractIdVersion
        .extractCantonContractIdVersion(inst.contractId)
        .leftMap(err => s"Invalid disclosed contract id: ${err.toString}")
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

  def create[Time <: CreationTime](
      inst: FatContractInstance { type CreatedAtTime <: Time }
  ): Either[String, GenContractInstance { type InstCreatedAtTime <: Time }] =
    for {
      _ <- CantonContractIdVersion
        .extractCantonContractIdVersion(inst.contractId)
        .leftMap(err => s"Invalid disclosed contract id: ${err.toString}")
      serialization <- encodeInst(inst)
      metadata <- ContractMetadata.create(
        signatories = inst.signatories,
        stakeholders = inst.stakeholders,
        maybeKeyWithMaintainersVersioned =
          inst.contractKeyWithMaintainers.map(Versioned(inst.version, _)),
      )
    } yield ContractInstanceImpl[inst.CreatedAtTime](inst, metadata, serialization)

  def fromSerializable(serializable: SerializableContract): Either[String, ContractInstance] = {
    val inst = FatContractInstance.fromCreateNode(
      serializable.toLf,
      serializable.ledgerCreateTime,
      serializable.authenticationData.toLfBytes,
    )
    for {
      serialization <- encodeInst(inst)
    } yield ContractInstanceImpl(inst, serializable.metadata, serialization)
  }

  def decode(bytes: ByteString): Either[String, GenContractInstance] =
    for {
      decoded <- TransactionCoder
        .decodeFatContractInstance(bytes)
        .leftMap(e => s"Failed to decode contract instance: $e")
      contract <- create[decoded.CreatedAtTime](decoded)
    } yield contract

  def decodeWithCreatedAt(bytes: ByteString): Either[String, ContractInstance] =
    decode(bytes).flatMap { decoded =>
      decoded.traverseCreatedAt {
        case createdAt: CreationTime.CreatedAt => Right(createdAt)
        case _ =>
          Left(
            s"Creation time must be CreatedAt for contract instances with id ${decoded.contractId}"
          )
      }
    }

  def decodeCreated(bytes: ByteString): Either[String, NewContractInstance] = decode(bytes)

  def assignCreationTime(created: NewContractInstance, let: CantonTimestamp): ContractInstance =
    created match {
      case c: ContractInstanceImpl[?] =>
        c.copy(inst = c.inst.updateCreateAt(let.toLf))
    }

  private def encodeInst(inst: FatContractInstance): Either[String, ByteString] =
    TransactionCoder
      .encodeFatContractInstance(inst)
      .leftMap(e => s"Failed to encode contract instance: $e")

  val prettyGenContractInstance: Pretty[GenContractInstance] = {
    import com.digitalasset.canton.logging.pretty.PrettyUtil.*
    prettyOfClass(
      param("contractId", _.contractId),
      param("metadata", _.metadata),
      param("created at", _.inst.createdAt),
    )
  }
}
