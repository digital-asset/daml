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

  def driverContractMetadata: Either[String, DriverContractMetadata] =
    ContractInstance.driverContractMetadata(inst)
}

object ContractInstance {
  private final case class ContractInstanceImpl[Time <: CreationTime](
      override val inst: FatContractInstance { type CreatedAtTime = Time },
      override val metadata: ContractMetadata,
      override val serialization: ByteString,
  ) extends GenContractInstance {
    override type InstCreatedAtTime = Time
  }

  def unapply[Time <: CreationTime](
      contractInstance: GenContractInstance { type InstCreatedAtTime = Time }
  ): Some[(FatContractInstance { type CreatedAtTime = Time }, ContractMetadata, ByteString)] =
    Some((contractInstance.inst, contractInstance.metadata, contractInstance.serialization))

  def driverContractMetadata(inst: FatContractInstance): Either[String, DriverContractMetadata] =
    if (inst.authenticationData.toByteArray.nonEmpty)
      DriverContractMetadata
        .fromLfBytes(inst.authenticationData.toByteArray)
        .leftMap(err => s"Failed parsing disclosed contract driver contract metadata: $err")
    else
      Left(
        value = "Missing driver contract metadata in provided disclosed contract"
      )

  def toSerializableContract(inst: LfFatContractInst): Either[String, SerializableContract] =
    for {
      _ <- CantonContractIdVersion
        .extractCantonContractIdVersion(inst.contractId)
        .leftMap(err => s"Invalid disclosed contract id: ${err.toString}")
      salt <- driverContractMetadata(inst).map(_.salt)
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
        contractSalt = salt,
      ).leftMap(err => s"Failed creating serializable contract from disclosed contract: $err")

    } yield serializable

  def apply[Time <: CreationTime](
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

  def apply(serializable: SerializableContract): Either[String, ContractInstance] =
    for {
      contractIdVersion <- CantonContractIdVersion
        .extractCantonContractIdVersion(serializable.contractId)
        .leftMap(err => s"Invalid disclosed contract id: ${err.toString}")
      inst = FatContractInstance.fromCreateNode(
        serializable.toLf,
        serializable.ledgerCreateTime,
        DriverContractMetadata(serializable.contractSalt).toLfBytes(contractIdVersion),
      )
      serialization <- encodeInst(inst)
    } yield {
      ContractInstanceImpl(inst, serializable.metadata, serialization)
    }

  def decode(bytes: ByteString): Either[String, GenContractInstance] =
    for {
      decoded <- TransactionCoder
        .decodeFatContractInstance(bytes)
        .leftMap(e => s"Failed to decode contract instance: $e")
      contract <- apply[decoded.CreatedAtTime](decoded)
    } yield contract

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def decodeWithCreatedAt(bytes: ByteString): Either[String, ContractInstance] =
    decode(bytes).flatMap { decoded =>
      decoded.inst.createdAt match {
        case _: CreationTime.CreatedAt =>
          Right(decoded.asInstanceOf[ContractInstance])
        case _ =>
          Left(
            s"Creation time must be CreatedAt for contract instances with id ${decoded.contractId}"
          )
      }
    }

  def decodeCreated(bytes: ByteString): Either[String, NewContractInstance] = decode(bytes)

  def assignCreationTime(created: NewContractInstance, let: CantonTimestamp): ContractInstance =
    created match {
      case c: ContractInstanceImpl[_] =>
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
