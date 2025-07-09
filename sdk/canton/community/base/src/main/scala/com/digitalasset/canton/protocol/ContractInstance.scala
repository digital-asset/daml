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

final case class ContractInstance private (
    inst: LfFatContractInst,
    metadata: ContractMetadata,
    serialization: ByteString,
) extends PrettyPrinting {

  def contractId: LfContractId = inst.contractId
  def templateId: LfTemplateId = inst.templateId
  def stakeholders: Set[LfPartyId] = inst.stakeholders
  def signatories: Set[LfPartyId] = inst.signatories
  def contractKeyWithMaintainers: Option[LfGlobalKeyWithMaintainers] =
    inst.contractKeyWithMaintainers
  def toLf: LfNodeCreate = inst.toCreateNode

  override protected def pretty: Pretty[ContractInstance] = prettyOfClass(
    param("contractId", _.contractId),
    param("metadata", _.metadata),
    param("created at", _.inst.createdAt),
  )

  def encoded: ByteString = serialization

  def driverContractMetadata: Either[String, DriverContractMetadata] =
    ContractInstance.driverContractMetadata(inst)

}

object ContractInstance {

  def driverContractMetadata(inst: LfFatContractInst): Either[String, DriverContractMetadata] =
    if (inst.cantonData.toByteArray.nonEmpty)
      DriverContractMetadata
        .fromLfBytes(inst.cantonData.toByteArray)
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

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def apply(inst: FatContractInstance): Either[String, ContractInstance] =
    for {
      inst <- inst.createdAt match {
        case _: CreationTime.CreatedAt => Right(inst.asInstanceOf[LfFatContractInst])
        case _ => Left("Creation time must be CreatedAt for contract instances")
      }
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
    } yield ContractInstance(inst, metadata, serialization)

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
      ContractInstance(inst, serializable.metadata, serialization)
    }

  def decode(bytes: ByteString): Either[String, ContractInstance] =
    for {
      decoded <- TransactionCoder
        .decodeFatContractInstance(bytes)
        .leftMap(e => s"Failed to decode contract instance: $e")
      contract <- apply(decoded)
    } yield contract

  private def encodeInst(inst: LfFatContractInst): Either[String, ByteString] =
    TransactionCoder
      .encodeFatContractInstance(inst)
      .leftMap(e => s"Failed to encode contract instance: $e")

}
