// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.implicits.toBifunctorOps
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
    serializable: SerializableContract,
    useUpgradeFriendlyHash: Boolean,
) extends PrettyPrinting {

  def contractId: LfContractId = inst.contractId
  def templateId: LfTemplateId = inst.templateId
  def stakeholders: Set[LfPartyId] = inst.stakeholders
  def signatories: Set[LfPartyId] = inst.signatories
  def contractKeyWithMaintainers: Option[LfGlobalKeyWithMaintainers] =
    inst.contractKeyWithMaintainers
  def toLf: LfNodeCreate = inst.toCreateNode
  def metadata: ContractMetadata = serializable.metadata

  override protected def pretty: Pretty[ContractInstance] = prettyOfClass(
    param("contractId", _.contractId),
    param("metadata", _.metadata),
    param("created at", _.inst.createdAt),
  )

  def encode(): Either[String, ByteString] =
    TransactionCoder
      .encodeFatContractInstance(inst)
      .leftMap(e => s"Failed to encode contract instance: $e")

}

object ContractInstance {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def apply(fat: FatContractInstance): Either[String, ContractInstance] =
    for {
      inst <- fat.createdAt match {
        case _: CreationTime.CreatedAt => Right(fat.asInstanceOf[LfFatContractInst])
        case _ => Left("Creation time must be CreatedAt for contract instances")
      }
      contractIdVersion <- CantonContractIdVersion
        .extractCantonContractIdVersion(inst.contractId)
        .leftMap(err => s"Invalid disclosed contract id: ${err.toString}")
      salt <- {
        if (inst.cantonData.toByteArray.nonEmpty)
          DriverContractMetadata
            .fromLfBytes(inst.cantonData.toByteArray)
            .leftMap(err => s"Failed parsing disclosed contract driver contract metadata: $err")
            .map(_.salt)
        else
          Left(
            value = "Missing driver contract metadata in provided disclosed contract"
          )
      }
      cantonContractMetadata <- ContractMetadata.create(
        signatories = inst.signatories,
        stakeholders = inst.stakeholders,
        maybeKeyWithMaintainersVersioned =
          inst.contractKeyWithMaintainers.map(Versioned(inst.version, _)),
      )
      serializable <- SerializableContract(
        contractId = inst.contractId,
        contractInstance = inst.toCreateNode.versionedCoinst,
        metadata = cantonContractMetadata,
        ledgerTime = CantonTimestamp(inst.createdAt.time),
        contractSalt = salt,
      ).leftMap(err => s"Failed creating serializable contract from disclosed contract: $err")

    } yield ContractInstance(inst, serializable, contractIdVersion.useUpgradeFriendlyHashing)

  def apply(serializable: SerializableContract): Either[String, ContractInstance] =
    for {
      contractIdVersion <- CantonContractIdVersion
        .extractCantonContractIdVersion(serializable.contractId)
        .leftMap(err => s"Invalid disclosed contract id: ${err.toString}")
    } yield {
      val inst = FatContractInstance.fromCreateNode(
        serializable.toLf,
        serializable.ledgerCreateTime,
        DriverContractMetadata(serializable.contractSalt).toLfBytes(contractIdVersion),
      )
      ContractInstance(inst, serializable, contractIdVersion.useUpgradeFriendlyHashing)
    }

  def decode(bytes: ByteString): Either[String, ContractInstance] =
    for {
      decoded <- TransactionCoder
        .decodeFatContractInstance(bytes)
        .leftMap(e => s"Failed to decode contract instance: $e")
      contract <- apply(decoded)
    } yield contract

}
