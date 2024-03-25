// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ContractMetadata.InvalidContractMetadata
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedMessageCompanionDbHelpers,
  HasVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
}
import com.digitalasset.canton.{LfPartyId, LfVersioned, checked}

/** Metadata for a contract.
  *
  * @param signatories Must include the maintainers of the key if any
  * @param stakeholders Must include the signatories
  * @throws ContractMetadata.InvalidContractMetadata if some maintainers are not signatories or some signatories are not stakeholders.
  */
final case class ContractMetadata private (
    signatories: Set[LfPartyId],
    stakeholders: Set[LfPartyId],
    maybeKeyWithMaintainersVersioned: Option[LfVersioned[LfGlobalKeyWithMaintainers]],
) extends HasVersionedWrapper[ContractMetadata]
    with PrettyPrinting {

  {
    val nonSignatoryMaintainers = maintainers -- signatories
    if (nonSignatoryMaintainers.nonEmpty)
      throw InvalidContractMetadata(show"Maintainers are not signatories: $nonSignatoryMaintainers")
    val nonStakeholderSignatories = signatories -- stakeholders
    if (nonStakeholderSignatories.nonEmpty)
      throw InvalidContractMetadata(
        show"Signatories are not stakeholders: $nonStakeholderSignatories"
      )
  }

  override protected def companionObj = ContractMetadata

  def maybeKeyWithMaintainers: Option[LfGlobalKeyWithMaintainers] =
    maybeKeyWithMaintainersVersioned.map(_.unversioned)

  def maybeKey: Option[LfGlobalKey] = maybeKeyWithMaintainers.map(_.globalKey)

  def maintainers: Set[LfPartyId] =
    maybeKeyWithMaintainers.fold(Set.empty[LfPartyId])(_.maintainers)

  def toProtoV0: v0.SerializableContract.Metadata = {
    v0.SerializableContract.Metadata(
      nonMaintainerSignatories = (signatories -- maintainers).toList,
      nonSignatoryStakeholders = (stakeholders -- signatories).toList,
      key = maybeKeyWithMaintainersVersioned.map(x =>
        GlobalKeySerialization.assertToProtoV0(
          x.map(keyWithMaintainers => keyWithMaintainers.globalKey)
        )
      ),
      maintainers = maintainers.toSeq,
    )
  }

  def toProtoV1: v1.Metadata = {
    v1.Metadata(
      nonMaintainerSignatories = (signatories -- maintainers).toList,
      nonSignatoryStakeholders = (stakeholders -- signatories).toList,
      key = maybeKeyWithMaintainersVersioned.map(x =>
        GlobalKeySerialization.assertToProtoV1(
          x.map(keyWithMaintainers => keyWithMaintainers.globalKey)
        )
      ),
      maintainers = maintainers.toSeq,
    )
  }

  override def pretty: Pretty[ContractMetadata] = prettyOfClass(
    param("signatories", _.signatories),
    param("stakeholders", _.stakeholders),
    paramIfDefined("key", _.maybeKey),
    paramIfNonEmpty("maintainers", _.maintainers),
  )
}

object ContractMetadata
    extends HasVersionedMessageCompanion[ContractMetadata]
    with HasVersionedMessageCompanionDbHelpers[ContractMetadata] {
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v3,
      supportedProtoVersion(v0.SerializableContract.Metadata)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> ProtoCodec(
      ProtocolVersion.v6,
      supportedProtoVersion(v1.Metadata)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  override def name: String = "contract metadata"

  final case class InvalidContractMetadata(message: String) extends RuntimeException(message)

  def tryCreate(
      signatories: Set[LfPartyId],
      stakeholders: Set[LfPartyId],
      maybeKeyWithMaintainers: Option[LfVersioned[LfGlobalKeyWithMaintainers]],
  ): ContractMetadata =
    new ContractMetadata(signatories, stakeholders, maybeKeyWithMaintainers)

  def create(
      signatories: Set[LfPartyId],
      stakeholders: Set[LfPartyId],
      maybeKeyWithMaintainers: Option[LfVersioned[LfGlobalKeyWithMaintainers]],
  ): Either[String, ContractMetadata] =
    Either
      .catchOnly[InvalidContractMetadata](
        tryCreate(signatories, stakeholders, maybeKeyWithMaintainers)
      )
      .leftMap(_.message)

  def empty: ContractMetadata = checked(ContractMetadata.tryCreate(Set.empty, Set.empty, None))

  def fromProtoV0(
      metadataP: v0.SerializableContract.Metadata
  ): ParsingResult[ContractMetadata] = {
    val v0.SerializableContract.Metadata(
      nonMaintainerSignatoriesP,
      nonSignatoryStakeholdersP,
      keyP,
      maintainersP,
    ) =
      metadataP
    for {
      nonMaintainerSignatories <- nonMaintainerSignatoriesP.traverse(ProtoConverter.parseLfPartyId)
      nonSignatoryStakeholders <- nonSignatoryStakeholdersP.traverse(ProtoConverter.parseLfPartyId)
      keyO <- keyP.traverse(GlobalKeySerialization.fromProtoV0)
      maintainersList <- maintainersP.traverse(ProtoConverter.parseLfPartyId)
      _ <- Either.cond(maintainersList.isEmpty || keyO.isDefined, (), FieldNotSet("Metadata.key"))
    } yield {
      val maintainers = maintainersList.toSet
      val keyWithMaintainersO = keyO.map(_.map(LfGlobalKeyWithMaintainers(_, maintainers)))
      val signatories = maintainers ++ nonMaintainerSignatories.toSet
      val stakeholders = signatories ++ nonSignatoryStakeholders.toSet
      checked(ContractMetadata.tryCreate(signatories, stakeholders, keyWithMaintainersO))
    }
  }

  def fromProtoV1(
      metadataP: v1.Metadata
  ): ParsingResult[ContractMetadata] = {
    val v1.Metadata(
      nonMaintainerSignatoriesP,
      nonSignatoryStakeholdersP,
      keyP,
      maintainersP,
    ) =
      metadataP
    for {
      nonMaintainerSignatories <- nonMaintainerSignatoriesP.traverse(ProtoConverter.parseLfPartyId)
      nonSignatoryStakeholders <- nonSignatoryStakeholdersP.traverse(ProtoConverter.parseLfPartyId)
      keyO <- keyP.traverse(GlobalKeySerialization.fromProtoV1)
      maintainersList <- maintainersP.traverse(ProtoConverter.parseLfPartyId)
      _ <- Either.cond(maintainersList.isEmpty || keyO.isDefined, (), FieldNotSet("Metadata.key"))
    } yield {
      val maintainers = maintainersList.toSet
      val keyWithMaintainersO = keyO.map(_.map(LfGlobalKeyWithMaintainers(_, maintainers)))
      val signatories = maintainers ++ nonMaintainerSignatories.toSet
      val stakeholders = signatories ++ nonSignatoryStakeholders.toSet
      checked(ContractMetadata.tryCreate(signatories, stakeholders, keyWithMaintainersO))
    }
  }

}

final case class WithContractMetadata[+A](private val x: A, metadata: ContractMetadata) {
  def unwrap: A = x
}

object WithContractMetadata {
  implicit def prettyWithContractMetadata[A: Pretty]: Pretty[WithContractMetadata[A]] = {
    import Pretty.*
    prettyOfClass(
      unnamedParam(_.x),
      param("metadata", _.metadata),
    )
  }
}
