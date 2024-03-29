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
import com.digitalasset.canton.{LfPartyId, checked}

/** Metadata for a contract.
  *
  * @param signatories Must include the maintainers of the key if any
  * @param stakeholders Must include the signatories
  * @throws ContractMetadata.InvalidContractMetadata if some maintainers are not signatories or some signatories are not stakeholders.
  */
final case class ContractMetadata private (
    signatories: Set[LfPartyId],
    stakeholders: Set[LfPartyId],
    maybeKeyWithMaintainers: Option[LfGlobalKeyWithMaintainers],
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

  def maybeKey: Option[LfGlobalKey] = maybeKeyWithMaintainers.map(_.globalKey)

  def maintainers: Set[LfPartyId] =
    maybeKeyWithMaintainers.fold(Set.empty[LfPartyId])(_.maintainers)

  private[protocol] def toProtoV30: v30.SerializableContract.Metadata = {
    v30.SerializableContract.Metadata(
      nonMaintainerSignatories = (signatories -- maintainers).toList,
      nonSignatoryStakeholders = (stakeholders -- signatories).toList,
      key = maybeKey.map(GlobalKeySerialization.assertToProto),
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
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v30,
      supportedProtoVersion(v30.SerializableContract.Metadata)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  override def name: String = "contract metadata"

  final case class InvalidContractMetadata(message: String) extends RuntimeException(message)

  def tryCreate(
      signatories: Set[LfPartyId],
      stakeholders: Set[LfPartyId],
      maybeKeyWithMaintainers: Option[LfGlobalKeyWithMaintainers],
  ): ContractMetadata =
    new ContractMetadata(signatories, stakeholders, maybeKeyWithMaintainers)

  def create(
      signatories: Set[LfPartyId],
      stakeholders: Set[LfPartyId],
      maybeKeyWithMaintainers: Option[LfGlobalKeyWithMaintainers],
  ): Either[String, ContractMetadata] =
    Either
      .catchOnly[InvalidContractMetadata](
        tryCreate(signatories, stakeholders, maybeKeyWithMaintainers)
      )
      .leftMap(_.message)

  def empty: ContractMetadata = checked(ContractMetadata.tryCreate(Set.empty, Set.empty, None))

  def fromProtoV30(
      metadataP: v30.SerializableContract.Metadata
  ): ParsingResult[ContractMetadata] = {
    val v30.SerializableContract.Metadata(
      nonMaintainerSignatoriesP,
      nonSignatoryStakeholdersP,
      keyP,
      maintainersP,
    ) =
      metadataP
    for {
      nonMaintainerSignatories <- nonMaintainerSignatoriesP.traverse(ProtoConverter.parseLfPartyId)
      nonSignatoryStakeholders <- nonSignatoryStakeholdersP.traverse(ProtoConverter.parseLfPartyId)
      keyO <- keyP.traverse(GlobalKeySerialization.fromProtoV30)
      maintainersList <- maintainersP.traverse(ProtoConverter.parseLfPartyId)
      _ <- Either.cond(maintainersList.isEmpty || keyO.isDefined, (), FieldNotSet("Metadata.key"))
    } yield {
      val maintainers = maintainersList.toSet
      val keyWithMaintainersO = keyO.map(LfGlobalKeyWithMaintainers(_, maintainers))
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
