// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.traverse.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{LfPartyId, ProtoDeserializationError}
import com.google.common.annotations.VisibleForTesting

// Invariant: signatories is a subset of all
final case class Stakeholders private (all: Set[LfPartyId], signatories: Set[LfPartyId])
    extends HasVersionedWrapper[Stakeholders]
    with PrettyPrinting {

  val nonConfirming: Set[LfPartyId] = all -- signatories

  override protected def companionObj: HasVersionedMessageCompanionCommon[Stakeholders] =
    Stakeholders

  {
    val nonStakeholderSignatories = signatories -- all
    if (nonStakeholderSignatories.nonEmpty)
      throw new IllegalArgumentException(
        show"Signatories are not stakeholders: $nonStakeholderSignatories"
      )
  }

  override protected def pretty: Pretty[Stakeholders.this.type] = prettyOfClass(
    param("all", _.all),
    param("signatories", _.signatories),
  )

  def toProtoV30: v30.Stakeholders = v30.Stakeholders(
    all = all.toSeq,
    signatories = signatories.toSeq,
  )
}

object Stakeholders extends HasVersionedMessageCompanion[Stakeholders] {
  override def name: String = "Stakeholders"
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v32,
      supportedProtoVersion(v30.Stakeholders)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def apply(metadata: ContractMetadata): Stakeholders =
    Stakeholders(all = metadata.stakeholders, signatories = metadata.signatories)

  def tryCreate(stakeholders: Set[LfPartyId], signatories: Set[LfPartyId]): Stakeholders =
    new Stakeholders(signatories = signatories, all = stakeholders)

  def withSignatories(signatories: Set[LfPartyId]): Stakeholders =
    Stakeholders(all = signatories, signatories = signatories)

  @VisibleForTesting
  def withSignatoriesAndObservers(
      signatories: Set[LfPartyId],
      observers: Set[LfPartyId],
  ): Stakeholders =
    Stakeholders(all = signatories.union(observers), signatories = signatories)

  def fromProtoV30(stakeholdersP: v30.Stakeholders): ParsingResult[Stakeholders] =
    for {
      stakeholders <- stakeholdersP.all
        .traverse(ProtoConverter.parseLfPartyId(_, "stakeholders"))
        .map(_.toSet)
      signatories <- stakeholdersP.signatories
        .traverse(ProtoConverter.parseLfPartyId(_, "signatories"))
        .map(_.toSet)

      nonStakeholderSignatories = signatories -- stakeholders

      _ <- Either.cond(
        nonStakeholderSignatories.isEmpty,
        (),
        ProtoDeserializationError.InvariantViolation(
          "signatories",
          s"The following signatories are not stakeholders: $nonStakeholderSignatories",
        ),
      )

    } yield Stakeholders(all = stakeholders, signatories = signatories)
}
