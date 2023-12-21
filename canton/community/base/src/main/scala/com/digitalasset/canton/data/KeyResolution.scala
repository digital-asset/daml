// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.{LfContractId, LfTransactionVersion, v3}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

sealed trait KeyResolution extends Product with Serializable with PrettyPrinting {
  def resolution: Option[LfContractId]

  /** lf version of the key */
  def version: LfTransactionVersion
}

sealed trait KeyResolutionWithMaintainers extends KeyResolution {
  def maintainers: Set[LfPartyId]

  def asSerializable: SerializableKeyResolution
}

sealed trait SerializableKeyResolution extends KeyResolution {
  def toProtoOneOfV0: v3.ViewParticipantData.ResolvedKey.Resolution
}

object SerializableKeyResolution {
  def fromProtoOneOfV3(
      resolutionP: v3.ViewParticipantData.ResolvedKey.Resolution,
      version: LfTransactionVersion,
  ): ParsingResult[SerializableKeyResolution] =
    resolutionP match {
      case v3.ViewParticipantData.ResolvedKey.Resolution.ContractId(contractIdP) =>
        ProtoConverter
          .parseLfContractId(contractIdP)
          .map(AssignedKey(_)(version))
      case v3.ViewParticipantData.ResolvedKey.Resolution
            .Free(v3.ViewParticipantData.FreeKey(maintainersP)) =>
        maintainersP
          .traverse(ProtoConverter.parseLfPartyId)
          .map(maintainers => FreeKey(maintainers.toSet)(version))
      case v3.ViewParticipantData.ResolvedKey.Resolution.Empty =>
        Left(FieldNotSet("ViewParticipantData.ResolvedKey.resolution"))
    }
}

final case class AssignedKey(contractId: LfContractId)(
    override val version: LfTransactionVersion
) extends SerializableKeyResolution {
  override def pretty: Pretty[AssignedKey] =
    prettyNode("Assigned", unnamedParam(_.contractId))

  override def resolution: Option[LfContractId] = Some(contractId)

  override def toProtoOneOfV0: v3.ViewParticipantData.ResolvedKey.Resolution =
    v3.ViewParticipantData.ResolvedKey.Resolution.ContractId(value = contractId.toProtoPrimitive)
}

final case class FreeKey(override val maintainers: Set[LfPartyId])(
    override val version: LfTransactionVersion
) extends SerializableKeyResolution
    with KeyResolutionWithMaintainers {
  override def pretty: Pretty[FreeKey] = prettyNode("Free", param("maintainers", _.maintainers))

  override def resolution: Option[LfContractId] = None

  override def toProtoOneOfV0: v3.ViewParticipantData.ResolvedKey.Resolution =
    v3.ViewParticipantData.ResolvedKey.Resolution.Free(
      value = v3.ViewParticipantData.FreeKey(maintainers = maintainers.toSeq)
    )

  override def asSerializable: SerializableKeyResolution = this
}

final case class AssignedKeyWithMaintainers(
    contractId: LfContractId,
    override val maintainers: Set[LfPartyId],
)(override val version: LfTransactionVersion)
    extends KeyResolutionWithMaintainers {
  override def resolution: Option[LfContractId] = Some(contractId)

  override def pretty: Pretty[AssignedKeyWithMaintainers] = prettyOfClass(
    unnamedParam(_.contractId),
    param("maintainers", _.maintainers),
  )

  override def asSerializable: SerializableKeyResolution =
    AssignedKey(contractId)(version)
}
