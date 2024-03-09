// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

/** @param consumed Whether this contract is consumed in the core of the view this [[InputContract]] belongs to.
  *
  * @see com.digitalasset.canton.data.ViewParticipantData.coreInputs
  */
final case class InputContract(contract: SerializableContract, consumed: Boolean)
    extends PrettyPrinting {

  def contractId: LfContractId = contract.contractId

  def contractKey: Option[LfGlobalKey] = contract.metadata.maybeKey

  def stakeholders: Set[LfPartyId] = contract.metadata.stakeholders

  def maintainers: Set[LfPartyId] = contract.metadata.maintainers

  def toProtoV0: v0.ViewParticipantData.InputContract =
    v0.ViewParticipantData.InputContract(
      contract = Some(contract.toProtoV0),
      consumed = consumed,
    )

  def toProtoV1: v1.InputContract =
    v1.InputContract(
      contract = Some(contract.toProtoV1),
      consumed = consumed,
    )

  def toProtoV2: v2.InputContract =
    v2.InputContract(
      contract = Some(contract.toProtoV2),
      consumed = consumed,
    )

  override def pretty: Pretty[InputContract] = prettyOfClass(
    unnamedParam(_.contract),
    paramIfTrue("consumed", _.consumed),
  )
}

object InputContract {
  def fromProtoV0(
      inputContractP: v0.ViewParticipantData.InputContract
  ): ParsingResult[InputContract] = {
    val v0.ViewParticipantData.InputContract(contractP, consumed) = inputContractP
    toInputContract(contractP, consumed, SerializableContract.fromProtoV0)
  }

  def fromProtoV1(
      inputContractP: v1.InputContract
  ): ParsingResult[InputContract] = {
    val v1.InputContract(contractP, consumed) = inputContractP
    toInputContract(contractP, consumed, SerializableContract.fromProtoV1)
  }

  def fromProtoV2(
      inputContractP: v2.InputContract
  ): ParsingResult[InputContract] = {
    val v2.InputContract(contractP, consumed) = inputContractP
    toInputContract(contractP, consumed, SerializableContract.fromProtoV2)
  }

  private def toInputContract[SerializableContractP](
      serializableContractO: Option[SerializableContractP],
      consumed: Boolean,
      deserializeContract: SerializableContractP => ParsingResult[SerializableContract],
  ): ParsingResult[InputContract] =
    ProtoConverter
      .required("InputContract.contract", serializableContractO)
      .flatMap(deserializeContract)
      .map(InputContract(_, consumed))
}
