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

  def toProtoV1: v1.InputContract =
    v1.InputContract(
      contract = Some(contract.toProtoV1),
      consumed = consumed,
    )

  override def pretty: Pretty[InputContract] = prettyOfClass(
    unnamedParam(_.contract),
    paramIfTrue("consumed", _.consumed),
  )
}

object InputContract {
  def fromProtoV1(
      inputContractP: v1.InputContract
  ): ParsingResult[InputContract] = {
    val v1.InputContract(contractP, consumed) = inputContractP
    toInputContract(contractP, consumed, SerializableContract.fromProtoV1)
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
