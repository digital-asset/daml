// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

/** @param consumedInCore Whether the contract is consumed in the core of the view.
  *   [[com.digitalasset.canton.protocol.WellFormedTransaction]] checks that a created contract
  *   can only be used in the same or deeper rollback scopes as the create, so if `rolledBack` is true
  *   then `consumedInCore` is false.
  * @param rolledBack Whether the contract creation has a different rollback scope than the view.
  */
final case class CreatedContract private (
    contract: SerializableContract,
    consumedInCore: Boolean,
    rolledBack: Boolean,
) extends PrettyPrinting {

  // Note that on behalf of rolledBack contracts we still send the SerializableContract along with the contract instance
  // mainly to support DAMLe.reinterpret on behalf of a top-level CreateActionDescription under a rollback node because
  // we need the contract instance to construct the LfCreateCommand.
  def toProtoV1: v1.CreatedContract =
    v1.CreatedContract(
      contract = Some(contract.toProtoV1),
      consumedInCore = consumedInCore,
      rolledBack = rolledBack,
    )

  override def pretty: Pretty[CreatedContract] = prettyOfClass(
    unnamedParam(_.contract),
    paramIfTrue("consumed in core", _.consumedInCore),
    paramIfTrue("rolled back", _.rolledBack),
  )
}

object CreatedContract {
  def create(
      contract: SerializableContract,
      consumedInCore: Boolean,
      rolledBack: Boolean,
  ): Either[String, CreatedContract] =
    CantonContractIdVersion
      .ensureCantonContractId(contract.contractId)
      .leftMap(err => s"Encountered invalid Canton contract id: ${err.toString}")
      .flatMap { _ =>
        // Contracts created with the "authenticated" contract id prefix-of-suffix
        // must have contract_salt present in order to be properly authenticated (and used for explicit disclosure)
        ProtoConverter
          .required("contract_salt", contract.contractSalt)
          .leftMap(err => s"Failed instantiating created contract: ${err.message}")
      }
      .map(_ => new CreatedContract(contract, consumedInCore, rolledBack))

  def tryCreate(
      contract: SerializableContract,
      consumedInCore: Boolean,
      rolledBack: Boolean,
  ): CreatedContract =
    create(
      contract = contract,
      consumedInCore = consumedInCore,
      rolledBack = rolledBack,
    ).valueOr(err => throw new IllegalArgumentException(err))

  def fromProtoV1(
      createdContractP: v1.CreatedContract
  ): ParsingResult[CreatedContract] = {
    val v1.CreatedContract(contractP, consumedInCore, rolledBack) =
      createdContractP

    for {
      contract <- ProtoConverter
        .required("contract", contractP)
        .flatMap(SerializableContract.fromProtoV1)
      createdContract <- create(
        contract = contract,
        consumedInCore = consumedInCore,
        rolledBack = rolledBack,
      ).leftMap(OtherError)
    } yield createdContract
  }
}

/** @param consumedInView Whether the contract is consumed in the view.
  *   [[com.digitalasset.canton.protocol.WellFormedTransaction]] checks that a created contract
  *   can only be used in the same or deeper rollback scopes as the create, so if `rolledBack` is true
  *   then `consumedInView` is false.
  * @param rolledBack Whether the contract creation has a different rollback scope than the view.
  */
final case class CreatedContractInView(
    contract: SerializableContract,
    consumedInView: Boolean,
    rolledBack: Boolean,
)
object CreatedContractInView {
  def fromCreatedContract(created: CreatedContract): CreatedContractInView =
    CreatedContractInView(
      created.contract,
      consumedInView = created.consumedInCore,
      created.rolledBack,
    )
}
