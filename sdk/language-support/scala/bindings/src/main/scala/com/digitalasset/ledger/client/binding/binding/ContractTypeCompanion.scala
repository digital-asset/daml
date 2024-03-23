// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import com.daml.ledger.api.refinements.ApiTypes.TemplateId
import com.daml.ledger.api.v1.{value => rpcvalue}
import encoding.ExerciseOn

/** Common superclass of template and interface companions objects. */
abstract class ContractTypeCompanion[T] extends ValueRefCompanion {

  /** Alias for contract IDs for this template or interface. Can be used
    * interchangeably with its expansion.
    */
  type ContractId = Primitive.ContractId[T]

  val id: Primitive.TemplateId[T]

  override protected lazy val ` dataTypeId` = TemplateId.unwrap(id)

  protected final def ` templateId`(
      packageId: String,
      moduleName: String,
      entityName: String,
  ): Primitive.TemplateId[T] =
    Primitive.TemplateId(packageId, moduleName, entityName)

  protected final def ` exercise`[ExOn, Out](
      receiver: ExOn,
      choiceId: String,
      arguments: Option[rpcvalue.Value],
  )(implicit exon: ExerciseOn[ExOn, T]): Primitive.Update[Out] =
    Primitive.exercise(this, receiver, choiceId, arguments getOrElse Value.encode(()))
}
