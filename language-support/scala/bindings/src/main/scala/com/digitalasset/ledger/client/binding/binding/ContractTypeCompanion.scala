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

  // TODO SC #13924 try to define just one version, here, instead of two
  protected def ` exercise`[ExOn, Out](
      actor: Primitive.Party,
      receiver: ExOn,
      choiceId: String,
      arguments: Option[rpcvalue.Value],
  )(implicit exon: ExerciseOn[ExOn, T]): Primitive.Update[Out]
}
