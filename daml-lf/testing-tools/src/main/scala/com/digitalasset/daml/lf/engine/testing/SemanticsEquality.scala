// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine
package testing

import com.digitalasset.daml.lf.CompiledPackages
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Node.KeyWithMaintainers
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, VersionedValue}

class SemanticsEquality(compiledPackages: CompiledPackages) {

  type Value = VersionedValue[AbsoluteContractId]

  private def getDef(id: Identifier) =
    for {
      pkg <- compiledPackages.getPackage(id.packageId)
      mod <- pkg.modules.get(id.qualifiedName.module)
      dfn <- mod.definitions.get(id.qualifiedName.name)
    } yield dfn

  private def getTemplate(id: Identifier) =
    getDef(id).flatMap {
      case Ast.DDataType(_, _, Ast.DataRecord(_, Some(template))) => Some(template)
      case _ => None
    }

  private val valueTranslator = new ValueTranslator(compiledPackages)

  private def assertDone[X](r: Result[X]) = r match {
    case ResultDone(x) => x
    case err => sys.error(s"unexpected failure: $err")
  }

  def equalValue(
      typ: Ast.Type,
      v1: Value,
      v2: Value
  ): Boolean =
    assertDone(valueTranslator.translateValue(typ, v1)) == assertDone(
      valueTranslator.translateValue(typ, v2))

  private def equalKey(
      typ: Option[Ast.TemplateKey],
      v1: Option[KeyWithMaintainers[Value]],
      v2: Option[KeyWithMaintainers[Value]],
  ): Boolean =
    (typ, v1, v2) match {
      case (None, None, None) => true
      case (Some(t), Some(x1), Some(x2)) =>
        equalValue(t.typ, x1.key, x2.key) && x1.maintainers == x2.maintainers
      case _ => false
    }

  private def equalResult(
      typ: Ast.Type,
      v1: Option[Value],
      v2: Option[Value]
  ) =
    (v1, v2) match {
      case (None, None) => true
      case (Some(x1), Some(x2)) => equalValue(typ, x1, x2)
      case _ => false
    }

  def equalEvent[Nid](
      e1: Event[Nid, AbsoluteContractId, Value],
      e2: Event[Nid, AbsoluteContractId, Value]): Boolean =
    (e1, e2) match {
      case ( //
          CreateEvent(
            contractId1,
            templateId1,
            contractKey1,
            argument1,
            agreementText1,
            signatories1,
            observers1,
            witnesses1
          ),
          CreateEvent(
            contractId2,
            templateId2,
            contractKey2,
            argument2,
            agreementText2,
            signatories2,
            observers2,
            witnesses2) //
          ) =>
        val keyTyp = getTemplate(templateId1).get.key

        contractId1 == contractId2 &&
        templateId1 == templateId2 &&
        equalKey(keyTyp, contractKey1, contractKey2)
        equalValue(Ast.TTyCon(templateId1), argument1, argument2) &&
        agreementText1 == agreementText2
        signatories1 == signatories2 &&
        observers1 == observers2 &&
        witnesses1 == witnesses2
      case ( //
          ExerciseEvent(
            contractId1,
            templateId1,
            choice1,
            choiceArgument1,
            actingParties1,
            isConsuming1,
            children1,
            stakeholders1,
            witnesses1,
            exerciseResult1),
          ExerciseEvent(
            contractId2,
            templateId2,
            choice2,
            choiceArgument2,
            actingParties2,
            isConsuming2,
            children2,
            stakeholders2,
            witnesses2,
            exerciseResult2) //
          ) =>
        val choice = getTemplate(templateId1).get.choices(choice1)

        contractId1 == contractId2 &&
        templateId1 == templateId2 &&
        choice1 == choice2 &&
        equalValue(choice.argBinder._2, choiceArgument1, choiceArgument2) &&
        actingParties1 == actingParties2 &&
        isConsuming1 == isConsuming2 &&
        children1 == children2 &&
        stakeholders1 == stakeholders2 &&
        witnesses1 == witnesses2 &&
        equalResult(choice.returnType, exerciseResult1, exerciseResult2)

      case _ => false
    }

}
