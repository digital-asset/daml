// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.benchmark

import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast.{PackageSignature, TTyCon}
import com.daml.lf.value.ValueOuterClass.Identifier

import scala.collection.JavaConverters.{asScalaIteratorConverter, iterableAsScalaIterableConverter}

final class TypedValueExtractor(signatures: PartialFunction[PackageId, PackageSignature]) {

  def fromTransaction(transaction: EncodedTransaction): Iterator[EncodedValueWithType] =
    transaction.getNodesList.iterator.asScala.flatMap { node =>
      if (node.hasCreate) {
        val create = node.getCreate
        val (packageId, qualifiedName) =
          validateIdentifier(create.getContractInstance.getTemplateId)
        val template =
          signatures(packageId).lookupTemplate(qualifiedName).fold(sys.error, identity)
        val argument =
          TypedValue(
            create.getContractInstance.getValue,
            TTyCon(TypeConName(packageId, qualifiedName)),
          )
        val key =
          template.key.map(key => TypedValue(create.getKeyWithMaintainers.getKey, key.typ))
        argument :: key.toList
      } else if (node.hasExercise) {
        val exercise = node.getExercise
        val (packageId, qualifiedName) =
          validateIdentifier(exercise.getTemplateId)
        val template =
          signatures(packageId).lookupTemplate(qualifiedName).fold(sys.error, identity)
        val choice = ChoiceName.assertFromString(exercise.getChoice)
        val argument =
          TypedValue(
            exercise.getChosenValue,
            template.choices(choice).argBinder._2,
          )
        val result =
          if (exercise.hasReturnValue)
            List(
              TypedValue(
                exercise.getReturnValue,
                template.choices(choice).returnType,
              )
            )
          else
            Nil
        argument :: result
      } else {
        Nil
      }
    }

  private def validateIdentifier(templateId: Identifier): (PackageId, QualifiedName) = {
    val packageId = PackageId.assertFromString(templateId.getPackageId)
    val qualifiedName = QualifiedName(
      ModuleName.assertFromSegments(templateId.getModuleNameList.asScala),
      DottedName.assertFromSegments(templateId.getNameList.asScala),
    )
    (packageId, qualifiedName)
  }

}
