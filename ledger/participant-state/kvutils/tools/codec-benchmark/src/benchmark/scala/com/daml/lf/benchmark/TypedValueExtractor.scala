// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.benchmark

import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast.{PackageSignature, TTyCon}
import com.daml.lf.transaction.{TransactionCoder, TransactionVersion}
import com.daml.lf.value.ValueOuterClass
import com.daml.lf.value.ValueOuterClass.Identifier

import scala.collection.JavaConverters.{asScalaIteratorConverter, iterableAsScalaIterableConverter}
import scala.Ordering.Implicits._

final class TypedValueExtractor(signatures: PartialFunction[PackageId, PackageSignature]) {

  private[this] def getValue(
      version: TransactionVersion,
      versioned: => ValueOuterClass.VersionedValue,
      unversioned: => ValueOuterClass.Value,
  ) = {
    if (version < TransactionVersion.minNoVersionValue) {
      Versioned(version, versioned.getValue)
    } else {
      Versioned(version, unversioned)
    }
  }

  def fromTransaction(transaction: EncodedTransaction): Iterator[EncodedValueWithType] =
    transaction.getNodesList.iterator.asScala.flatMap { node =>
      val version =
        TransactionCoder.decodeVersion(node).fold(err => sys.error(err.errorMessage), identity)
      if (node.hasCreate) {
        val create = node.getCreate
        val (packageId, qualifiedName) =
          validateIdentifier(create.getContractInstance.getTemplateId)
        val template =
          signatures(packageId).lookupTemplate(qualifiedName).fold(sys.error, identity)
        val argument =
          TypedValue(
            getValue(version, create.getContractInstance.getValue, create.getArgUnversioned),
            TTyCon(TypeConName(packageId, qualifiedName)),
          )
        val key =
          template.key.map(key =>
            TypedValue(
              getValue(
                version,
                create.getKeyWithMaintainers.getKeyVersioned,
                create.getKeyWithMaintainers.getKeyUnversioned,
              ),
              key.typ,
            )
          )
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
            getValue(version, exercise.getArgVersioned, exercise.getArgUnversioned),
            template.choices(choice).argBinder._2,
          )
        val result =
          if (exercise.hasResultVersioned || exercise.hasResultUnversioned)
            List(
              TypedValue(
                getValue(version, exercise.getResultVersioned, exercise.getResultUnversioned),
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
