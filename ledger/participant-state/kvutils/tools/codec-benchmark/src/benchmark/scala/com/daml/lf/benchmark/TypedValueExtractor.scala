// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package benchmark

import com.daml.lf.data.Ref._
import com.daml.lf.transaction.TransactionOuterClass.Node.NodeTypeCase
import com.daml.lf.transaction.{TransactionCoder, TransactionVersion}
import com.daml.lf.value.ValueOuterClass
import com.google.protobuf.ByteString

import scala.jdk.CollectionConverters._
import scala.Ordering.Implicits._

final class TypedValueExtractor(interface: language.PackageInterface) {

  private[this] def handleLookup[X](lookup: => Either[language.LookupError, X]) =
    lookup match {
      case Right(value) => value
      case Left(error) => sys.error(error.pretty)
    }

  private[this] def getValue(
      version: TransactionVersion,
      versioned: => ValueOuterClass.VersionedValue,
      unversioned: => ByteString,
  ) = {
    if (version < TransactionVersion.minNoVersionValue) {
      versioned
    } else {
      ValueOuterClass.VersionedValue
        .newBuilder()
        .setVersion(version.protoValue)
        .setValue(unversioned)
        .build()
    }
  }

  def fromTransaction(transaction: EncodedTransaction): Iterator[EncodedValueWithType] =
    transaction.getNodesList.iterator.asScala.flatMap { node =>
      val version =
        TransactionCoder.decodeVersion(node).fold(err => sys.error(err.errorMessage), identity)
      node.getNodeTypeCase match {
        case NodeTypeCase.CREATE =>
          val create = node.getCreate
          val templateId = {
            TransactionCoder.decodeVersion(node.getVersion) match {
              case Right(ver) if ver >= TransactionVersion.V12 =>
                validateIdentifier(create.getTemplateId)
              case Right(_) =>
                validateIdentifier(create.getContractInstance.getTemplateId)
              case Left(message) =>
                sys.error(message.errorMessage)
            }
          }
          val template = handleLookup(interface.lookupTemplate(templateId))
          val argument = TypedValue(
            getValue(
              version,
              create.getContractInstance.getArgVersioned,
              create.getArgUnversioned,
            ),
            language.Ast.TTyCon(templateId),
          )
          val key = template.key.map(key =>
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
        case NodeTypeCase.EXERCISE =>
          val exercise = node.getExercise
          val templateId = validateIdentifier(exercise.getTemplateId)
          val template = handleLookup(interface.lookupTemplate(templateId))
          val choice = ChoiceName.assertFromString(exercise.getChoice)
          val argument =
            TypedValue(
              getValue(version, exercise.getArgVersioned, exercise.getArgUnversioned),
              template.choices(choice).argBinder._2,
            )
          val result =
            if (exercise.hasResultVersioned || !exercise.getResultUnversioned.isEmpty)
              List(
                TypedValue(
                  getValue(version, exercise.getResultVersioned, exercise.getResultUnversioned),
                  template.choices(choice).returnType,
                )
              )
            else
              Nil
          argument :: result
        case _ =>
          Nil
      }
    }

  private[this] def validateIdentifier(templateId: ValueOuterClass.Identifier): Identifier =
    Identifier(
      packageId = PackageId.assertFromString(templateId.getPackageId),
      qualifiedName = QualifiedName(
        ModuleName.assertFromSegments(templateId.getModuleNameList.asScala),
        DottedName.assertFromSegments(templateId.getNameList.asScala),
      ),
    )
}
