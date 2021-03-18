// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive.Decode
import com.daml.lf.command._
import com.daml.lf.data.Ref.{ChoiceName, QualifiedName}
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.language.Ast
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ValueParty, ValueRecord}
import com.daml.platform.testing.TestDarReader

import scala.util.Success

class SimplePackage(testDarName: String) {

  private val Success(dar) = TestDarReader.read(testDarName)

  val archives: Map[Ref.PackageId, DamlLf.Archive] = dar.all.map { archive =>
    (Ref.PackageId.assertFromString(archive.getHash), archive)
  }.toMap

  val packages: Map[Ref.PackageId, Ast.Package] = dar.all.map(Decode.decodeArchive).toMap

  val mainArchive: DamlLf.Archive = dar.main

  val mainPackageId: Ref.PackageId = Ref.PackageId.assertFromString(mainArchive.getHash)

  def typeConstructorId(typeConstructor: String): Ref.Identifier = {
    val qualifiedName = QualifiedName.assertFromString(typeConstructor)
    val packageId = packages
      .find { case (_, pkg) =>
        pkg.lookupDataType(qualifiedName).isRight
      }
      .map(_._1)
      .get
    Ref.Identifier(packageId, qualifiedName)
  }

  def createCmd(templateId: Ref.Identifier, templateArg: Value[Value.ContractId]): Command =
    CreateCommand(templateId, templateArg)

  def exerciseCmd(
      contractId: Value.ContractId,
      templateId: Ref.Identifier,
      choiceName: Ref.ChoiceName,
  ): Command =
    ExerciseCommand(
      templateId,
      contractId,
      choiceName,
      choiceArgument(choiceName),
    )

  def exerciseByKeyCmd(
      partyKey: Ref.Party,
      templateId: Ref.Identifier,
      choiceName: Ref.ChoiceName,
  ): Command =
    ExerciseByKeyCommand(
      templateId,
      ValueParty(partyKey),
      choiceName,
      choiceArgument(choiceName),
    )

  def createAndExerciseCmd(
      templateId: Ref.Identifier,
      templateArg: Value[Value.ContractId],
      choiceName: Ref.ChoiceName,
  ): Command =
    CreateAndExerciseCommand(
      templateId,
      templateArg,
      choiceName,
      choiceArgument(choiceName),
    )

  private def choiceArgument(choiceName: ChoiceName) = {
    ValueRecord(Some(typeConstructorId(s"Simple:$choiceName")), ImmArray.empty)
  }

  private val simpleTemplateId: Ref.Identifier =
    Ref.Identifier(
      mainPackageId,
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString("Simple"),
        Ref.DottedName.assertFromString("SimpleTemplate"),
      ),
    )

  private val simpleHolderTemplateId: Ref.Identifier =
    Ref.Identifier(
      mainPackageId,
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString("Simple"),
        Ref.DottedName.assertFromString("SimpleTemplateHolder"),
      ),
    )

  private val simpleConsumeChoiceName: Ref.ChoiceName =
    Ref.ChoiceName.assertFromString("Consume")

  private val simpleReplaceChoiceName: Ref.ChoiceName =
    Ref.ChoiceName.assertFromString("Replace")

  def simpleCreateCmd(templateArg: Value[Value.ContractId]): Command =
    createCmd(simpleTemplateId, templateArg)

  def simpleExerciseConsumeCmd(contractId: Value.ContractId): Command =
    exerciseCmd(contractId, simpleTemplateId, simpleConsumeChoiceName)

  def simpleCreateAndExerciseConsumeCmd(templateArg: Value[Value.ContractId]): Command =
    createAndExerciseCmd(simpleTemplateId, templateArg, simpleConsumeChoiceName)

  def simpleExerciseReplaceByKeyCmd(partyKey: Ref.Party): Command =
    exerciseByKeyCmd(partyKey, simpleTemplateId, simpleReplaceChoiceName)

  def mkSimpleTemplateArg(
      owner: String,
      observer: String,
      additionalContractValue: Value[Value.ContractId],
  ): Value[Value.ContractId] =
    Value.ValueRecord(
      Some(simpleTemplateId),
      ImmArray(
        Some(Ref.Name.assertFromString("owner")) -> Value.ValueParty(
          Ref.Party.assertFromString(owner)
        ),
        Some(Ref.Name.assertFromString("observer")) -> Value.ValueParty(
          Ref.Party.assertFromString(observer)
        ),
        Some(Ref.Name.assertFromString("contractData")) -> additionalContractValue,
      ),
    )

  private val simpleHolderReplaceHeldByKeyChoiceName: Ref.ChoiceName =
    Ref.ChoiceName.assertFromString("ReplaceHeldByKey")

  def simpleHolderCreateCmd(arg: Value[Value.ContractId]): Command =
    createCmd(simpleHolderTemplateId, arg)

  def simpleHolderExerciseReplaceHeldByKeyCmd(contractId: Value.ContractId): Command =
    exerciseCmd(contractId, simpleHolderTemplateId, simpleHolderReplaceHeldByKeyChoiceName)

  def mkSimpleHolderTemplateArg(owner: Ref.Party): Value[Value.ContractId] =
    Value.ValueRecord(
      Some(simpleHolderTemplateId),
      ImmArray(Some(Ref.Name.assertFromString("owner")) -> Value.ValueParty(owner)),
    )
}
