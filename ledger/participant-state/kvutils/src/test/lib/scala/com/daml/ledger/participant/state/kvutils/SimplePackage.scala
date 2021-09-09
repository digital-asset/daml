// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.test.TestDar
import com.daml.lf.archive.Decode
import com.daml.lf.command._
import com.daml.lf.data.Ref.QualifiedName
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.language.Ast
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ValueParty, ValueRecord}
import com.daml.platform.testing.TestDarReader

import scala.util.Success

class SimplePackage(testDar: TestDar) {

  private val Success(dar) = TestDarReader.readCommonTestDar(testDar)

  val archives: Map[Ref.PackageId, DamlLf.Archive] = dar.all.map { archive =>
    (Ref.PackageId.assertFromString(archive.getHash), archive)
  }.toMap

  val packages: Map[Ref.PackageId, Ast.Package] = dar.all.map(Decode.assertDecodeArchive(_)).toMap

  val mainArchive: DamlLf.Archive = dar.main

  val mainPackageId: Ref.PackageId = Ref.PackageId.assertFromString(mainArchive.getHash)

  def typeConstructorId(typeConstructor: String): Ref.Identifier = {
    val qName = QualifiedName.assertFromString(typeConstructor)
    packages.iterator
      .flatMap { case (pkgId, pkg) =>
        pkg.modules.collect {
          case (modName, mod) if modName == qName.module && mod.definitions.contains(qName.name) =>
            Ref.Identifier(pkgId, qName)
        }
      }
      .next()
  }

  def createCmd(templateId: Ref.Identifier, templateArg: Value[Value.ContractId]): CreateCommand =
    CreateCommand(templateId, templateArg)

  def exerciseCmd(
      contractId: Value.ContractId,
      templateId: Ref.Identifier,
      choiceName: Ref.ChoiceName,
  ): ExerciseCommand =
    ExerciseCommand(
      templateId,
      contractId,
      choiceName,
      choiceArgument,
    )

  def exerciseByKeyCmd(
      partyKey: Ref.Party,
      templateId: Ref.Identifier,
      choiceName: Ref.ChoiceName,
  ): ExerciseByKeyCommand =
    ExerciseByKeyCommand(
      templateId,
      ValueParty(partyKey),
      choiceName,
      choiceArgument,
    )

  def createAndExerciseCmd(
      templateId: Ref.Identifier,
      templateArg: Value[Value.ContractId],
      choiceName: Ref.ChoiceName,
  ): CreateAndExerciseCommand =
    CreateAndExerciseCommand(
      templateId,
      templateArg,
      choiceName,
      choiceArgument,
    )

  private val choiceArgument = ValueRecord(None, ImmArray.Empty)

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

  private val simpleArchiveChoiceName: Ref.ChoiceName =
    Ref.ChoiceName.assertFromString("Archive")

  private val simpleReplaceChoiceName: Ref.ChoiceName =
    Ref.ChoiceName.assertFromString("Replace")

  def simpleCreateCmd(templateArg: Value[Value.ContractId]): CreateCommand =
    createCmd(simpleTemplateId, templateArg)

  def simpleExerciseArchiveCmd(contractId: Value.ContractId): ExerciseCommand =
    exerciseCmd(contractId, simpleTemplateId, simpleArchiveChoiceName)

  def simpleCreateAndExerciseArchiveCmd(
      templateArg: Value[Value.ContractId]
  ): CreateAndExerciseCommand =
    createAndExerciseCmd(simpleTemplateId, templateArg, simpleArchiveChoiceName)

  def simpleExerciseReplaceByKeyCmd(partyKey: Ref.Party): ExerciseByKeyCommand =
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

  def simpleHolderCreateCmd(arg: Value[Value.ContractId]): CreateCommand =
    createCmd(simpleHolderTemplateId, arg)

  def simpleHolderExerciseReplaceHeldByKeyCmd(contractId: Value.ContractId): ExerciseCommand =
    exerciseCmd(contractId, simpleHolderTemplateId, simpleHolderReplaceHeldByKeyChoiceName)

  def mkSimpleHolderTemplateArg(owner: Ref.Party): Value[Value.ContractId] =
    Value.ValueRecord(
      Some(simpleHolderTemplateId),
      ImmArray(Some(Ref.Name.assertFromString("owner")) -> Value.ValueParty(owner)),
    )
}
