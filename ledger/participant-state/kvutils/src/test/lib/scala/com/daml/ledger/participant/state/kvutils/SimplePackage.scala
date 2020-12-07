// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive.testing.Encode
import com.daml.lf.command.{
  Command,
  CreateAndExerciseCommand,
  CreateCommand,
  ExerciseByKeyCommand,
  ExerciseCommand
}
import com.daml.lf.data.Ref.QualifiedName
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ValueParty, ValueUnit}

class SimplePackage(additionalContractDataType: String) {
  val damlPackageWithContractData: Ast.Package =
    p"""
      module DA.Types {
        record @serializable Tuple2 (a: *) (b: *) = { x1: a, x2: b } ;
      }

      module Simple {
       record @serializable SimpleTemplate = { owner: Party, observer: Party, contractData: $additionalContractDataType } ;
       variant @serializable SimpleVariant = SV: Party ;
       template (this : SimpleTemplate) =  {
          precondition True,
          signatories Cons @Party [Simple:SimpleTemplate {owner} this] (Nil @Party),
          observers Cons @Party [Simple:SimpleTemplate {observer} this] (Nil @Party),
          agreement "",
          choices {
            choice Consume (self) (x: Unit) : Unit, controllers Cons @Party [Simple:SimpleTemplate {owner} this] (Nil @Party) to upure @Unit ()
          },
          key @Party (Simple:SimpleTemplate {owner} this) (\ (p: Party) -> Cons @Party [p] (Nil @Party))
        } ;
      }
    """

  val packageId: Ref.PackageId =
    defaultParserParameters.defaultPackageId

  val decodedPackage: Ast.Package = {
    val metadata = if (LanguageVersion.ordering
        .gteq(defaultParserParameters.languageVersion, LanguageVersion.Features.packageMetadata)) {
      Some(
        Ast.PackageMetadata(
          Ref.PackageName.assertFromString("kvutils-tests"),
          Ref.PackageVersion.assertFromString("1.0.0")))
    } else None
    damlPackageWithContractData.copy(metadata = metadata)
  }

  val archive: DamlLf.Archive = {
    Encode.encodeArchive(
      packageId -> decodedPackage,
      defaultParserParameters.languageVersion,
    )
  }

  val archiveHash: String =
    archive.getHash

  def typeConstructorId(typeConstructor: String): Ref.Identifier =
    Ref.Identifier(packageId, QualifiedName.assertFromString(typeConstructor))

  def createCmd(templateId: Ref.Identifier, templateArg: Value[Value.ContractId]): Command =
    CreateCommand(templateId, templateArg)

  def exerciseCmd(
      contractId: Value.ContractId,
      templateId: Ref.Identifier,
      choiceName: Ref.ChoiceName,
  ): Command =
    ExerciseCommand(templateId, contractId, choiceName, ValueUnit)

  def exerciseByKeyCmd(
      partyKey: Ref.Party,
      templateId: Ref.Identifier,
      choiceName: Ref.ChoiceName,
  ): Command =
    ExerciseByKeyCommand(templateId, ValueParty(partyKey), choiceName, ValueUnit)

  def createAndExerciseCmd(
      templateId: Ref.Identifier,
      templateArg: Value[Value.ContractId],
      choiceName: Ref.ChoiceName,
  ): Command =
    CreateAndExerciseCommand(templateId, templateArg, choiceName, ValueUnit)

  private val simpleTemplateId: Ref.Identifier =
    Ref.Identifier(
      packageId,
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString("Simple"),
        Ref.DottedName.assertFromString("SimpleTemplate")
      )
    )

  private val simpleConsumeChoiceName: Ref.ChoiceName =
    Ref.ChoiceName.assertFromString("Consume")

  def simpleCreateCmd(templateArg: Value[Value.ContractId]): Command =
    createCmd(simpleTemplateId, templateArg)

  def simpleExerciseConsumeCmd(contractId: Value.ContractId): Command =
    exerciseCmd(contractId, simpleTemplateId, simpleConsumeChoiceName)

  def simpleCreateAndExerciseConsumeCmd(templateArg: Value[Value.ContractId]): Command =
    createAndExerciseCmd(simpleTemplateId, templateArg, simpleConsumeChoiceName)

  def mkSimpleTemplateArg(
      owner: String,
      observer: String,
      additionalContractValue: Value[Value.ContractId],
  ): Value[Value.ContractId] =
    Value.ValueRecord(
      Some(simpleTemplateId),
      ImmArray(
        Some(Ref.Name.assertFromString("owner")) -> Value.ValueParty(
          Ref.Party.assertFromString(owner)),
        Some(Ref.Name.assertFromString("observer")) -> Value.ValueParty(
          Ref.Party.assertFromString(observer)),
        Some(Ref.Name.assertFromString("contractData")) -> additionalContractValue
      )
    )
}
