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

import scala.Ordering.Implicits.infixOrderingOps

class SimplePackage(additionalContractDataType: String) {
  val damlPackageWithContractData: Ast.Package =
    p"""
      module DA.Types {
        record @serializable Tuple2 (a: *) (b: *) = { x1: a, x2: b } ;

        record @serializable FetchedContract (a: *) = { contract: a, contractId: ContractId a } ;
      }

      module Simple {
        record @serializable SimpleTemplate = { owner: Party, observer: Party, contractData: $additionalContractDataType } ;
        variant @serializable SimpleVariant = SV: Party ;
        template (this : SimpleTemplate) = {
          precondition True,
          signatories Cons @Party [Simple:SimpleTemplate {owner} this] (Nil @Party),
          observers Cons @Party [Simple:SimpleTemplate {observer} this] (Nil @Party),
          agreement "",
          choices {
            choice Consume (self) (x: Unit) : Unit,
              controllers Cons @Party [Simple:SimpleTemplate {owner} this] (Nil @Party) to
                upure @Unit (),
            choice Replace (self) (x: Unit) : ContractId Simple:SimpleTemplate,
              controllers Cons @Party [Simple:SimpleTemplate {owner} this] (Nil @Party) to
                create @Simple:SimpleTemplate (Simple:SimpleTemplate {
                  owner = Simple:SimpleTemplate {owner} this,
                  observer = Simple:SimpleTemplate {observer} this,
                  contractData = Simple:SimpleTemplate {contractData} this
                })
          },
          key @Party (Simple:SimpleTemplate {owner} this) (\ (p: Party) -> Cons @Party [p] (Nil @Party))
        } ;

        record @serializable SimpleTemplateHolder = { owner: Party } ;
        template (this : SimpleTemplateHolder) = {
          precondition True,
          signatories Cons @Party [Simple:SimpleTemplateHolder {owner} this] (Nil @Party),
          observers Nil @Party,
          agreement "",
          choices {
            choice @nonConsuming ReplaceHeldByKey (self) (x: Unit) : Unit,
              controllers Cons @Party [Simple:SimpleTemplateHolder {owner} this] (Nil @Party) to
                ubind
                  fetched: <contractId: ContractId Simple:SimpleTemplate, contract: Simple:SimpleTemplate> <- fetch_by_key @Simple:SimpleTemplate (Simple:SimpleTemplateHolder {owner} this) ;
                  consumed: Unit <- exercise @Simple:SimpleTemplate Consume (fetched).contractId () ;
                  created: ContractId Simple:SimpleTemplate <- create @Simple:SimpleTemplate (Simple:SimpleTemplate {
                    owner = Simple:SimpleTemplate {owner} (fetched).contract,
                    observer = Simple:SimpleTemplate {observer} (fetched).contract,
                    contractData = Simple:SimpleTemplate {contractData} (fetched).contract
                  })
                in
                  upure @Unit ()
          }
        } ;
      }
    """

  val packageId: Ref.PackageId =
    defaultParserParameters.defaultPackageId

  val decodedPackage: Ast.Package = {
    val metadata =
      if (defaultParserParameters.languageVersion >= LanguageVersion.Features.packageMetadata) {
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

  private val simpleHolderTemplateId: Ref.Identifier =
    Ref.Identifier(
      packageId,
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString("Simple"),
        Ref.DottedName.assertFromString("SimpleTemplateHolder")
      )
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
          Ref.Party.assertFromString(owner)),
        Some(Ref.Name.assertFromString("observer")) -> Value.ValueParty(
          Ref.Party.assertFromString(observer)),
        Some(Ref.Name.assertFromString("contractData")) -> additionalContractValue
      )
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
      ImmArray(Some(Ref.Name.assertFromString("owner")) -> Value.ValueParty(owner))
    )
}
