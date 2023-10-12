// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.digitalasset.canton.http.domain.ContractTypeId
import com.daml.ledger.api.{v1 => lav1}
import org.scalacheck.Gen
import scalaz.{-\/, \/, \/-}
import spray.json.{JsNumber, JsObject, JsString, JsValue}

object Generators {
  def genApiIdentifier: Gen[lav1.value.Identifier] =
    for {
      p <- Gen.identifier
      m <- Gen.identifier
      e <- Gen.identifier
    } yield lav1.value.Identifier(packageId = p, moduleName = m, entityName = e)

  def genDomainTemplateId: Gen[domain.ContractTypeId.Template.RequiredPkg] =
    genDomainContractTypeId[domain.ContractTypeId.Template]

  private def genDomainContractTypeId[CtId[T] <: domain.ContractTypeId[T]](implicit
      CtId: domain.ContractTypeId.Like[CtId]
  ): Gen[CtId.RequiredPkg] =
    genApiIdentifier.map(CtId.fromLedgerApi)

  def genDomainTemplateIdO[CtId[T] <: domain.ContractTypeId[T], A](implicit
      CtId: domain.ContractTypeId.Like[CtId],
      ev: PackageIdGen[A],
  ): Gen[CtId[A]] =
    for {
      p <- ev.gen
      m <- Gen.identifier
      e <- Gen.identifier
    } yield CtId(p, m, e)

  def nonEmptySetOf[A](gen: Gen[A]): Gen[Set[A]] = Gen.nonEmptyListOf(gen).map(_.toSet)

  // Generate Identifiers with unique packageId values, but the same moduleName and entityName.
  def genDuplicateModuleEntityApiIdentifiers: Gen[Set[lav1.value.Identifier]] =
    for {
      id0 <- genApiIdentifier
      otherPackageIds <- nonEmptySetOf(Gen.identifier.filter(x => x != id0.packageId))
    } yield Set(id0) ++ otherPackageIds.map(a => id0.copy(packageId = a))

  def genDuplicateModuleEntityTemplateIds: Gen[Set[domain.ContractTypeId.Template.RequiredPkg]] =
    genDuplicateModuleEntityApiIdentifiers.map(xs =>
      xs.map(domain.ContractTypeId.Template.fromLedgerApi)
    )

  final case class PackageIdGen[A](gen: Gen[A])

  implicit val RequiredPackageIdGen: PackageIdGen[String] = PackageIdGen(Gen.identifier)
  implicit val NoPackageIdGen: PackageIdGen[Unit] = PackageIdGen(Gen.const(()))
  implicit val OptionalPackageIdGen: PackageIdGen[Option[String]] = PackageIdGen(
    Gen.option(RequiredPackageIdGen.gen)
  )

  def contractIdGen: Gen[domain.ContractId] = domain.ContractId subst Gen.identifier
  def partyGen: Gen[domain.Party] = domain.Party subst Gen.identifier

  def scalazEitherGen[A, B](a: Gen[A], b: Gen[B]): Gen[A \/ B] =
    Gen.oneOf(a.map(-\/(_)), b.map(\/-(_)))

  def inputContractRefGen[LfV](lfv: Gen[LfV]): Gen[domain.InputContractRef[LfV]] =
    scalazEitherGen(
      Gen.zip(genDomainTemplateIdO[domain.ContractTypeId.Template, Option[String]], lfv),
      Gen.zip(
        Gen.option(genDomainTemplateIdO: Gen[domain.ContractTypeId.OptionalPkg]),
        contractIdGen,
      ),
    )

  def contractLocatorGen[LfV](lfv: Gen[LfV]): Gen[domain.ContractLocator[LfV]] =
    inputContractRefGen(lfv) map (domain.ContractLocator.structure.from(_))

  def contractGen: Gen[domain.Contract[JsValue]] =
    scalazEitherGen(archivedContractGen, activeContractGen).map(domain.Contract(_))

  def activeContractGen: Gen[domain.ActiveContract.ResolvedCtTyId[JsValue]] =
    for {
      contractId <- contractIdGen
      templateId <- Gen.oneOf(
        genDomainTemplateIdO[domain.ContractTypeId.Template, String],
        genDomainTemplateIdO[domain.ContractTypeId.Interface, String],
      )
      key <- Gen.option(Gen.identifier.map(JsString(_)))
      argument <- Gen.identifier.map(JsString(_))
      signatories <- Gen.listOf(partyGen)
      observers <- Gen.listOf(partyGen)
      agreementText <- Gen.identifier
    } yield domain.ActiveContract[ContractTypeId.Resolved, JsValue](
      contractId = contractId,
      templateId = templateId,
      key = key,
      payload = argument,
      signatories = signatories,
      observers = observers,
      agreementText = agreementText,
    )

  def archivedContractGen: Gen[domain.ArchivedContract] =
    for {
      contractId <- contractIdGen
      templateId <- Generators.genDomainTemplateId
    } yield domain.ArchivedContract(
      contractId = contractId,
      templateId = templateId,
    )

  def contractLocatorGen: Gen[domain.ContractLocator[JsObject]] =
    Gen.oneOf(enrichedContractIdGen, enrichedContractKeyGen)

  def enrichedContractKeyGen: Gen[domain.EnrichedContractKey[JsObject]] =
    for {
      templateId <- genDomainTemplateIdO[domain.ContractTypeId.Template, Option[String]]
      key <- genJsObj
    } yield domain.EnrichedContractKey(templateId, key)

  def enrichedContractIdGen: Gen[domain.EnrichedContractId] =
    for {
      templateId <- Gen.option(genDomainTemplateIdO: Gen[domain.ContractTypeId.OptionalPkg])
      contractId <- contractIdGen
    } yield domain.EnrichedContractId(templateId, contractId)

  def exerciseCmdGen
      : Gen[domain.ExerciseCommand.OptionalPkg[JsValue, domain.ContractLocator[JsValue]]] =
    for {
      ref <- contractLocatorGen
      arg <- genJsObj
      cIfId <- Gen.option(genDomainTemplateIdO: Gen[domain.ContractTypeId.OptionalPkg])
      choice <- Gen.identifier.map(domain.Choice(_))
      meta <- Gen.option(metaGen)
    } yield domain.ExerciseCommand(
      reference = ref,
      choice = choice,
      choiceInterfaceId = cIfId,
      argument = arg,
      meta = meta,
    )

  def metaGen: Gen[domain.CommandMeta.NoDisclosed] =
    for {
      commandId <- Gen.option(Gen.identifier.map(domain.CommandId(_)))
    } yield domain.CommandMeta(commandId, None, None, None, None, None, None)

  private def genJsObj: Gen[JsObject] =
    Gen.listOf(genJsValPair).map(xs => JsObject(xs.toMap))

  private def genJsValPair: Gen[(String, JsValue)] =
    for {
      k <- Gen.identifier
      v <- genJsValue
    } yield (k, v)

  private def genJsValue: Gen[JsValue] = Gen.oneOf(
    Gen.identifier.map(JsString(_): JsValue),
    Gen.posNum[Int].map(JsNumber(_): JsValue),
  )

  def absoluteLedgerOffsetVal: Gen[lav1.ledger_offset.LedgerOffset.Value.Absolute] =
    Gen.posNum[Long].map(n => lav1.ledger_offset.LedgerOffset.Value.Absolute(n.toString))

  def genUnknownTemplateIds: Gen[domain.UnknownTemplateIds] =
    Gen
      .listOf(genDomainTemplateIdO: Gen[domain.ContractTypeId.OptionalPkg])
      .map(domain.UnknownTemplateIds)

  def genUnknownParties: Gen[domain.UnknownParties] =
    Gen.listOf(partyGen).map(domain.UnknownParties)

  def genServiceWarning: Gen[domain.ServiceWarning] =
    Gen.oneOf(genUnknownTemplateIds, genUnknownParties)

  def genWarningsWrapper: Gen[domain.AsyncWarningsWrapper] =
    genServiceWarning.map(domain.AsyncWarningsWrapper)
}
