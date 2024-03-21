// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

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

  def genDomainTemplateId: Gen[domain.TemplateId.RequiredPkg] =
    genApiIdentifier.map(domain.TemplateId.fromLedgerApi)

  def genDomainTemplateIdO[A](implicit ev: PackageIdGen[A]): Gen[domain.TemplateId[A]] =
    for {
      p <- ev.gen
      m <- Gen.identifier
      e <- Gen.identifier
    } yield domain.TemplateId(p, m, e)

  def nonEmptySetOf[A](gen: Gen[A]): Gen[Set[A]] = Gen.nonEmptyListOf(gen).map(_.toSet)

  // Generate Identifiers with unique packageId values, but the same moduleName and entityName.
  def genDuplicateModuleEntityApiIdentifiers: Gen[Set[lav1.value.Identifier]] =
    for {
      id0 <- genApiIdentifier
      otherPackageIds <- nonEmptySetOf(Gen.identifier.filter(x => x != id0.packageId))
    } yield Set(id0) ++ otherPackageIds.map(a => id0.copy(packageId = a))

  def genDuplicateModuleEntityTemplateIds: Gen[Set[domain.TemplateId.RequiredPkg]] =
    genDuplicateModuleEntityApiIdentifiers.map(xs => xs.map(domain.TemplateId.fromLedgerApi))

  trait PackageIdGen[A] {
    def gen: Gen[A]
  }

  implicit object RequiredPackageIdGen extends PackageIdGen[String] {
    override def gen: Gen[String] = Gen.identifier
  }

  implicit object NoPackageIdGen extends PackageIdGen[Unit] {
    override def gen: Gen[Unit] = Gen.const(())
  }

  implicit object OptionalPackageIdGen extends PackageIdGen[Option[String]] {
    override def gen: Gen[Option[String]] = Gen.option(RequiredPackageIdGen.gen)
  }

  def contractIdGen: Gen[domain.ContractId] = domain.ContractId subst Gen.identifier
  def partyGen: Gen[domain.Party] = domain.Party subst Gen.identifier

  def scalazEitherGen[A, B](a: Gen[A], b: Gen[B]): Gen[A \/ B] =
    Gen.oneOf(a.map(-\/(_)), b.map(\/-(_)))

  def inputContractRefGen[LfV](lfv: Gen[LfV]): Gen[domain.InputContractRef[LfV]] =
    scalazEitherGen(
      Gen.zip(genDomainTemplateIdO[Option[String]], lfv),
      Gen.zip(Gen.option(genDomainTemplateIdO[Option[String]]), contractIdGen),
    )

  def contractLocatorGen[LfV](lfv: Gen[LfV]): Gen[domain.ContractLocator[LfV]] =
    inputContractRefGen(lfv) map (domain.ContractLocator.structure.from(_))

  def contractGen: Gen[domain.Contract[JsValue]] =
    scalazEitherGen(archivedContractGen, activeContractGen).map(domain.Contract(_))

  def activeContractGen: Gen[domain.ActiveContract[JsValue]] =
    for {
      contractId <- contractIdGen
      templateId <- Generators.genDomainTemplateId
      key <- Gen.option(Gen.identifier.map(JsString(_)))
      argument <- Gen.identifier.map(JsString(_))
      signatories <- Gen.listOf(partyGen)
      observers <- Gen.listOf(partyGen)
      agreementText <- Gen.identifier
    } yield domain.ActiveContract[JsValue](
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
      templateId <- genDomainTemplateIdO(OptionalPackageIdGen)
      key <- genJsObj
    } yield domain.EnrichedContractKey(templateId, key)

  def enrichedContractIdGen: Gen[domain.EnrichedContractId] =
    for {
      templateId <- Gen.option(genDomainTemplateIdO(OptionalPackageIdGen))
      contractId <- contractIdGen
    } yield domain.EnrichedContractId(templateId, contractId)

  def exerciseCmdGen: Gen[domain.ExerciseCommand[JsValue, domain.ContractLocator[JsValue]]] =
    for {
      ref <- contractLocatorGen
      arg <- genJsObj
      choice <- Gen.identifier.map(domain.Choice(_))
      meta <- Gen.option(metaGen)
    } yield domain.ExerciseCommand(reference = ref, choice = choice, argument = arg, meta = meta)

  def metaGen: Gen[domain.CommandMeta] =
    for {
      commandId <- Gen.option(Gen.identifier.map(domain.CommandId(_)))
    } yield domain.CommandMeta(commandId, None, None)

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
      .listOf(genDomainTemplateIdO(OptionalPackageIdGen))
      .map(domain.UnknownTemplateIds)

  def genUnknownParties: Gen[domain.UnknownParties] =
    Gen.listOf(partyGen).map(domain.UnknownParties)

  def genServiceWarning: Gen[domain.ServiceWarning] =
    Gen.oneOf(genUnknownTemplateIds, genUnknownParties)

  def genWarningsWrapper: Gen[domain.AsyncWarningsWrapper] =
    genServiceWarning.map(domain.AsyncWarningsWrapper)
}
