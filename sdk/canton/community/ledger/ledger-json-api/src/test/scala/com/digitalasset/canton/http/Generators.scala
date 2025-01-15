// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.ledger.api.v2 as lav2
import com.digitalasset.canton.http.ContractTypeId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref
import org.scalacheck.Gen
import scalaz.{-\/, \/, \/-}
import spray.json.{JsNumber, JsObject, JsString, JsValue}

object Generators {
  def genApiIdentifier: Gen[lav2.value.Identifier] =
    for {
      p <- genPkgIdentifier
      m <- Gen.identifier
      e <- Gen.identifier
    } yield lav2.value.Identifier(packageId = p, moduleName = m, entityName = e)

  def genDomainTemplateId: Gen[ContractTypeId.Template.RequiredPkg] =
    genDomainTemplateIdO[ContractTypeId.Template, Ref.PackageRef]

  def genDomainTemplateIdPkgId: Gen[ContractTypeId.Template.RequiredPkgId] =
    genDomainTemplateIdO[ContractTypeId.Template, Ref.PackageId]

  def genDomainTemplateIdO[CtId[T] <: ContractTypeId[T], A](implicit
      CtId: ContractTypeId.Like[CtId],
      ev: PackageIdGen[A],
  ): Gen[CtId[A]] =
    for {
      p <- ev.gen
      m <- Gen.identifier
      e <- Gen.identifier
    } yield CtId(p, m, e)

  def nonEmptySetOf[A](gen: Gen[A]): Gen[Set[A]] = Gen.nonEmptyListOf(gen).map(_.toSet)

  // Generate Identifiers with unique packageId values, but the same moduleName and entityName.
  def genDuplicateModuleEntityApiIdentifiers: Gen[Set[lav2.value.Identifier]] =
    for {
      id0 <- genApiIdentifier
      otherPackageIds <- nonEmptySetOf(genPkgIdentifier.filter(x => x != id0.packageId))
    } yield Set(id0) ++ otherPackageIds.map(a => id0.copy(packageId = a))

  def genDuplicateModuleEntityTemplateIds: Gen[Set[ContractTypeId.Template.RequiredPkgId]] =
    genDuplicateModuleEntityApiIdentifiers.map(xs => xs.map(ContractTypeId.Template.fromLedgerApi))

  private val genPkgIdentifier = Gen.identifier.map(s => s.substring(0, 64 min s.length))

  final case class PackageIdGen[A](gen: Gen[A])

  implicit val RequiredPackageIdGen: PackageIdGen[Ref.PackageId] = PackageIdGen(
    genPkgIdentifier.map(Ref.PackageId.assertFromString)
  )
  implicit val RequiredPackageRefGen: PackageIdGen[Ref.PackageRef] = PackageIdGen(
    Gen.oneOf(
      genPkgIdentifier.map(s => Ref.PackageRef.Id(Ref.PackageId.assertFromString(s))),
      genPkgIdentifier.map(s => Ref.PackageRef.Name(Ref.PackageName.assertFromString(s))),
    )
  )

  def contractIdGen: Gen[ContractId] = ContractId subst Gen.identifier
  def partyGen: Gen[Party] = Party subst Gen.identifier

  def scalazEitherGen[A, B](a: Gen[A], b: Gen[B]): Gen[A \/ B] =
    Gen.oneOf(a.map(-\/(_)), b.map(\/-(_)))

  def inputContractRefGen[LfV](lfv: Gen[LfV]): Gen[InputContractRef[LfV]] =
    scalazEitherGen(
      Gen.zip(genDomainTemplateIdO[ContractTypeId.Template, Ref.PackageRef], lfv),
      Gen.zip(
        Gen.option(genDomainTemplateIdO: Gen[ContractTypeId.RequiredPkg]),
        contractIdGen,
      ),
    )

  def contractLocatorGen[LfV](lfv: Gen[LfV]): Gen[ContractLocator[LfV]] =
    inputContractRefGen(lfv) map (ContractLocator.structure.from(_))

  def contractGen: Gen[Contract[JsValue]] =
    scalazEitherGen(archivedContractGen, activeContractGen).map(Contract(_))

  def activeContractGen: Gen[ActiveContract.ResolvedCtTyId[JsValue]] =
    for {
      contractId <- contractIdGen
      templateId <- Gen.oneOf(
        genDomainTemplateIdO[ContractTypeId.Template, Ref.PackageId],
        genDomainTemplateIdO[ContractTypeId.Interface, Ref.PackageId],
      )
      key <- Gen.option(Gen.identifier.map(JsString(_)))
      argument <- Gen.identifier.map(JsString(_))
      signatories <- Gen.listOf(partyGen)
      observers <- Gen.listOf(partyGen)
    } yield ActiveContract[ContractTypeId.ResolvedPkgId, JsValue](
      contractId = contractId,
      templateId = templateId,
      key = key,
      payload = argument,
      signatories = signatories,
      observers = observers,
    )

  def archivedContractGen: Gen[ArchivedContract] =
    for {
      contractId <- contractIdGen
      templateId <- Generators.genDomainTemplateIdO: Gen[ContractTypeId.RequiredPkgId]
    } yield ArchivedContract(
      contractId = contractId,
      templateId = templateId,
    )

  def contractLocatorGen: Gen[ContractLocator[JsObject]] =
    Gen.oneOf(enrichedContractIdGen, enrichedContractKeyGen)

  def enrichedContractKeyGen: Gen[EnrichedContractKey[JsObject]] =
    for {
      templateId <- genDomainTemplateIdO[ContractTypeId.Template, Ref.PackageRef]
      key <- genJsObj
    } yield EnrichedContractKey(templateId, key)

  def enrichedContractIdGen: Gen[EnrichedContractId] =
    for {
      templateId <- Gen.option(genDomainTemplateIdO: Gen[ContractTypeId.RequiredPkg])
      contractId <- contractIdGen
    } yield EnrichedContractId(templateId, contractId)

  def exerciseCmdGen: Gen[ExerciseCommand.RequiredPkg[JsValue, ContractLocator[JsValue]]] =
    for {
      ref <- contractLocatorGen
      arg <- genJsObj
      cIfId <- Gen.option(genDomainTemplateIdO: Gen[ContractTypeId.RequiredPkg])
      choice <- Gen.identifier.map(Choice(_))
      meta <- Gen.option(metaGen)
    } yield ExerciseCommand(
      reference = ref,
      choice = choice,
      choiceInterfaceId = cIfId,
      argument = arg,
      meta = meta,
    )

  def metaGen: Gen[CommandMeta.NoDisclosed] =
    for {
      commandId <- Gen.option(Gen.identifier.map(CommandId(_)))
      synchronizerId <- Gen.option(Gen.const(SynchronizerId.tryFromString("some::synchronizerid")))
    } yield CommandMeta(commandId, None, None, None, None, None, None, synchronizerId, None)

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

  def genUnknownTemplateIds: Gen[UnknownTemplateIds] =
    Gen
      .listOf(genDomainTemplateIdO: Gen[ContractTypeId.RequiredPkg])
      .map(UnknownTemplateIds.apply)

  def genUnknownParties: Gen[UnknownParties] =
    Gen.listOf(partyGen).map(UnknownParties.apply)

  def genServiceWarning: Gen[ServiceWarning] =
    Gen.oneOf(genUnknownTemplateIds, genUnknownParties)

  def genWarningsWrapper: Gen[AsyncWarningsWrapper] =
    genServiceWarning.map(AsyncWarningsWrapper.apply)
}
