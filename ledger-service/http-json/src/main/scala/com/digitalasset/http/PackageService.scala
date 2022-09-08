// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref
import com.daml.lf.typesig
import domain.{Choice, ContractTypeId, TemplateId}
import ContractTypeId.ResolvedOf
import com.daml.http.util.IdentifierConverters
import com.daml.http.util.Logging.InstanceUUID
import com.daml.jwt.domain.Jwt
import com.daml.ledger.service.LedgerReader.PackageStore
import com.daml.ledger.service.{LedgerReader, TemplateIds}
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.nonempty.{NonEmpty, Singleton}
import scalaz.{\/, \/-, EitherT, Show}
import scalaz.std.option.none
import scalaz.std.scalaFuture._
import scalaz.syntax.apply._
import scalaz.syntax.std.option._

import scala.concurrent.{ExecutionContext, Future}
import java.time._
import com.daml.ledger.api.{domain => LedgerApiDomain}

private class PackageService(
    reloadPackageStoreIfChanged: (
        Jwt,
        LedgerApiDomain.LedgerId,
    ) => PackageService.ReloadPackageStore,
    timeoutInSeconds: Long = 60L,
) {

  private[this] val logger = ContextualizedLogger.get(getClass)

  import PackageService._
  private type ET[A] = EitherT[Future, Error, A]

  private case class State(
      packageIds: Set[String],
      contractTypeIdMap: ContractTypeIdMap[ContractTypeId],
      templateIdMap: TemplateIdMap,
      choiceTypeMap: ChoiceTypeMap,
      keyTypeMap: KeyTypeMap,
      packageStore: PackageStore,
  ) {

    def append(diff: PackageStore): State = {
      val newPackageStore = appendAndResolveRetroactiveInterfaces(resolveChoicesIn(diff))
      val (tpIdMap, ifaceIdMap) = getTemplateIdInterfaceMaps(newPackageStore)
      State(
        packageIds = newPackageStore.keySet,
        contractTypeIdMap = tpIdMap.widen[ContractTypeId] ++ ifaceIdMap.widen,
        templateIdMap = tpIdMap,
        choiceTypeMap = getChoiceTypeMap(newPackageStore),
        keyTypeMap = getKeyTypeMap(newPackageStore),
        packageStore = newPackageStore,
      )
    }

    // `diff` but with interface-inherited choices resolved
    private[this] def resolveChoicesIn(diff: PackageStore): PackageStore = {
      def lookupIf(pkgId: Ref.PackageId) = (packageStore get pkgId) orElse (diff get pkgId)
      val findIface = typesig.PackageSignature.findInterface(Function unlift lookupIf)
      diff.transform((_, iface) => iface resolveChoicesAndIgnoreUnresolvedChoices findIface)
    }

    private[this] def appendAndResolveRetroactiveInterfaces(diff: PackageStore): PackageStore = {
      def lookupIf(packageStore: PackageStore, pkId: Ref.PackageId) =
        packageStore
          .get(pkId)
          .map((_, { newSig: typesig.PackageSignature => packageStore.updated(pkId, newSig) }))

      val (packageStore2, diffElems) =
        typesig.PackageSignature.resolveRetroImplements(packageStore, diff.values.toSeq)(lookupIf)
      packageStore2 ++ diffElems.view.map(p => (p.packageId, p))
    }
  }

  private class StateCache private () {
    // volatile, reading threads don't need synchronization
    @volatile private var _state: State =
      State(
        Set.empty,
        TemplateIdMap.Empty,
        TemplateIdMap.Empty,
        Map.empty,
        Map.empty,
        Map.empty,
      )

    private def updateState(diff: PackageStore): Unit = synchronized {
      this._state = this._state.append(diff)
    }

    @volatile private var lastUpdated = Instant.MIN

    private def updateInstant(instant: Instant): Unit = synchronized {
      if (lastUpdated.isBefore(instant))
        lastUpdated = instant
    }

    def state: State = _state

    // Regular updates should happen regardless of the current state every minute.
    def packagesShouldBeFetchedAgain: Boolean =
      lastUpdated.until(Instant.now(), temporal.ChronoUnit.SECONDS) >= timeoutInSeconds

    def reload(jwt: Jwt, ledgerId: LedgerApiDomain.LedgerId)(implicit
        ec: ExecutionContext,
        lc: LoggingContextOf[InstanceUUID],
    ): Future[Error \/ Unit] =
      EitherT
        .eitherT(
          Future(
            logger.debug("Trying to execute a package update")
          ) *> reloadPackageStoreIfChanged(jwt, ledgerId)(_state.packageIds)
        )
        .map {
          case Some(diff) =>
            updateState(diff)
            logger.info(s"new package IDs loaded: ${diff.keySet.mkString(", ")}")
            logger.debug(s"loaded diff: $diff")
          case None => logger.debug("new package IDs not found")
        }
        .map { res =>
          updateInstant(Instant.now())
          res
        }
        .run
  }

  private object StateCache {
    def apply() = new StateCache()
  }

  private val cache = StateCache()
  private def state: State = cache.state

  @inline
  def reload(jwt: Jwt, ledgerId: LedgerApiDomain.LedgerId)(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): Future[Error \/ Unit] = cache.reload(jwt, ledgerId)

  def packageStore: PackageStore = state.packageStore

  def resolveContractTypeId(implicit
      ec: ExecutionContext
  ): ResolveContractTypeId.AnyKind =
    resolveContractTypeIdFromState(() => state.contractTypeIdMap)

  // Do not reduce it to something like `PackageService.resolveTemplateId(state.templateIdMap)`
  // `state.templateIdMap` will be cached in this case.
  def resolveTemplateId(implicit ec: ExecutionContext): ResolveTemplateId =
    resolveContractTypeIdFromState(() => state.templateIdMap)

  private[this] def resolveContractTypeIdFromState[
      CtId[T] <: ContractTypeId[T] with ContractTypeId.Ops[CtId, T]
  ](
      latestMap: () => ContractTypeIdMap[CtId]
  )(implicit ec: ExecutionContext): ResolveContractTypeId[CtId] = new ResolveContractTypeId[CtId] {
    private type ResultType = Option[ResolvedOf[CtId]]
    def apply(jwt: Jwt, ledgerId: LedgerApiDomain.LedgerId)(
        x: CtId[Option[String]]
    )(implicit lc: LoggingContextOf[InstanceUUID]): Future[Error \/ ResultType] = {
      def doSearch() = PackageService.resolveTemplateId(latestMap())(x)
      def doReloadAndSearchAgain() = EitherT(reload(jwt, ledgerId)).map(_ => doSearch())
      def keep(it: ResultType) = EitherT.pure(it): ET[ResultType]
      for {
        result <- EitherT.pure(doSearch()): ET[ResultType]
        _ = logger.trace(s"Result: $result")
        finalResult <-
          x.packageId.fold {
            if (result.isDefined)
              // no package id and we do have the package, refresh if timeout
              if (cache.packagesShouldBeFetchedAgain) {
                logger.trace(
                  "no package id and we do have the package, refresh because of timeout"
                )
                doReloadAndSearchAgain()
              } else {
                logger.trace(
                  "no package id and we do have the package, -no timeout- no refresh"
                )
                keep(result)
              }
            // no package id and we don’t have the package, always refresh
            else {
              logger.trace("no package id and we don’t have the package, always refresh")
              doReloadAndSearchAgain()
            }
          } { packageId =>
            if (result.isDefined) {
              logger.trace("package id defined & template id found, no refresh necessary")
              keep(result)
            } else {
              // package id and we have the package, never refresh
              if (state.packageIds.contains(packageId)) {
                logger.trace("package id and we have the package, never refresh")
                keep(result)
              }
              // package id and we don’t have the package, always refresh
              else {
                logger.trace("package id and we don’t have the package, always refresh")
                doReloadAndSearchAgain()
              }
            }
          }: ET[ResultType]
        _ = logger.trace(s"Final result: $finalResult")
      } yield finalResult
    }.run
  }

  def resolveTemplateRecordType: ResolveTemplateRecordType =
    templateId =>
      \/-(
        typesig
          .TypeCon(
            typesig.TypeConName(IdentifierConverters.lfIdentifier(templateId)),
            ImmArraySeq(),
          )
      )

  def allTemplateIds(implicit ec: ExecutionContext): AllTemplateIds = {
    implicit lc => (jwt, ledgerId) =>
      val f =
        if (cache.packagesShouldBeFetchedAgain) {
          logger.trace(
            "no package id and we do have the package, refresh because of timeout"
          )
          reload(jwt, ledgerId)
        } else Future.successful(())
      f.map(_ => state.templateIdMap.all.keySet)
  }

  // See the above comment on resolveTemplateId
  def resolveChoiceArgType: ResolveChoiceArgType =
    (ctid, c) => PackageService.resolveChoiceArgType(state.choiceTypeMap)(ctid, c)

  // See the above comment on resolveTemplateId
  def resolveKeyType: ResolveKeyType =
    x => PackageService.resolveKey(state.keyTypeMap)(x)
}

object PackageService {
  sealed trait Error
  final case class InputError(message: String) extends Error
  final case class ServerError(message: String) extends Error

  object Error {
    implicit val errorShow: Show[Error] = Show shows {
      case InputError(m) => s"PackageService input error: ${m: String}"
      case ServerError(m) => s"PackageService server error: ${m: String}"
    }
  }

  type ReloadPackageStore =
    Set[String] => Future[PackageService.Error \/ Option[LedgerReader.PackageStore]]

  type ResolveTemplateId = ResolveContractTypeId[ContractTypeId.Template]

  // Like ResolveTemplateId but includes interfaces
  sealed abstract class ResolveContractTypeId[CtId[_]] {
    def apply(jwt: Jwt, ledgerId: LedgerApiDomain.LedgerId)(
        x: CtId[Option[String]]
    )(implicit lc: LoggingContextOf[InstanceUUID]): Future[
      PackageService.Error \/ Option[ResolvedOf[CtId]]
    ]
  }

  object ResolveContractTypeId {
    type AnyKind = ResolveContractTypeId[domain.ContractTypeId]
  }

  type ResolveTemplateRecordType =
    TemplateId.RequiredPkg => Error \/ typesig.Type

  type AllTemplateIds =
    LoggingContextOf[
      InstanceUUID
    ] => (Jwt, LedgerApiDomain.LedgerId) => Future[Set[domain.ContractTypeId.Template.Resolved]]

  type ResolveChoiceArgType =
    (
        ContractTypeId.Resolved,
        Choice,
    ) => Error \/ (Option[ContractTypeId.Interface.Resolved], typesig.Type)

  type ResolveKeyType =
    TemplateId.RequiredPkg => Error \/ typesig.Type

  final case class ContractTypeIdMap[CtId[_]](
      all: Map[RequiredPkg[CtId], ResolvedOf[CtId]],
      unique: Map[NoPkg[CtId], ResolvedOf[CtId]],
  ) {
    // forms a monoid with Empty
    private[PackageService] def ++(o: ContractTypeIdMap[CtId]): ContractTypeIdMap[CtId] = {
      ContractTypeIdMap(
        all ++ o.all,
        (unique -- o.unique.keySet) ++ (o.unique -- unique.keySet),
      )
    }

    private[PackageService] def widen[O[T] >: CtId[T]]: ContractTypeIdMap[O] =
      ContractTypeIdMap(all.toMap, unique.toMap)
  }

  type TemplateIdMap = ContractTypeIdMap[ContractTypeId.Template]
  type InterfaceIdMap = ContractTypeIdMap[ContractTypeId.Interface]

  object TemplateIdMap {
    def Empty[CtId[_]]: ContractTypeIdMap[CtId] = ContractTypeIdMap(Map.empty, Map.empty)
  }

  private type ChoiceTypeMap = Map[ContractTypeId.Resolved, NonEmpty[
    Map[Choice, NonEmpty[Map[Option[ContractTypeId.Interface.Resolved], typesig.Type]]]
  ]]

  type KeyTypeMap = Map[TemplateId.RequiredPkg, typesig.Type]

  def getTemplateIdInterfaceMaps(
      packageStore: PackageStore
  ): (TemplateIdMap, ContractTypeIdMap[ContractTypeId.Interface]) = {
    import TemplateIds.{getTemplateIds, getInterfaceIds}
    val interfaces = packageStore.values.toSet
    (
      buildTemplateIdMap(getTemplateIds(interfaces) map ContractTypeId.Template.fromLedgerApi),
      buildTemplateIdMap(getInterfaceIds(interfaces) map ContractTypeId.Interface.fromLedgerApi),
    )
  }

  def buildTemplateIdMap[CtId[T] <: ContractTypeId.Definite[T] with ContractTypeId.Ops[CtId, T]](
      ids: Set[RequiredPkg[CtId]]
  ): ContractTypeIdMap[CtId] = {
    val all = ids.view.map(k => (k, k)).toMap
    val unique = filterUniqueTemplateIs(ids)
    ContractTypeIdMap(all, unique)
  }

  private type RequiredPkg[CtId[_]] = CtId[String]
  private type NoPkg[CtId[_]] = CtId[Unit]

  private[http] def key2[CtId[T] <: ContractTypeId.Ops[CtId, T]](
      k: RequiredPkg[CtId]
  ): NoPkg[CtId] =
    k.copy(packageId = ())

  private def filterUniqueTemplateIs[CtId[T] <: ContractTypeId[T] with ContractTypeId.Ops[CtId, T]](
      all: Set[RequiredPkg[CtId]]
  ): Map[NoPkg[CtId], RequiredPkg[CtId]] =
    all
      .groupBy(key2)
      .collect { case (k, v) if v.sizeIs == 1 => (k, v.head) }

  // TODO SC #14727 make sensitive to whether `a` is Unknown, Template, or Interface
  // this will entail restructuring `ContractTypeIdMap`, possibly unifying
  // the two in how we expose ResolveContractTypeId and ResolveTemplateId
  def resolveTemplateId[CtId[T] <: ContractTypeId[T] with ContractTypeId.Ops[CtId, T]](
      m: ContractTypeIdMap[CtId]
  )(a: CtId[Option[String]]): Option[ResolvedOf[CtId]] =
    a.packageId match {
      case Some(p) => m.all get a.copy(packageId = p)
      case None => m.unique get a.copy(packageId = ())
    }

  private def resolveChoiceArgType(
      choiceIdMap: ChoiceTypeMap
  )(
      ctId: ContractTypeId.Resolved,
      choice: Choice,
  ): Error \/ (Option[ContractTypeId.Interface.Resolved], typesig.Type) = {
    // TODO #14727 skip indirect resolution if ctId is an interface ID
    val resolution = for {
      choices <- choiceIdMap get ctId
      overloads <- choices get choice
      onlyChoice <- Singleton.unapply(overloads) orElse (overloads get None map ((None, _)))
    } yield onlyChoice
    resolution.toRightDisjunction(
      InputError(s"Cannot resolve Choice Argument type, given: ($ctId, $choice)")
    )
  }

  def resolveKey(
      keyTypeMap: KeyTypeMap
  )(templateId: TemplateId.RequiredPkg): Error \/ typesig.Type =
    keyTypeMap
      .get(templateId)
      .toRightDisjunction(
        InputError(s"Cannot resolve Template Key type, given: ${templateId.toString}")
      )

  // assert that the given identifier is resolved
  private[this] def fromIdentifier[CtId[T] <: ContractTypeId.Definite[T]](
      b: ContractTypeId.Like[CtId],
      id: Ref.Identifier,
  ): b.Resolved =
    fromQualifiedName(b, id.packageId, id.qualifiedName)

  // assert that the given identifier is resolved
  private[this] def fromQualifiedName[CtId[T] <: ContractTypeId.Definite[T]](
      b: ContractTypeId.Like[CtId],
      pkgId: Ref.PackageId,
      qn: Ref.QualifiedName,
  ): b.Resolved =
    b(pkgId, qn.module.dottedName, qn.name.dottedName)

  // TODO (Leo): merge getChoiceTypeMap and getKeyTypeMap, so we build them in one iteration over all templates
  private def getChoiceTypeMap(packageStore: PackageStore): ChoiceTypeMap =
    packageStore.values.view.flatMap(getChoices).toMap

  private def getChoices(
      signature: typesig.PackageSignature
  ) =
    signature.typeDecls.iterator.collect(joinPF {
      case (
            qn,
            typesig.PackageSignature.TypeDecl.Template(_, typesig.DefTemplate(choices, _, _)),
          ) =>
        NonEmpty from getTChoices(choices.resolvedChoices) map ((
          fromQualifiedName(ContractTypeId.Template, signature.packageId, qn),
          _,
        ))
    }) ++ signature.interfaces.iterator.collect(Function unlift { case (qn, defIf) =>
      NonEmpty from getIChoices(defIf.choices) map ((
        fromQualifiedName(ContractTypeId.Interface, signature.packageId, qn),
        _,
      ))
    })

  private[this] type ChoicesByInterface[Ty] =
    Map[Choice, NonEmpty[Map[Option[ContractTypeId.Interface.Resolved], Ty]]]

  private def getTChoices[Ty](
      choices: Map[Ref.ChoiceName, NonEmpty[
        Map[Option[Ref.TypeConName], typesig.TemplateChoice[Ty]]
      ]]
  ): ChoicesByInterface[Ty] = {
    import typesig._
    choices.map { case (name, resolvedChoices) =>
      (
        Choice(name: String),
        resolvedChoices.map { case (oIface, TemplateChoice(pTy, _, _)) =>
          (oIface map (fromIdentifier(ContractTypeId.Interface, _)), pTy)
        }.toMap,
      )
    }
  }

  private def getIChoices[Ty](
      choices: Map[Ref.ChoiceName, typesig.TemplateChoice[Ty]]
  ): ChoicesByInterface[Ty] =
    choices.map { case (name, typesig.TemplateChoice(pTy, _, _)) =>
      (Choice(name: String), NonEmpty(Map, none[ContractTypeId.Interface.Resolved] -> pTy))
    }

  // flatten two levels of partiality into one
  private[this] def joinPF[T, R](f: T PartialFunction Option[R]): T PartialFunction R =
    new PartialFunction[T, R] {
      override def applyOrElse[A1 <: T, B1 >: R](x: A1, default: A1 => B1): B1 =
        f.applyOrElse(x, Function const None) getOrElse default(x)

      override def isDefinedAt(x: T): Boolean = f.applyOrElse(x, Function const None).isDefined

      override def apply(v1: T): R = f(v1) getOrElse (throw new MatchError(v1))
    }

  // TODO (Leo): merge getChoiceTypeMap and getKeyTypeMap, so we build them in one iteration over all templates
  private def getKeyTypeMap(packageStore: PackageStore): KeyTypeMap =
    packageStore.flatMap { case (_, interface) => getKeys(interface) }

  private def getKeys(
      interface: typesig.PackageSignature
  ): Map[TemplateId.RequiredPkg, typesig.Type] =
    interface.typeDecls.collect {
      case (
            qn,
            typesig.PackageSignature.TypeDecl
              .Template(_, typesig.DefTemplate(_, Some(keyType), _)),
          ) =>
        val templateId = TemplateId(interface.packageId, qn.module.dottedName, qn.name.dottedName)
        (templateId, keyType)
    }
}
