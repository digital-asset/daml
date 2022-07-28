// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.ledger.api.v1.value.{Identifier => Lav1Identifier}
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref
import com.daml.lf.iface
import com.daml.http.domain.{Choice, ContractTypeId, TemplateId}
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
      contractTypeIdMap: ContractTypeIdMap[ContractTypeId.Unknown],
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
        contractTypeIdMap = tpIdMap ++ ifaceIdMap,
        templateIdMap = tpIdMap,
        choiceTypeMap = getChoiceTypeMap(newPackageStore),
        keyTypeMap = getKeyTypeMap(newPackageStore),
        packageStore = newPackageStore,
      )
    }

    // `diff` but with interface-inherited choices resolved
    private[this] def resolveChoicesIn(diff: PackageStore): PackageStore = {
      def lookupIf(pkgId: Ref.PackageId) = (packageStore get pkgId) orElse (diff get pkgId)
      val findIface = iface.Interface.findAstInterface(Function unlift lookupIf)
      diff.transform((_, iface) => iface resolveChoicesAndIgnoreUnresolvedChoices findIface)
    }

    private[this] def appendAndResolveRetroactiveInterfaces(diff: PackageStore): PackageStore = {
      def lookupIf(packageStore: PackageStore, pkId: Ref.PackageId) =
        packageStore
          .get(pkId)
          .map((_, { newSig: iface.Interface => packageStore.updated(pkId, newSig) }))

      val (packageStore2, diffElems) =
        iface.Interface.resolveRetroImplements(packageStore, diff.values.toSeq)(lookupIf)
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

  def resolveContractTypeId(implicit ec: ExecutionContext): ResolveContractTypeId =
    resolveContractTypeIdFromState(() => state.contractTypeIdMap)

  // Do not reduce it to something like `PackageService.resolveTemplateId(state.templateIdMap)`
  // `state.templateIdMap` will be cached in this case.
  def resolveTemplateId(implicit ec: ExecutionContext): ResolveTemplateId =
    resolveContractTypeIdFromState(() => state.templateIdMap)

  private[this] def resolveContractTypeIdFromState(
      latestMap: () => TemplateIdMap
  )(implicit ec: ExecutionContext): ResolveContractTypeId = {
    implicit lc: LoggingContextOf[InstanceUUID] => (jwt, ledgerId) => (x: TemplateId.OptionalPkg) =>
      {
        type ResultType = Option[TemplateId.RequiredPkg]
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
        iface
          .TypeCon(iface.TypeConName(IdentifierConverters.lfIdentifier(templateId)), ImmArraySeq())
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
      f.map(_ => state.templateIdMap.all)
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

  type ResolveTemplateId =
    LoggingContextOf[
      InstanceUUID
    ] => (Jwt, LedgerApiDomain.LedgerId) => TemplateId.OptionalPkg => Future[
      PackageService.Error \/ Option[TemplateId.RequiredPkg]
    ]

  // Like ResolveTemplateId but includes interfaces
  type ResolveContractTypeId = ResolveTemplateId

  type ResolveTemplateRecordType =
    TemplateId.RequiredPkg => Error \/ iface.Type

  type AllTemplateIds =
    LoggingContextOf[
      InstanceUUID
    ] => (Jwt, LedgerApiDomain.LedgerId) => Future[Set[TemplateId.RequiredPkg]]

  type ResolveChoiceArgType =
    (
        ContractTypeId.Unknown.RequiredPkg,
        Choice,
    ) => Error \/ (Option[ContractTypeId.Interface.Resolved], iface.Type)

  type ResolveKeyType =
    TemplateId.RequiredPkg => Error \/ iface.Type

  final case class ContractTypeIdMap[CtId[_]](
      all: Set[ContractTypeId.Resolved[CtId[String]]],
      unique: Map[CtId[Unit], ContractTypeId.Resolved[CtId[String]]],
  ) {
    // forms a monoid with Empty
    def ++[O[X] >: CtId[X]](o: ContractTypeIdMap[O]): ContractTypeIdMap[O] = {
      type UniqueGoal = Map[O[Unit], ContractTypeId.Resolved[CtId[String]]]
      ContractTypeIdMap(
        all ++ o.all,
        ((unique.toMap: UniqueGoal) -- o.unique.keySet) ++ (o.unique -- unique.keySet),
      )
    }
  }

  type TemplateIdMap = ContractTypeIdMap[ContractTypeId.Template]
  type InterfaceIdMap = ContractTypeIdMap[ContractTypeId.Interface]

  object TemplateIdMap {
    val Empty: TemplateIdMap = ContractTypeIdMap(Set.empty, Map.empty)
  }

  private type ChoiceTypeMap = Map[ContractTypeId.Unknown.Resolved, NonEmpty[
    Map[Choice, NonEmpty[Map[Option[ContractTypeId.Interface.Resolved], iface.Type]]]
  ]]

  type KeyTypeMap = Map[TemplateId.RequiredPkg, iface.Type]

  def getTemplateIdInterfaceMaps(
      packageStore: PackageStore
  ): (TemplateIdMap, ContractTypeIdMap[ContractTypeId.Interface]) = {
    import TemplateIds.{getTemplateIds, getInterfaceIds}
    def tpId(x: Lav1Identifier): TemplateId.RequiredPkg =
      TemplateId(x.packageId, x.moduleName, x.entityName)
    val interfaces = packageStore.values.toSet
    (
      buildTemplateIdMap(getTemplateIds(interfaces) map tpId),
      buildTemplateIdMap(getInterfaceIds(interfaces) map tpId),
    )
  }

  def buildTemplateIdMap(ids: Set[TemplateId.RequiredPkg]): TemplateIdMap = {
    val all: Set[TemplateId.RequiredPkg] = ids
    val unique: Map[TemplateId.NoPkg, TemplateId.RequiredPkg] = filterUniqueTemplateIs(all)
    ContractTypeIdMap(all, unique)
  }

  private[http] def key2(k: TemplateId.RequiredPkg): TemplateId.NoPkg =
    TemplateId[Unit]((), k.moduleName, k.entityName)

  private def filterUniqueTemplateIs(
      all: Set[TemplateId.RequiredPkg]
  ): Map[TemplateId.NoPkg, TemplateId.RequiredPkg] =
    all
      .groupBy(k => key2(k))
      .collect { case (k, v) if v.sizeIs == 1 => (k, v.head) }

  def resolveTemplateId(
      m: TemplateIdMap
  )(a: TemplateId.OptionalPkg): Option[TemplateId.RequiredPkg] =
    a.packageId match {
      case Some(p) => findTemplateIdByK3(m.all)(TemplateId(p, a.moduleName, a.entityName))
      case None => findTemplateIdByK2(m.unique)(TemplateId((), a.moduleName, a.entityName))
    }

  private def findTemplateIdByK3(m: Set[TemplateId.RequiredPkg])(
      k: TemplateId.RequiredPkg
  ): Option[TemplateId.RequiredPkg] = Some(k).filter(m.contains)

  private def findTemplateIdByK2(m: Map[TemplateId.NoPkg, TemplateId.RequiredPkg])(
      k: TemplateId.NoPkg
  ): Option[TemplateId.RequiredPkg] = m.get(k)

  private def resolveChoiceArgType(
      choiceIdMap: ChoiceTypeMap
  )(
      ctId: ContractTypeId.Unknown.Resolved,
      choice: Choice,
  ): Error \/ (Option[ContractTypeId.Interface.Resolved], iface.Type) = {
    // TODO #14067 skip indirect resolution if ctId is an interface ID
    val resolution = for {
      choices <- choiceIdMap get ctId
      overloads <- choices get choice
      onlyChoice <- Singleton.unapply(overloads) orElse (overloads get None map ((None, _)))
    } yield onlyChoice
    resolution.toRightDisjunction(
      InputError(s"Cannot resolve Choice Argument type, given: ($ctId, $choice)")
    )
  }

  def resolveKey(keyTypeMap: KeyTypeMap)(templateId: TemplateId.RequiredPkg): Error \/ iface.Type =
    keyTypeMap
      .get(templateId)
      .toRightDisjunction(
        InputError(s"Cannot resolve Template Key type, given: ${templateId.toString}")
      )

  // assert that the given identifier is resolved
  private[this] def fromIdentifier[CtId[T] <: ContractTypeId.Unknown[T]](
      b: ContractTypeId.Like[CtId],
      id: Ref.Identifier,
  ): b.Resolved =
    fromQualifiedName(b, id.packageId, id.qualifiedName)

  // assert that the given identifier is resolved
  private[this] def fromQualifiedName[CtId[T] <: ContractTypeId.Unknown[T]](
      b: ContractTypeId.Like[CtId],
      pkgId: Ref.PackageId,
      qn: Ref.QualifiedName,
  ): b.Resolved =
    b(pkgId, qn.module.dottedName, qn.name.dottedName)

  // TODO (Leo): merge getChoiceTypeMap and getKeyTypeMap, so we build them in one iteration over all templates
  private def getChoiceTypeMap(packageStore: PackageStore): ChoiceTypeMap =
    packageStore.values.view.flatMap(getChoices).toMap

  private def getChoices(
      signature: iface.Interface
  ) =
    signature.typeDecls.iterator.collect(joinPF {
      case (qn, iface.InterfaceType.Template(_, iface.DefTemplate(choices, _, _))) =>
        NonEmpty from getTChoices(choices.resolvedChoices) map ((
          fromQualifiedName(ContractTypeId.Template, signature.packageId, qn),
          _,
        ))
    }) ++ signature.astInterfaces.iterator.collect(Function unlift { case (qn, defIf) =>
      NonEmpty from getIChoices(defIf.choices) map ((
        fromQualifiedName(ContractTypeId.Interface, signature.packageId, qn),
        _,
      ))
    })

  private[this] type ChoicesByInterface[Ty] =
    Map[Choice, NonEmpty[Map[Option[ContractTypeId.Interface.Resolved], Ty]]]

  private def getTChoices[Ty](
      choices: Map[Ref.ChoiceName, NonEmpty[Map[Option[Ref.TypeConName], iface.TemplateChoice[Ty]]]]
  ): ChoicesByInterface[Ty] = {
    import iface._
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
      choices: Map[Ref.ChoiceName, iface.TemplateChoice[Ty]]
  ): ChoicesByInterface[Ty] =
    choices.map { case (name, iface.TemplateChoice(pTy, _, _)) =>
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

  private def getKeys(interface: iface.Interface): Map[TemplateId.RequiredPkg, iface.Type] =
    interface.typeDecls.collect {
      case (
            qn,
            iface.InterfaceType
              .Template(_, iface.DefTemplate(_, Some(keyType), _)),
          ) =>
        val templateId = TemplateId(interface.packageId, qn.module.dottedName, qn.name.dottedName)
        (templateId, keyType)
    }
}
