// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.ledger.api.v1.value.{Identifier => Lav1Identifier}
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref
import com.daml.lf.iface
import com.daml.http.domain.{Choice, TemplateId}
import com.daml.http.util.IdentifierConverters
import com.daml.http.util.Logging.InstanceUUID
import com.daml.jwt.domain.Jwt
import com.daml.ledger.service.LedgerReader.PackageStore
import com.daml.ledger.service.{LedgerReader, TemplateIds}
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import scalaz.Scalaz._
import scalaz._

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
      contractTypeIdMap: ContractTypeIdMap,
      templateIdMap: TemplateIdMap,
      choiceTypeMap: ChoiceTypeMap,
      keyTypeMap: KeyTypeMap,
      packageStore: PackageStore,
  ) {

    def append(diff: PackageStore): State = {
      val newPackageStore = this.packageStore ++ resolveChoicesIn(diff)
      val (tpIdMap, ifaceIdMap) = getTemplateIdInterfaceMaps(newPackageStore)
      State(
        newPackageStore.keySet,
        tpIdMap ++ ifaceIdMap,
        tpIdMap,
        getChoiceTypeMap(newPackageStore),
        getKeyTypeMap(newPackageStore),
        newPackageStore,
      )
    }

    // `diff` but with interface-inherited choices resolved
    private[this] def resolveChoicesIn(diff: PackageStore): PackageStore = {
      def lookupIf(pkgId: Ref.PackageId) = (packageStore get pkgId) orElse (diff get pkgId)
      val findIface = iface.Interface.findAstInterface(Function unlift lookupIf)
      diff.transform((_, iface) => iface resolveChoicesAndIgnoreUnresolvedChoices findIface)
    }
  }

  private class StateCache private () {
    // volatile, reading threads don't need synchronization
    @volatile private var _state: State =
      State(Set.empty, TemplateIdMap.Empty, TemplateIdMap.Empty, Map.empty, Map.empty, Map.empty)

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

  // See the above comment
  def resolveChoiceArgType: ResolveChoiceArgType =
    (x, y) => PackageService.resolveChoiceArgType(state.choiceTypeMap)(x, y)

  // See the above comment
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
    (TemplateId.RequiredPkg, Choice) => Error \/ iface.Type

  type ResolveKeyType =
    TemplateId.RequiredPkg => Error \/ iface.Type

  type ContractTypeIdMap = TemplateIdMap

  case class TemplateIdMap(
      all: Set[TemplateId.RequiredPkg],
      unique: Map[TemplateId.NoPkg, TemplateId.RequiredPkg],
  ) {
    // forms a monoid with Empty
    def ++(o: TemplateIdMap): TemplateIdMap =
      TemplateIdMap(all ++ o.all, (unique -- o.unique.keySet) ++ (o.unique -- unique.keySet))
  }

  object TemplateIdMap {
    val Empty: TemplateIdMap = TemplateIdMap(Set.empty, Map.empty)
  }

  type ChoiceTypeMap = Map[(TemplateId.RequiredPkg, Choice), iface.Type]

  type KeyTypeMap = Map[TemplateId.RequiredPkg, iface.Type]

  def getTemplateIdInterfaceMaps(packageStore: PackageStore): (TemplateIdMap, ContractTypeIdMap) = {
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
    TemplateIdMap(all, unique)
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

  def resolveChoiceArgType(
      choiceIdMap: ChoiceTypeMap
  )(templateId: TemplateId.RequiredPkg, choice: Choice): Error \/ iface.Type = {
    val k = (templateId, choice)
    choiceIdMap
      .get(k)
      .toRightDisjunction(InputError(s"Cannot resolve Choice Argument type, given: ${k.toString}"))
  }

  def resolveKey(keyTypeMap: KeyTypeMap)(templateId: TemplateId.RequiredPkg): Error \/ iface.Type =
    keyTypeMap
      .get(templateId)
      .toRightDisjunction(
        InputError(s"Cannot resolve Template Key type, given: ${templateId.toString}")
      )

  // TODO (Leo): merge getChoiceTypeMap and getKeyTypeMap, so we build them in one iteration over all templates
  def getChoiceTypeMap(packageStore: PackageStore): ChoiceTypeMap =
    packageStore.flatMap { case (_, interface) => getChoices(interface) }

  // TODO (#13923) probably needs to change signature
  private def getChoices(
      interface: iface.Interface
  ): Map[(TemplateId.RequiredPkg, Choice), iface.Type] = {
    val allChoices: Iterator[(Ref.QualifiedName, Map[Ref.ChoiceName, iface.TemplateChoice.FWT])] =
      interface.typeDecls.iterator.collect {
        case (qn, iface.InterfaceType.Template(_, iface.DefTemplate(choices, _, _))) =>
          (qn, choices.assumeNoOverloadedChoices(githubIssue = 13923))
      } ++ interface.astInterfaces.iterator.map { case (qn, defIf) =>
        (qn, defIf.choices)
      }
    allChoices.flatMap { case (qn, choices) =>
      val templateId = TemplateId(interface.packageId, qn.module.toString, qn.name.toString)
      getChoices(choices).view.map { case (choice, id) => ((templateId, choice), id) }
    }.toMap
  }

  private def getChoices(
      choices: Map[Ref.Name, iface.TemplateChoice[iface.Type]]
  ): Seq[(Choice, iface.Type)] = {
    import iface._
    choices.toSeq.map { case (name, TemplateChoice(choiceType, _, _)) =>
      (Choice(name: String), choiceType)
    }
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
