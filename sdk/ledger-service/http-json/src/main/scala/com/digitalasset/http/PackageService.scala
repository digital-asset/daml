// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref
import com.daml.lf.typesig
import domain.{Choice, ContractTypeId}
import ContractTypeId.ResolvedOf
import com.daml.http.util.IdentifierConverters
import com.daml.http.util.Logging.InstanceUUID
import com.daml.jwt.domain.Jwt
import com.daml.ledger.service.LedgerReader.PackageStore
import com.daml.ledger.service.{LedgerReader, TemplateIds}
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.nonempty.{NonEmpty, Singleton}
import scalaz.{EitherT, Show, \/, \/-}
import scalaz.std.option.none
import scalaz.std.scalaFuture._
import scalaz.syntax.apply._
import scalaz.syntax.std.option._

import scala.concurrent.{ExecutionContext, Future}
import java.time._
import com.daml.ledger.api.{domain => LedgerApiDomain}
import com.daml.lf.crypto.Hash.KeyPackageName

import scala.collection.MapView

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
      interfaceIdMap: InterfaceIdMap,
      templateIdMap: TemplateIdMap,
      choiceTypeMap: ChoiceTypeMap,
      keyTypeMap: KeyTypeMap,
      packageStore: PackageStore,
  ) {

    val packageNameMap: PackageNameMap = PackageService.packageNameMap(packageStore)

    def append(diff: PackageStore): State = {
      val newPackageStore: PackageStore = appendAndResolveRetroactiveInterfaces(
        resolveChoicesIn(diff)
      )
      val (tpIdMap, ifaceIdMap) = getTemplateIdInterfaceMaps(newPackageStore)
      State(
        packageIds = newPackageStore.keySet,
        interfaceIdMap = ifaceIdMap,
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
            // this is not a perfect reduction, but is never less efficient
            // and often more efficient in concurrent loading.
            //
            // But how can we just drop half of the packages on the floor?
            // Because if a package is in _state already, then by definition
            // it cannot depend on any of the packages that remain in
            // loadsSinceReloading; therefore, loadsSinceReloading is the valid
            // diff we would have seen had we started the reload *now*.
            val loadsSinceReloading = diff -- _state.packageIds
            if (diff.sizeIs > loadsSinceReloading.size)
              logger.debug(
                s"discarding ${diff.size - loadsSinceReloading.size} redundant loaded packages"
              )
            if (loadsSinceReloading.isEmpty)
              logger.debug("new package IDs not found")
            else {
              updateState(loadsSinceReloading)
              logger.info(s"new package IDs loaded: ${loadsSinceReloading.keySet.mkString(", ")}")
              logger.debug(s"loaded diff: $loadsSinceReloading")
            }
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
    resolveContractTypeIdFromState { () =>
      val st = state
      (st.templateIdMap, st.interfaceIdMap, st.packageNameMap)
    }

  private[this] def resolveContractTypeIdFromState(
      latestMaps: () => (TemplateIdMap, InterfaceIdMap, PackageNameMap)
  )(implicit ec: ExecutionContext): ResolveContractTypeId = new ResolveContractTypeId {
    import ResolveContractTypeId.{Overload => O}, domain.{ContractTypeId => C}
    override def apply[U, R](jwt: Jwt, ledgerId: LedgerApiDomain.LedgerId)(
        x: U with ContractTypeId.OptionalPkg
    )(implicit
        lc: LoggingContextOf[InstanceUUID],
        overload: O[U, R],
    ): Future[Error \/ Option[(R, KeyPackageName)]] = {
      type ResultType = Option[(R, KeyPackageName)]
      def withKeyPackageName[T[_]](
          pkgNames: PackageNameMap
      )(r: ResolvedOf[T]): Option[(ResolvedOf[T], KeyPackageName)] =
        pkgNames.get(r.packageId).map { case (name, _) => (r, name) }
      // we use a different resolution strategy depending on the static type
      // determined by 'overload', as well as the class of 'x'.  We figure the
      // strategy exactly once so the reload is cheaper
      val doSearch: ((TemplateIdMap, InterfaceIdMap, PackageNameMap)) => ResultType =
        overload match {
          case O.Template => { case (tids, _, pkgNames) =>
            (tids resolve x).flatMap(withKeyPackageName(pkgNames))
          }
          case O.Top =>
            (x: C.OptionalPkg) match {
              // only search the template or interface map, if that is the origin
              // class, since searching the other map would convert template IDs
              // to interface IDs and vice versa
              case x: C.Template.OptionalPkg => { case (tids, _, pkgNames) =>
                (tids resolve x).flatMap(withKeyPackageName(pkgNames))
              }
              case x: C.Interface.OptionalPkg => { case (_, iids, pkgNames) =>
                (iids resolve x).flatMap(withKeyPackageName(pkgNames))
              }
              case x: C.Unknown.OptionalPkg => { case (tids, iids, pkgNames) =>
                (tids resolve x, iids resolve x) match {
                  case (tid @ Some(_), None) => tid.flatMap(withKeyPackageName(pkgNames))
                  case (None, iid @ Some(_)) => iid.flatMap(withKeyPackageName(pkgNames))
                  // presence in both means the ID is ambiguous
                  case (None, None) | (Some(_), Some(_)) => None
                }
              }
            }
        }
      def doReloadAndSearchAgain() = EitherT(reload(jwt, ledgerId)).map(_ => doSearch(latestMaps()))
      def keep(it: ResultType) = EitherT.pure(it): ET[ResultType]
      for {
        result <- EitherT.pure(doSearch(latestMaps())): ET[ResultType]
        _ = logger.trace(s"Result: $result")
        finalResult <- (x: C.OptionalPkg).packageId.fold {
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
      f.map(_ => state.templateIdMap.all.keySet.map(k => (k, state.packageNameMap(k.packageId)._1)))
  }

  val resolvePackageName: ResolvePackageName = templateId => state.packageNameMap(templateId)._1

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

  sealed abstract class ResolveContractTypeId {
    import ResolveContractTypeId.Overload
    def apply[U, R](jwt: Jwt, ledgerId: LedgerApiDomain.LedgerId)(
        x: U with ContractTypeId.OptionalPkg
    )(implicit lc: LoggingContextOf[InstanceUUID], overload: Overload[U, R]): Future[
      PackageService.Error \/ Option[(R, KeyPackageName)]
    ]
  }

  object ResolveContractTypeId {
    sealed abstract class Overload[-Unresolved, +Resolved]

    import domain.{ContractTypeId => C}

    object Overload extends LowPriority {
      /* TODO #15293 see below note about Top
      implicit case object Unknown
          extends Overload[C.Unknown.OptionalPkg, C.ResolvedId[C.Definite[String]]]
       */
      implicit case object Template extends Overload[C.Template.OptionalPkg, C.Template.Resolved]
      case object Top extends Overload[C.OptionalPkg, C.ResolvedId[C.Definite[String]]]
    }

    // TODO #15293 if the request model has .Unknown included, then LowPriority and Top are
    // no longer needed and can be replaced with Overload.Unknown above
    sealed abstract class LowPriority { this: Overload.type =>
      // needs to be low priority so it doesn't win against Template
      implicit def `fallback Top`: Overload[C.OptionalPkg, C.ResolvedId[C.Definite[String]]] = Top
    }
  }

  type ResolveTemplateRecordType =
    ContractTypeId.Template.RequiredPkg => Error \/ typesig.Type

  type AllTemplateIds =
    LoggingContextOf[
      InstanceUUID
    ] => (
        Jwt,
        LedgerApiDomain.LedgerId,
    ) => Future[Set[(domain.ContractTypeId.Template.Resolved, KeyPackageName)]]

  type ResolveChoiceArgType =
    (
        ContractTypeId.Resolved,
        Choice,
    ) => Error \/ (Option[ContractTypeId.Interface.Resolved], typesig.Type)

  type ResolveKeyType =
    ContractTypeId.Template.RequiredPkg => Error \/ typesig.Type

  final case class ContractTypeIdMap[CtId[T] <: ContractTypeId[T]](
      all: Map[RequiredPkg[CtId], ResolvedOf[CtId]],
      unique: Map[NoPkg[CtId], ResolvedOf[CtId]],
      nameIds: Map[Ref.PackageName, NonEmpty[Seq[String]]],
  ) {
    private def packageRefToPackageId(pkgRef: String): Option[String] = pkgRef match {
      case s"#${name}" => nameIds.get(asPackageName(name)).map(pkgIds => pkgIds.head)
      case pkgId => Some(pkgId)
    }

    private[http] def resolve(
        a: ContractTypeId[Option[String]]
    )(implicit makeKey: ContractTypeId.Like[CtId]): Option[ResolvedOf[CtId]] =
      a.packageId.flatMap(packageRefToPackageId) match {
        case Some(p) => all get makeKey(p, a.moduleName, a.entityName)
        case None => unique get makeKey((), a.moduleName, a.entityName)
      }
  }

  type TemplateIdMap = ContractTypeIdMap[ContractTypeId.Template]
  private type InterfaceIdMap = ContractTypeIdMap[ContractTypeId.Interface]

  object TemplateIdMap {
    def Empty[CtId[T] <: ContractTypeId[T]]: ContractTypeIdMap[CtId] =
      ContractTypeIdMap(Map.empty, Map.empty, Map.empty)
  }

  private type ChoiceTypeMap = Map[ContractTypeId.Resolved, NonEmpty[
    Map[Choice, NonEmpty[Map[Option[ContractTypeId.Interface.Resolved], typesig.Type]]]
  ]]

  type KeyTypeMap = Map[ContractTypeId.Template.Resolved, typesig.Type]

  type ResolvePackageName = String => KeyPackageName

  case class PackageNameMap(
      private val mapView: MapView[String, (KeyPackageName, Ref.PackageVersion)]
  ) {
    def apply(pkgId: String) = mapView(pkgId)
    def get(pkgId: String) = mapView.get(pkgId)
    def size = mapView.size
  }
  object PackageNameMap {
    val empty = PackageNameMap(MapView.empty)
  }

  private def asPackageName(name: String): Ref.PackageName = Ref.PackageName.assertFromString(name)

  private def packageNameMap(packageStore: PackageStore): PackageNameMap =
    PackageNameMap(
      packageStore.view
        .flatMap { case ((pkgId: String, p: typesig.PackageSignature)) =>
          p.metadata.map(m =>
            (pkgId, (KeyPackageName(Some(asPackageName(m.name)), p.languageVersion), m.version))
          )
        }
        .toMap
        .view
    )

  private def getTemplateIdInterfaceMaps(
      packageStore: PackageStore
  ): (TemplateIdMap, InterfaceIdMap) = {
    import TemplateIds.{getTemplateIds, getInterfaceIds}
    val packageSigs = packageStore.values.toSet
    val idName = packageNameMap(packageStore)
    (
      buildTemplateIdMap(
        idName,
        getTemplateIds(packageSigs) map ContractTypeId.Template.fromLedgerApi,
      ),
      buildTemplateIdMap(
        idName,
        getInterfaceIds(packageSigs) map ContractTypeId.Interface.fromLedgerApi,
      ),
    )
  }

  def buildTemplateIdMap[CtId[T] <: ContractTypeId.Definite[T] with ContractTypeId.Ops[CtId, T]](
      idName: PackageNameMap,
      ids: Set[RequiredPkg[CtId]],
  ): ContractTypeIdMap[CtId] = {
    import com.daml.nonempty.NonEmptyReturningOps._
    val all = ids.view.map(k => (k, k)).toMap
    val unique = filterUniqueTemplateIs(ids)
    val nameIds = ids
      .flatMap(id => // for each contract type id
        idName
          .get(id.packageId) // get the package name and version for its package id
          .flatMap { case (kpn, v) => kpn.toOption.map((_, v)) } // ensure we have something
          .map { case (n, v) => (n, (id.packageId, v)) } // pair name with package id and version
      )
      .groupBy1(_._1) // group by name
      .map {
        // Sort the package ids by version, descending
        case (n, nPkgIdVers) => {
          val pkgIdVers: NonEmpty[Set[(String, Ref.PackageVersion)]] = nPkgIdVers.map(_._2)
          val orderedPkgIds: NonEmpty[Seq[String]] = pkgIdVers.toSeq.sortBy(_._2).map(_._1).reversed
          (n, orderedPkgIds)
        }
      }
      .toMap
    ContractTypeIdMap(all, unique, nameIds)
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
  )(templateId: ContractTypeId.Template.RequiredPkg): Error \/ typesig.Type =
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
    packageStore.values.flatMap(getChoices).toMap

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
  ): Map[ContractTypeId.Template.Resolved, typesig.Type] =
    interface.typeDecls.collect {
      case (
            qn,
            typesig.PackageSignature.TypeDecl
              .Template(_, typesig.DefTemplate(_, Some(keyType), _)),
          ) =>
        val templateId =
          ContractTypeId.Template(interface.packageId, qn.module.dottedName, qn.name.dottedName)
        (templateId, keyType)
    }
}
