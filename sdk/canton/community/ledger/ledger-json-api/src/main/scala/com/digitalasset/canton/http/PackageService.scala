// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.jwt.Jwt
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.typesig
import com.digitalasset.canton.http.domain.{ContractTypeId, ContractTypeRef}
import com.daml.logging.LoggingContextOf
import com.daml.nonempty.{NonEmpty, Singleton}
import com.digitalasset.canton.http.domain.ContractTypeId.ResolvedOf
import com.digitalasset.canton.http.domain.Choice
import com.digitalasset.canton.http.util.IdentifierConverters
import com.digitalasset.canton.http.util.Logging.InstanceUUID
import com.digitalasset.canton.ledger.service.LedgerReader.{PackageStore, Signatures}
import com.digitalasset.canton.ledger.service.{LedgerReader, TemplateIds}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import scalaz.std.option.none
import scalaz.std.scalaFuture.*
import scalaz.syntax.apply.*
import scalaz.syntax.std.option.*
import scalaz.{EitherT, Show, \/, \/-}

import java.time.*
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.MapView

class PackageService(
    reloadPackageStoreIfChanged: Jwt => PackageService.ReloadPackageStore,
    val loggerFactory: NamedLoggerFactory,
    timeoutInSeconds: Long = 60L,
) extends NamedLogging
    with NoTracing {

  import PackageService.*
  private type ET[A] = EitherT[Future, Error, A]

  private case class State(
      packageIds: Set[String],
      interfaceIdMap: InterfaceIdMap,
      templateIdMap: TemplateIdMap,
      choiceTypeMap: ChoiceTypeMap,
      keyTypeMap: KeyTypeMap,
      packageStore: PackageStore,
  ) {

    def append(diff: PackageStore): State = {
      val newPackageStore = this.packageStore ++ resolveChoicesIn(diff)

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
      val findIface =
        typesig.PackageSignature.findInterface((Function unlift lookupIf).andThen(_.typesig))
      diff.transform((_, iface) =>
        Signatures(iface.typesig.resolveChoicesAndIgnoreUnresolvedChoices(findIface), iface.pack)
      )
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

    def reload(jwt: Jwt)(implicit
        ec: ExecutionContext,
        lc: LoggingContextOf[InstanceUUID],
    ): Future[Error \/ Unit] =
      EitherT
        .eitherT(
          Future(
            logger.debug(s"Trying to execute a package update, ${lc.makeString}")
          ) *> reloadPackageStoreIfChanged(jwt)(_state.packageIds)
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
                s"discarding ${diff.size - loadsSinceReloading.size} redundant loaded packages, ${lc.makeString}"
              )
            if (loadsSinceReloading.isEmpty)
              logger.debug(s"new package IDs not found, ${lc.makeString}")
            else {
              updateState(loadsSinceReloading)
              logger.info(
                s"new package IDs loaded: ${loadsSinceReloading.keySet.mkString(", ")}, ${lc.makeString}"
              )
              logger.debug(
                s"loaded diff: $loadsSinceReloading, ${lc.makeString}"
                  .take(1000) /* truncate output */
              )
            }
          case None => logger.debug(s"new package IDs not found, ${lc.makeString}")
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
  def reload(jwt: Jwt)(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): Future[Error \/ Unit] = cache.reload(jwt)

  def packageStore: PackageStore = state.packageStore

  def resolveContractTypeId(implicit ec: ExecutionContext): ResolveContractTypeId =
    resolveContractTypeIdFromState { () =>
      val st = state
      (st.templateIdMap, st.interfaceIdMap)
    }

  private[this] def resolveContractTypeIdFromState(
      latestMaps: () => (TemplateIdMap, InterfaceIdMap)
  )(implicit ec: ExecutionContext): ResolveContractTypeId = new ResolveContractTypeId {
    import ResolveContractTypeId.Overload as O
    import com.digitalasset.canton.http.domain.ContractTypeId as C

    override def apply[U, R[T] <: ContractTypeId[T]](jwt: Jwt)(
        x: U with ContractTypeId.RequiredPkg
    )(implicit
        lc: LoggingContextOf[InstanceUUID],
        overload: O[U, R],
    ): Future[Error \/ Option[ContractTypeRef[R]]] = {
      type ResultType = Option[ContractTypeRef[R]]
      // we use a different resolution strategy depending on the static type
      // determined by 'overload', as well as the class of 'x'.  We figure the
      // strategy exactly once so the reload is cheaper
      val doSearch: ((TemplateIdMap, InterfaceIdMap)) => ResultType = overload match {
        case O.Template => { case (tids, _) => tids resolve x }
        case O.Top =>
          (x: C.RequiredPkg) match {
            // only search the template or interface map, if that is the origin
            // class, since searching the other map would convert template IDs
            // to interface IDs and vice versa
            case x: C.Template.RequiredPkg => { case (tids, _) => tids resolve x }
            case x: C.Interface.RequiredPkg => { case (_, iids) => iids resolve x }
            case x: C.Unknown.RequiredPkg => { case (tids, iids) =>
              (tids resolve x, iids resolve x) match {
                case (tid @ Some(_), None) => tid
                case (None, iid @ Some(_)) => iid
                // presence in both means the ID is ambiguous
                case (None, None) | (Some(_), Some(_)) => None
              }
            }
          }
      }
      def doReloadAndSearchAgain() = EitherT(reload(jwt)).map(_ => doSearch(latestMaps()))
      def keep(it: ResultType) = EitherT.pure(it): ET[ResultType]
      for {
        result <- EitherT.pure(doSearch(latestMaps())): ET[ResultType]
        _ = logger.trace(s"Result: $result, ${lc.makeString}")
        finalResult <- ((x: C.RequiredPkg).packageId match {
          case Ref.PackageRef.Name(_) => // Used package name, not package id
            if (result.isDefined)
              // no package id and we do have the package, refresh if timeout
              if (cache.packagesShouldBeFetchedAgain) {
                logger.trace(
                  s"no package id and we do have the package, refresh because of timeout, ${lc.makeString}"
                )
                doReloadAndSearchAgain()
              } else {
                logger.trace(
                  s"no package id and we do have the package, -no timeout- no refresh, ${lc.makeString}"
                )
                keep(result)
              }
            // no package id and we don’t have the package, always refresh
            else {
              logger.trace(
                s"no package id and we don’t have the package, always refresh, ${lc.makeString}"
              )
              doReloadAndSearchAgain()
            }
          case Ref.PackageRef.Id(packageId) =>
            if (result.isDefined) {
              logger.trace(
                s"package id defined & template id found, no refresh necessary, ${lc.makeString}"
              )
              keep(result)
            } else {
              // package id and we have the package, never refresh
              if (state.packageIds.contains(packageId)) {
                logger.trace(s"package id and we have the package, never refresh, ${lc.makeString}")
                keep(result)
              }
              // package id and we don’t have the package, always refresh
              else {
                logger.trace(
                  s"package id and we don’t have the package, always refresh, ${lc.makeString}"
                )
                doReloadAndSearchAgain()
              }
            }
        }): ET[ResultType]
        _ = logger.trace(s"Final result: $finalResult, ${lc.makeString}")
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

  def allTemplateIds(implicit ec: ExecutionContext): AllTemplateIds = { implicit lc => jwt =>
    val f =
      if (cache.packagesShouldBeFetchedAgain) {
        logger.trace(
          s"no package id and we do have the package, refresh because of timeout, ${lc.makeString}"
        )
        reload(jwt)
      } else Future.successful(())
    f.map(_ => state.templateIdMap.allIds)
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

  sealed abstract class ResolveContractTypeId {
    import ResolveContractTypeId.Overload
    def apply[U, R[T] <: ContractTypeId[T]](jwt: Jwt)(
        x: U with ContractTypeId.RequiredPkg
    )(implicit lc: LoggingContextOf[InstanceUUID], overload: Overload[U, R]): Future[
      PackageService.Error \/ Option[ContractTypeRef[R]]
    ]
  }

  object ResolveContractTypeId {
    sealed abstract class Overload[-Unresolved, +Resolved[_]]

    import com.digitalasset.canton.http.domain.ContractTypeId as C

    object Overload extends LowPriority {
      /* TODO #15293 see below note about Top
    implicit case object Unknown
        extends Overload[C.Unknown.RequiredPkg, C.ResolvedId[C.Definite[String]]]
       */
      implicit case object Template extends Overload[C.Template.RequiredPkg, C.Template]
      case object Top extends Overload[C.RequiredPkg, C.Definite]
    }

    // TODO #15293 if the request model has .Unknown included, then LowPriority and Top are
    // no longer needed and can be replaced with Overload.Unknown above
    sealed abstract class LowPriority { this: Overload.type =>
      // needs to be low priority so it doesn't win against Template
      implicit def `fallback Top`: Overload[C.RequiredPkg, C.Definite] = Top
    }
  }

  type ResolveTemplateRecordType =
    ContractTypeId.Template.RequiredPkgId => Error \/ typesig.Type

  type AllTemplateIds =
    LoggingContextOf[
      InstanceUUID
    ] => Jwt => Future[Set[ContractTypeRef[ContractTypeId.Template]]]

  type ResolveChoiceArgType =
    (
        ContractTypeId.ResolvedPkgId,
        Choice,
    ) => Error \/ (Option[ContractTypeId.Interface.ResolvedPkgId], typesig.Type)

  type ResolveKeyType =
    ContractTypeId.Template.RequiredPkgId => Error \/ typesig.Type

  final case class ContractTypeIdMap[CtId[T] <: ContractTypeId[T]](
      all: Map[CtId[Ref.PackageRef], ResolvedOf[CtId]],
      nameIds: Map[Ref.PackageName, NonEmpty[Seq[Ref.PackageId]]],
      idNames: PackageNameMap,
  ) {
    private[http] def toContractTypeRef(
        id: ContractTypeId.ResolvedOf[CtId]
    ): Option[ContractTypeRef[CtId]] =
      id.packageId match {
        case Ref.PackageRef.Name(pname) =>
          for {
            pkgIdsForName <- nameIds.get(pname)
            pkgIdsForCtId <- NonEmpty.from(
              pkgIdsForName.filter(pId => all.contains(id.copy(packageId = Ref.PackageRef.Id(pId))))
            )
            (name, _) <- idNames.get(Ref.PackageRef.Name(pname))
          } yield ContractTypeRef(id, pkgIdsForCtId, Some(name))
        case Ref.PackageRef.Id(pid) =>
          Some(ContractTypeRef.unnamed[CtId](id.copy(packageId = pid)))
      }

    def allIds: Set[ContractTypeRef[CtId]] =
      all.values.flatMap { case id =>
        // If the package has a name, use the package name instead of package id.
        val useId = idNames
          .get(id.packageId)
          .fold(id) { case (name, _) => id.copy(packageId = Ref.PackageRef.Name(name)) }
        toContractTypeRef(useId)
      }.toSet

    def resolve(
        a: ContractTypeId.RequiredPkg
    )(implicit makeKey: ContractTypeId.Like[CtId]): Option[ContractTypeRef[CtId]] =
      (all get makeKey(a.packageId, a.moduleName, a.entityName)).flatMap(toContractTypeRef)
  }

  type TemplateIdMap = ContractTypeIdMap[ContractTypeId.Template]
  private type InterfaceIdMap = ContractTypeIdMap[ContractTypeId.Interface]

  object TemplateIdMap {
    def Empty[CtId[T] <: ContractTypeId.Definite[T]]: ContractTypeIdMap[CtId] =
      ContractTypeIdMap(Map.empty, Map.empty, PackageNameMap.empty)
  }

  private type ChoiceTypeMap = Map[ContractTypeId.ResolvedPkgId, NonEmpty[
    Map[Choice, NonEmpty[Map[Option[ContractTypeId.Interface.ResolvedPkgId], typesig.Type]]]
  ]]

  type KeyTypeMap = Map[ContractTypeId.Template.ResolvedPkgId, typesig.Type]

  case class PackageNameMap(
      private val mapView: MapView[Ref.PackageRef, (Ref.PackageName, Ref.PackageVersion)]
  ) {
    def get(pkgId: Ref.PackageRef) = mapView.get(pkgId)
    override def toString() = s"PackageNameMap(${mapView.toMap})"
  }
  object PackageNameMap {
    val empty = PackageNameMap(MapView.empty)
  }

  private def buildPackageNameMap(packageStore: PackageStore): PackageNameMap =
    PackageNameMap(
      packageStore.view
        .flatMap { case ((pkgId, p)) =>
          // We make two entries per package: one by package id and another by package name.
          val meta = p.typesig.metadata
          val pId = Ref.PackageId.assertFromString(pkgId)
          val pName = Ref.PackageName.assertFromString(meta.name)
          val nameInfo = (pName, meta.version)
          List(
            (Ref.PackageRef.Id(pId), nameInfo),
            (Ref.PackageRef.Name(pName), nameInfo),
          )
        }
        .toMap[Ref.PackageRef, (Ref.PackageName, Ref.PackageVersion)]
        .view
    )

  private def getTemplateIdInterfaceMaps(
      packageStore: PackageStore
  ): (TemplateIdMap, InterfaceIdMap) = {
    import TemplateIds.{getInterfaceIds, getTemplateIds}
    val packageSigs = packageStore.values.toSet
    val idName = buildPackageNameMap(packageStore)
    (
      buildTemplateIdMap(
        idName,
        getTemplateIds(packageSigs.map(_.typesig)) map ContractTypeId.Template.fromLedgerApi,
      ),
      buildTemplateIdMap(
        idName,
        getInterfaceIds(packageSigs.map(_.typesig)) map ContractTypeId.Interface.fromLedgerApi,
      ),
    )
  }

  def buildTemplateIdMap[CtId[T] <: ContractTypeId.Definite[T] with ContractTypeId.Ops[CtId, T]](
      idName: PackageNameMap,
      ids: Set[CtId[Ref.PackageId]],
  ): ContractTypeIdMap[CtId] = {
    import com.daml.nonempty.NonEmptyReturningOps.*
    val all: Map[CtId[Ref.PackageRef], ResolvedOf[CtId]] = ids.view.map { id =>
      val k = id.copy(packageId = Ref.PackageRef.Id(id.packageId): Ref.PackageRef)
      (k, k)
    }.toMap

    val idPkgNamePkgVer: Set[(CtId[Ref.PackageId], Ref.PackageName, Ref.PackageVersion)] =
      ids
        .flatMap { id =>
          idName
            .get(Ref.PackageRef.Id(id.packageId))
            .map { case (nm, ver) => (id, nm, ver) }
        }

    val nameIds: Map[Ref.PackageName, NonEmpty[Seq[Ref.PackageId]]] = idPkgNamePkgVer
      .groupBy1(_._2) // group by package name
      .map {
        case (name, idNameVers) => {
          // Sort the package ids by version, descending
          val orderedPkgIds: NonEmpty[Seq[Ref.PackageId]] = idNameVers
            .map { case (id, _, ver) => (id.packageId, ver) }
            .toSeq
            .sorted(
              Ordering.by((pkgIdVer: (Ref.PackageId, Ref.PackageVersion)) => pkgIdVer._2).reverse
            )
            .map(_._1)
          (name, orderedPkgIds)
        }
      }
      .toMap

    val allByPkgName: Map[CtId[Ref.PackageRef], ResolvedOf[CtId]] = idPkgNamePkgVer.map {
      case (id, name, _) =>
        val idWithPkgName = id.copy(packageId = Ref.PackageRef.Name(name): Ref.PackageRef)
        (idWithPkgName, idWithPkgName)
    }.toMap

    ContractTypeIdMap(all ++ allByPkgName, nameIds, idName)
  }

  private def resolveChoiceArgType(
      choiceIdMap: ChoiceTypeMap
  )(
      ctId: ContractTypeId.ResolvedPkgId,
      choice: Choice,
  ): Error \/ (Option[ContractTypeId.Interface.ResolvedPkgId], typesig.Type) = {
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
  )(templateId: ContractTypeId.Template.RequiredPkgId): Error \/ typesig.Type =
    keyTypeMap
      .get(templateId)
      .toRightDisjunction(
        InputError(s"Cannot resolve Template Key type, given: ${templateId.toString}")
      )

  // assert that the given identifier is resolved
  private[this] def fromIdentifier[CtId[T] <: ContractTypeId.Definite[T]](
      b: ContractTypeId.Like[CtId],
      id: Ref.Identifier,
  ): b.ResolvedPkgId =
    fromQualifiedName(b, id.packageId, id.qualifiedName)

  // assert that the given identifier is resolved
  private[this] def fromQualifiedName[CtId[T] <: ContractTypeId.Definite[T]](
      b: ContractTypeId.Like[CtId],
      pkgId: Ref.PackageId,
      qn: Ref.QualifiedName,
  ): b.ResolvedPkgId =
    b(pkgId, qn.module.dottedName, qn.name.dottedName)

  private def getChoiceTypeMap(packageStore: PackageStore): ChoiceTypeMap =
    packageStore.values.view.map(_.typesig).flatMap(getChoices).toMap

  private def getChoices(
      signature: typesig.PackageSignature
  ): IterableOnce[(ContractTypeId.ResolvedPkgId, NonEmpty[ChoicesByInterface[typesig.Type]])] =
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
    Map[Choice, NonEmpty[Map[Option[ContractTypeId.Interface.ResolvedPkgId], Ty]]]

  private def getTChoices[Ty](
      choices: Map[Ref.ChoiceName, NonEmpty[
        Map[Option[Ref.TypeConName], typesig.TemplateChoice[Ty]]
      ]]
  ): ChoicesByInterface[Ty] = {
    import typesig.*
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
      (Choice(name: String), NonEmpty(Map, none[ContractTypeId.Interface.ResolvedPkgId] -> pTy))
    }

  // flatten two levels of partiality into one
  private[this] def joinPF[T, R](f: T PartialFunction Option[R]): T PartialFunction R =
    new PartialFunction[T, R] {
      override def applyOrElse[A1 <: T, B1 >: R](x: A1, default: A1 => B1): B1 =
        f.applyOrElse(x, Function const None) getOrElse default(x)

      override def isDefinedAt(x: T): Boolean = f.applyOrElse(x, Function const None).isDefined

      override def apply(v1: T): R = f(v1) getOrElse (throw new MatchError(v1))
    }

  private def getKeyTypeMap(packageStore: PackageStore): KeyTypeMap =
    packageStore.flatMap { case (_, interface) => getKeys(interface.typesig) }

  private def getKeys(
      interface: typesig.PackageSignature
  ): Map[ContractTypeId.Template.ResolvedPkgId, typesig.Type] =
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
