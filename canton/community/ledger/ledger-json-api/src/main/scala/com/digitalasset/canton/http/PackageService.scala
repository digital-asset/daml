// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.jwt.domain.Jwt
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref
import com.daml.lf.typesig
import com.digitalasset.canton.http.domain.ContractTypeId
import com.daml.logging.LoggingContextOf
import com.daml.nonempty.{NonEmpty, Singleton}
import com.digitalasset.canton.http.domain.ContractTypeId.ResolvedOf
import com.digitalasset.canton.http.domain.Choice
import com.digitalasset.canton.http.util.IdentifierConverters
import com.digitalasset.canton.http.util.Logging.InstanceUUID
import com.digitalasset.canton.ledger.service.LedgerReader.PackageStore
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

class PackageService(
    reloadPackageStoreIfChanged: (
        Jwt
    ) => PackageService.ReloadPackageStore,
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
      val newPackageStore = appendAndResolveRetroactiveInterfaces(resolveChoicesIn(diff))
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
          .map((_, { (newSig: typesig.PackageSignature) => packageStore.updated(pkId, newSig) }))

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

    def reload(jwt: Jwt)(implicit
        ec: ExecutionContext,
        lc: LoggingContextOf[InstanceUUID],
    ): Future[Error \/ Unit] = {
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
              logger.debug(s"loaded diff: $loadsSinceReloading, ${lc.makeString}")
            }
          case None => logger.debug(s"new package IDs not found, ${lc.makeString}")
        }
        .map { res =>
          updateInstant(Instant.now())
          res
        }
        .run
    }
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

    override def apply[U, R](jwt: Jwt)(
        x: U with ContractTypeId.OptionalPkg
    )(implicit
        lc: LoggingContextOf[InstanceUUID],
        overload: O[U, R],
    ): Future[Error \/ Option[R]] = {
      type ResultType = Option[R]
      // we use a different resolution strategy depending on the static type
      // determined by 'overload', as well as the class of 'x'.  We figure the
      // strategy exactly once so the reload is cheaper
      val doSearch: () => ResultType = overload match {
        case O.Template => () => latestMaps()._1 resolve x
        case O.Top =>
          (x: C.OptionalPkg) match {
            // only search the template or interface map, if that is the origin
            // class, since searching the other map would convert template IDs
            // to interface IDs and vice versa
            case x: C.Template.OptionalPkg => () => latestMaps()._1 resolve x
            case x: C.Interface.OptionalPkg => () => latestMaps()._2 resolve x
            case x: C.Unknown.OptionalPkg => { () =>
              val (tids, iids) = latestMaps()
              (tids resolve x, iids resolve x) match {
                case (tid @ Some(_), None) => tid
                case (None, iid @ Some(_)) => iid
                // presence in both means the ID is ambiguous
                case (None, None) | (Some(_), Some(_)) => None
              }
            }
          }
      }
      def doReloadAndSearchAgain() = EitherT(reload(jwt)).map(_ => doSearch())
      def keep(it: ResultType) = EitherT.pure(it): ET[ResultType]
      for {
        result <- EitherT.pure(doSearch()): ET[ResultType]
        _ = logger.trace(s"Result: $result, ${lc.makeString}")
        finalResult <- (x: C.OptionalPkg).packageId.fold {
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
        } { packageId =>
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
        }: ET[ResultType]
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

  sealed abstract class ResolveContractTypeId {
    import ResolveContractTypeId.Overload
    def apply[U, R](jwt: Jwt)(
        x: U with ContractTypeId.OptionalPkg
    )(implicit lc: LoggingContextOf[InstanceUUID], overload: Overload[U, R]): Future[
      PackageService.Error \/ Option[R]
    ]
  }

  object ResolveContractTypeId {
    sealed abstract class Overload[-Unresolved, +Resolved]

    import com.digitalasset.canton.http.domain.ContractTypeId as C

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
    ] => Jwt => Future[Set[ContractTypeId.Template.Resolved]]

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
  ) {
    def resolve(
        a: ContractTypeId[Option[String]]
    )(implicit makeKey: ContractTypeId.Like[CtId]): Option[ResolvedOf[CtId]] =
      a.packageId match {
        case Some(p) => all get makeKey(p, a.moduleName, a.entityName)
        case None => unique get makeKey((), a.moduleName, a.entityName)
      }
  }

  type TemplateIdMap = ContractTypeIdMap[ContractTypeId.Template]
  private type InterfaceIdMap = ContractTypeIdMap[ContractTypeId.Interface]

  object TemplateIdMap {
    def Empty[CtId[T] <: ContractTypeId[T]]: ContractTypeIdMap[CtId] =
      ContractTypeIdMap(Map.empty, Map.empty)
  }

  private type ChoiceTypeMap = Map[ContractTypeId.Resolved, NonEmpty[
    Map[Choice, NonEmpty[Map[Option[ContractTypeId.Interface.Resolved], typesig.Type]]]
  ]]

  type KeyTypeMap = Map[ContractTypeId.Template.Resolved, typesig.Type]

  private def getTemplateIdInterfaceMaps(
      packageStore: PackageStore
  ): (TemplateIdMap, InterfaceIdMap) = {
    import TemplateIds.{getInterfaceIds, getTemplateIds}
    val packageSigs = packageStore.values.toSet
    (
      buildTemplateIdMap(getTemplateIds(packageSigs) map ContractTypeId.Template.fromLedgerApi),
      buildTemplateIdMap(getInterfaceIds(packageSigs) map ContractTypeId.Interface.fromLedgerApi),
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

  def key2[CtId[T] <: ContractTypeId.Ops[CtId, T]](
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
