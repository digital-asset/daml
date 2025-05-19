// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package validation

import com.daml.lf.data.Ref.TypeConName
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.language.Ast._
import com.daml.lf.language.{Ast, LanguageVersion}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.daml.lf.language.Util

import cats.implicits._
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

case class Upgrading[A](past: A, present: A) {
  def map[B](f: A => B): Upgrading[B] = Upgrading(f(past), f(present))
  def fold[B](f: (A, A) => B): B = f(past, present)
  def zip[B, C](that: Upgrading[B], f: (A, B) => C) =
    Upgrading(f(this.past, that.past), f(this.present, that.present))
}

final case class UpgradeError(err: UpgradeError.Error) extends ValidationError {
  def prettyInternal: String = err.message
  def context: Context = Context.None
}

object UpgradeError {
  sealed abstract class Error {
    def message: String;
  }

  final case class CannotImplementNonUpgradeableInterface(
      pkg: Ref.PackageId,
      iface: Ref.TypeConName,
      tpl: Ref.DottedName,
  ) extends Error {
    override def message: String =
      s"Template ${tpl} implements interface ${iface} from package ${pkg} which has LF version <= 1.15. It is forbidden for upgradeable templates (LF version >= 1.17) to implement interfaces from non-upgradeable packages (LF version <= 1.15)."
  }

  final case class CouldNotResolveUpgradedPackageId(packageId: Upgrading[Ref.PackageId])
      extends Error {
    override def message: String =
      s"Package ${packageId.present} claims to upgrade package with id ${packageId.past}, but that package cannot be found."
  }

  final case class MissingModule(name: Ref.ModuleName) extends Error {
    override def message: String =
      s"Module $name appears in package that is being upgraded, but does not appear in the upgrading package."
  }

  final case class MissingTemplate(name: Ref.DottedName) extends Error {
    override def message: String =
      s"Template $name appears in package that is being upgraded, but does not appear in the upgrading package."
  }

  final case class MissingDataCon(name: Ref.DottedName) extends Error {
    override def message: String =
      s"Data type $name appears in package that is being upgraded, but does not appear in the upgrading package."
  }

  final case class MissingChoice(name: Ref.ChoiceName) extends Error {
    override def message: String =
      s"Choice $name appears in package that is being upgraded, but does not appear in the upgrading package."
  }

  final case class TemplateChangedKeyType(templateName: Ref.DottedName, key: Upgrading[Ast.Type])
      extends Error {
    override def message: String =
      s"The upgraded template $templateName cannot change its key type."
  }

  final case class TemplateRemovedKey(templateName: Ref.DottedName, key: Ast.TemplateKey)
      extends Error {
    override def message: String =
      s"The upgraded template $templateName cannot remove its key."
  }

  final case class TemplateAddedKey(templateName: Ref.DottedName, key: Ast.TemplateKey)
      extends Error {
    override def message: String =
      s"The upgraded template $templateName cannot add a key."
  }

  final case class ChoiceChangedReturnType(choice: Ref.ChoiceName, typ: Upgrading[Ast.Type])
      extends Error {
    override def message: String =
      s"The upgraded choice $choice cannot change its return type."
  }

  final case class RecordChangedOrigin(
      dataConName: Ref.DottedName,
      origin: Upgrading[UpgradedRecordOrigin],
  ) extends Error {
    override def message: String =
      s"The record $dataConName has changed origin from ${origin.past} to ${origin.present}"
  }

  final case class MismatchDataConsVariety(
      dataConName: Ref.DottedName,
      variety: Upgrading[Ast.DataCons],
  ) extends Error {
    override def message: String =
      s"The upgraded data type $dataConName has changed from a ${printCons(variety.past)} to a ${printCons(variety.present)}. Datatypes cannot change variety via upgrades."

    def printCons(variety: DataCons) =
      variety match {
        case _: Ast.DataRecord => "record"
        case _: Ast.DataVariant => "variant"
        case _: Ast.DataEnum => "enum"
        case Ast.DataInterface => "interface"
      }
  }

  final case class RecordFieldsMissing(
      origin: UpgradedRecordOrigin,
      fields: Map[Ast.FieldName, Ast.Type],
  ) extends Error {
    override def message: String =
      s"The upgraded $origin is missing some of its original fields."
  }

  final case class RecordFieldsExistingChanged(
      origin: UpgradedRecordOrigin,
      fields: Map[Ast.FieldName, Upgrading[Ast.Type]],
  ) extends Error {
    override def message: String =
      s"The upgraded $origin has changed the types of some of its original fields."
  }

  final case class RecordFieldsNewNonOptional(origin: UpgradedRecordOrigin) extends Error {
    override def message: String =
      s"The upgraded $origin has added new fields, but those fields are not Optional."
  }

  final case class RecordFieldsOrderChanged(origin: UpgradedRecordOrigin) extends Error {
    override def message: String =
      s"The upgraded $origin has changed the order of its fields - any new fields must be added at the end of the record."
  }

  final case class VariantAddedVariant(origin: UpgradedRecordOrigin) extends Error {
    override def message: String =
      s"The upgraded $origin has added a new variant."
  }

  final case class VariantRemovedVariant(origin: UpgradedRecordOrigin) extends Error {
    override def message: String =
      s"The upgraded $origin has removed an existing variant."
  }

  final case class VariantChangedVariantType(origin: UpgradedRecordOrigin) extends Error {
    override def message: String =
      s"The upgraded $origin has changed the type of a variant."
  }

  final case class VariantVariantsOrderChanged(origin: UpgradedRecordOrigin) extends Error {
    override def message: String =
      s"The upgraded $origin has changed the order of its variants - any new variant must be added at the end of the variant."
  }

  final case class VariantAddedVariantField(origin: UpgradedRecordOrigin) extends Error {
    override def message: String =
      s"The upgraded $origin has added a field."
  }

  final case class EnumAddedVariant(origin: UpgradedRecordOrigin) extends Error {
    override def message: String =
      s"The upgraded $origin has added a new variant."
  }

  final case class EnumRemovedVariant(origin: UpgradedRecordOrigin) extends Error {
    override def message: String =
      s"The upgraded $origin has removed an existing variant."
  }

  final case class EnumVariantsOrderChanged(origin: UpgradedRecordOrigin) extends Error {
    override def message: String =
      s"The upgraded $origin has changed the order of its variants - any new variant must be added at the end of the enum."
  }

  final case class DecreasingLfVersion(
      pastVersion: LanguageVersion,
      presentVersion: LanguageVersion,
  ) extends Error {
    override def message: String =
      s"The upgraded package uses an older LF version (${presentVersion.pretty} < ${pastVersion.pretty})"
  }

  final case class DependencyHasLowerVersionDespiteUpgrade(
      depName: Ref.PackageName,
      depPresentVersion: Ref.PackageVersion,
      depPastVersion: Ref.PackageVersion,
  ) extends Error {
    override def message: String =
      s"Dependency $depName has version $depPresentVersion on the upgrading package, which is older than version $depPastVersion on the upgraded package.\nDependency versions of upgrading packages must always be greater or equal to the dependency versions on upgraded packages."
  }

  final case class TriedToUpgradeIface(iface: Ref.DottedName) extends Error {
    override def message: String =
      s"Tried to upgrade interface $iface, but interfaces cannot be upgraded. They should be removed in any upgrading package."
  }

  final case class MissingImplementation(tpl: Ref.DottedName, iface: Ref.TypeConName)
      extends Error {
    override def message: String =
      s"Implementation of interface $iface by template $tpl appears in package that is being upgraded, but does not appear in this package."
  }

  final case class ForbiddenNewInstance(tpl: Ref.DottedName, iface: Ref.TypeConName) extends Error {
    override def message: String =
      s"Implementation of interface $iface by template $tpl appears in this package, but does not appear in package that is being upgraded."
  }

  final case class TriedToUpgradeException(exception: Ref.DottedName) extends Error {
    override def message: String =
      s"Tried to upgrade exception $exception, but exceptions cannot be upgraded. They should be removed in any upgrading package."
  }

  final case class DatatypeBecameUnserializable(origin: UpgradedRecordOrigin) extends Error {
    override def message: String =
      s"The upgraded $origin was serializable and is now unserializable. Datatypes cannot change their serializability via upgrades."
  }

  final case class DifferentParamsCount(origin: UpgradedRecordOrigin) extends Error {
    override def message: String =
      s"The upgraded $origin has changed the number of type variables it has."
  }

  final case class DifferentParamsKinds(origin: UpgradedRecordOrigin) extends Error {
    override def message: String =
      s"The upgraded $origin has changed the kind of one of its type variables."
  }
}

sealed abstract class UpgradedRecordOrigin

final case class TemplateBody(template: Ref.DottedName) extends UpgradedRecordOrigin {
  override def toString(): String =
    s"template $template"
}

final case class TemplateChoiceInput(template: Ref.DottedName, choice: Ref.ChoiceName)
    extends UpgradedRecordOrigin {
  override def toString(): String =
    s"input type of choice $choice on template $template"
}

final case class VariantConstructor(datatype: Ref.DottedName, variant: Ref.TypeConName)
    extends UpgradedRecordOrigin {
  override def toString(): String =
    s"variant constructor ${variant.qualifiedName.name} from variant $datatype"
}

final case class TopLevel(datatype: Ref.DottedName) extends UpgradedRecordOrigin {
  override def toString(): String =
    s"data type $datatype"
}

final case class ModuleWithMetadata(module: Ast.Module) {
  type ChoiceNameMap = Map[Ref.DottedName, (Ref.DottedName, Ref.ChoiceName)]

  lazy val choiceNameMap: ChoiceNameMap =
    for {
      (templateName, template) <- module.templates
      prefix = templateName.segments.init
      (choiceName, choice) <- template.choices
      fullName = prefix.slowSnoc(choiceName)
    } yield (Ref.DottedName.unsafeFromNames(fullName), (templateName, choiceName))

  type VariantNameMap = Map[Ref.DottedName, (Ref.DottedName, Ref.TypeConName)]

  lazy val variantNameMap: VariantNameMap =
    for {
      (dataTypeName, Ast.DDataType(_, _, variant: Ast.DataVariant)) <- module.definitions
      (recordName, variantType) <- variant.variants.iterator
      variantName <- leftMostApp(variantType).iterator
      fullName = dataTypeName.segments.slowSnoc(recordName)
    } yield (Ref.DottedName.unsafeFromNames(fullName), (dataTypeName, variantName))

  private def leftMostApp(typ: Ast.Type): Option[Ref.TypeConName] = {
    typ match {
      case Ast.TApp(func, arg @ _) => leftMostApp(func)
      case Ast.TTyCon(typeConName) => Some(typeConName)
      case _ => None
    }
  }

  def dataTypeOrigin(
      name: Ref.DottedName
  ): UpgradedRecordOrigin = {
    module.templates.get(name) match {
      case Some(template @ _) => TemplateBody(name)
      case None => {
        choiceNameMap.get(name) match {
          case Some((templateName, choiceName)) =>
            TemplateChoiceInput(templateName, choiceName)
          case None =>
            variantNameMap.get(name) match {
              case Some((dataTypeName, variantName)) =>
                VariantConstructor(dataTypeName, variantName);
              case _ => TopLevel(name)
            }
        }
      }
    }
  }

}

private case class Env(
    currentDepth: Int = 0,
    binderDepth: Map[TypeVarName, Int] = Map.empty,
) {
  def extend(varNames: ImmArray[TypeVarName]): Env = {
    varNames.foldLeft(this) { case (env, varName) =>
      env.extend(varName)
    }
  }

  def extend(varName: TypeVarName): Env = Env(
    currentDepth + 1,
    binderDepth.updated(varName, currentDepth),
  )
}

/** A datatype closing over the free type variables of [[value]] with [[env]]. */
private case class Closure[A](env: Env, value: A)

abstract class TypecheckUpgradesUtils(
    val packageMap: Map[
      Ref.PackageId,
      (Ref.PackageName, Ref.PackageVersion),
    ]
) extends NamedLogging {
  protected def getIfUpgradeable(
      pkgId: Ref.PackageId
  ): Option[(Ref.PackageName, Ref.PackageVersion)] = {
    packageMap.get(pkgId)
  }

  protected def failIf(predicate: Boolean, err: => UpgradeError.Error): Try[Unit] =
    if (predicate)
      fail(err)
    else
      Success(())

  protected def fail[A](err: UpgradeError.Error): Try[A] =
    Failure(UpgradeError(err))

  protected def warn(err: UpgradeError.Error)(implicit
      loggingContext: LoggingContextWithTrace
  ): Unit =
    logger.warn(s"Warning while typechecking upgrades: ${err.message}")

  protected def tryAll[A, B](t: Iterable[A], f: A => Try[B]): Try[Seq[B]] =
    t.toSeq.traverse(f)

  protected def extractDelExistNew[K, V](
      arg: Upgrading[Map[K, V]]
  ): (Map[K, V], Map[K, Upgrading[V]], Map[K, V]) =
    (
      arg.past -- arg.present.keySet,
      arg.past.keySet
        .intersect(arg.present.keySet)
        .map(k => k -> Upgrading(arg.past(k), arg.present(k)))
        .toMap,
      arg.present -- arg.past.keySet,
    )

  protected def checkDeleted[K, V](
      arg: Upgrading[Map[K, V]],
      handler: (K, V) => UpgradeError.Error,
      filter: (K, V) => Boolean = (_: K, _: V) => true,
  ): Try[(Map[K, Upgrading[V]], Map[K, V])] = {
    val (deletedV, existingV, newV) = extractDelExistNew(arg)
    deletedV.view.filter((kv: (K, V)) => filter(kv._1, kv._2)).headOption match {
      case Some((k, v)) => fail(handler(k, v))
      case _ => Success((existingV, newV))
    }
  }

  protected def checkLfVersions(
      arg: Upgrading[LanguageVersion]
  ): Try[Unit] = {
    import Ordering.Implicits._
    if (arg.past > arg.present)
      fail(UpgradeError.DecreasingLfVersion(arg.past, arg.present))
    else
      Success(())
  }

}

object TypecheckUpgrades {
  sealed abstract class UploadPhaseCheck[A] {
    def uploadedPackage: A
    def map[B](f: A => B): UploadPhaseCheck[B]
  }
  implicit class UploadPhaseCheckOptionalHelper[A](
      phase: UploadPhaseCheck[Option[A]]
  ) {
    def sequenceOptional: Option[UploadPhaseCheck[A]] =
      phase match {
        case MaximalDarCheck(Some(oldPkg), Some(newPkg)) => Some(MaximalDarCheck(oldPkg, newPkg))
        case MinimalDarCheck(Some(oldPkg), Some(newPkg)) => Some(MinimalDarCheck(oldPkg, newPkg))
        case StandaloneDarCheck(Some(newPkg)) => Some(StandaloneDarCheck(newPkg))
        case MaximalDarCheck(_, _) => None
        case MinimalDarCheck(_, _) => None
        case StandaloneDarCheck(_) => None
      }
  }
  final case class MinimalDarCheck[A](
      oldPackage: A,
      newPackage: A,
  ) extends UploadPhaseCheck[A] {
    override def uploadedPackage = newPackage
    override def toString: String = "minimal-dar-check"
    override def map[B](f: A => B) = MinimalDarCheck(f(oldPackage), f(newPackage))
  }
  final case class MaximalDarCheck[A](
      oldPackage: A,
      newPackage: A,
  ) extends UploadPhaseCheck[A] {
    override def uploadedPackage = oldPackage
    override def toString: String = "maximal-dar-check"
    override def map[B](f: A => B) = MaximalDarCheck(f(oldPackage), f(newPackage))
  }
  final case class StandaloneDarCheck[A](
      newPackage: A
  ) extends UploadPhaseCheck[A] {
    override def uploadedPackage = newPackage
    override def toString: String = "standalone-dar-check"
    override def map[B](f: A => B) = StandaloneDarCheck(f(newPackage))
  }
}

case class TypecheckUpgrades(
    val loggerFactory: NamedLoggerFactory
) {
  private def typecheckUpgradesStandalone(
      packageMap: Map[
        Ref.PackageId,
        (Ref.PackageName, Ref.PackageVersion),
      ],
      present: (
          Util.PkgIdWithNameAndVersion,
          Ast.Package,
      ),
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Try[Unit] = {
    val (presentPackageId, presentPkg) = present
    val tc =
      TypecheckUpgradesStandalone(
        (presentPackageId.pkgId, presentPkg),
        packageMap + (presentPackageId.pkgId -> (presentPkg.name.get, presentPkg.metadata.get.version)),
        loggerFactory,
      )
    tc.check()
  }

  private def typecheckUpgradesPair(
      packageMap: Map[
        Ref.PackageId,
        (Ref.PackageName, Ref.PackageVersion),
      ],
      past: (
          Util.PkgIdWithNameAndVersion,
          Ast.Package,
      ),
      present: (
          Util.PkgIdWithNameAndVersion,
          Ast.Package,
      ),
  ): Try[Unit] = {
    val (pastPackageId, pastPkg) = past
    val (presentPackageId, presentPkg) = present
    val tc = TypecheckUpgradesPair(
      Upgrading((pastPackageId.pkgId, pastPkg), (presentPackageId.pkgId, presentPkg)),
      packageMap
        + (presentPackageId.pkgId -> (presentPkg.name.get, presentPkg.metadata.get.version))
        + (pastPackageId.pkgId -> (pastPkg.name.get, pastPkg.metadata.get.version)),
      loggerFactory,
    )
    tc.check()
  }

  def typecheckUpgrades(
      packageMap: Map[
        Ref.PackageId,
        (Ref.PackageName, Ref.PackageVersion),
      ],
      phase: TypecheckUpgrades.UploadPhaseCheck[
        (
            Util.PkgIdWithNameAndVersion,
            Ast.Package,
        )
      ],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Try[Unit] = {
    phase match {
      case p: TypecheckUpgrades.MaximalDarCheck[_] =>
        typecheckUpgradesPair(packageMap, p.oldPackage, p.newPackage)
      case p: TypecheckUpgrades.MinimalDarCheck[_] =>
        typecheckUpgradesPair(packageMap, p.oldPackage, p.newPackage)
      case p: TypecheckUpgrades.StandaloneDarCheck[_] =>
        typecheckUpgradesStandalone(packageMap, p.newPackage)
    }
  }
}

case class TypecheckUpgradesStandalone(
    pkg: (Ref.PackageId, Ast.Package),
    override val packageMap: Map[
      Ref.PackageId,
      (Ref.PackageName, Ref.PackageVersion),
    ],
    val loggerFactory: NamedLoggerFactory,
) extends TypecheckUpgradesUtils(packageMap = packageMap) {

  def check()(implicit
      loggingContext: LoggingContextWithTrace
  ): Try[Unit] = {
    for {
      mod <- pkg._2.modules.values
      (tplName, template) <- mod.templates
      instance <- template.implements
      ifaceId = instance._2.interfaceId
    } {
      if (getIfUpgradeable(ifaceId.packageId).isEmpty) {
        warn(
          UpgradeError.CannotImplementNonUpgradeableInterface(
            pkg._1,
            ifaceId,
            tplName,
          )
        )
      }
    }

    Try(())
  }
}

case class TypecheckUpgradesPair(
    packages: Upgrading[
      (Ref.PackageId, Ast.Package)
    ],
    override val packageMap: Map[
      Ref.PackageId,
      (Ref.PackageName, Ref.PackageVersion),
    ],
    val loggerFactory: NamedLoggerFactory,
) extends TypecheckUpgradesUtils(packageMap = packageMap) {
  private lazy val _package: Upgrading[Ast.Package] = packages.map(_._2)

  def check(): Try[Unit] = {
    for {
      _ <- checkLfVersions(_package.map(_.languageVersion))
      (upgradedModules, newModules @ _) <-
        checkDeleted(
          _package.map(_.modules),
          (name: Ref.ModuleName, _: Ast.Module) => UpgradeError.MissingModule(name),
        )
      _ <- tryAll(upgradedModules.values, checkModule(_))
    } yield ()
  }

  private def splitModuleDts(
      module: Ast.Module
  ): (
      Map[Ref.DottedName, (Ast.DDataType, Ast.DefInterface)],
      Map[Ref.DottedName, (Ast.DDataType, Ast.DefException)],
      Map[Ref.DottedName, Ast.DDataType],
  ) = {
    val datatypes: Map[Ref.DottedName, Ast.DDataType] = module.definitions.collect({
      case (name, dt: Ast.DDataType) => (name, dt)
    })
    val (ifaces, other1) = datatypes.partitionMap({ case (tcon, dt) =>
      lookupInterfaceOrException(module, tcon, dt)
    })
    val (exceptions, other) = other1.partitionMap(identity)
    (ifaces.toMap, exceptions.toMap, other.toMap)
  }

  private def lookupInterfaceOrException(
      module: Ast.Module,
      tcon: Ref.DottedName,
      dt: Ast.DDataType,
  ): Either[
    (Ref.DottedName, (Ast.DDataType, Ast.DefInterface)),
    Either[
      (Ref.DottedName, (Ast.DDataType, Ast.DefException)),
      (Ref.DottedName, Ast.DDataType),
    ],
  ] = {
    module.interfaces.get(tcon) match {
      case None =>
        Right(module.exceptions.get(tcon) match {
          case None => Right((tcon, dt))
          case Some(exception) => Left((tcon, (dt, exception)))
        })
      case Some(iface) => Left((tcon, (dt, iface)))
    }
  }

  def flattenInstances(
      module: Ast.Module
  ): Map[(Ref.DottedName, Ref.TypeConName), (Ast.Template, Ast.TemplateImplements)] = {
    for {
      (templateName, template) <- module.templates
      (implName, impl) <- template.implements
    } yield ((templateName, implName), (template, impl))
  }

  private def checkModule(module: Upgrading[Ast.Module]): Try[Unit] = {
    val (pastIfaceDts, pastExceptionDts, pastUnownedDts) = splitModuleDts(module.past)
    val (presentIfaceDts, presentExceptionDts, presentUnownedDts) = splitModuleDts(module.present)
    val ifaceDts = Upgrading(past = pastIfaceDts, present = presentIfaceDts)
    val exceptionDts = Upgrading(past = pastExceptionDts, present = presentExceptionDts)
    val unownedDts = Upgrading(past = pastUnownedDts, present = presentUnownedDts)

    val moduleWithMetadata = module.map(ModuleWithMetadata)
    for {
      (existingTemplates, newTemplates) <- checkDeleted(
        module.map(_.templates),
        (name: Ref.DottedName, _: Ast.Template) => UpgradeError.MissingTemplate(name),
      )
      _ <- tryAll(existingTemplates, checkTemplate(_))

      (_ifaceDel, ifaceExisting, _ifaceNew) = extractDelExistNew(ifaceDts)
      _ <- checkContinuedIfaces(ifaceExisting)

      (_exceptionDel, exceptionExisting, _exceptionNew) = extractDelExistNew(exceptionDts)
      _ <- checkContinuedExceptions(exceptionExisting)

      (instanceDel, _instanceExisting, instanceNew) = extractDelExistNew(
        module.map(flattenInstances)
      )
      _ <- checkDeletedInstances(instanceDel)
      _ <- checkAddedInstances(instanceNew.view.filterKeys { case (tyCon, _) =>
        !newTemplates.contains(tyCon)
      }.toMap)

      (existingDatatypes, _new) <- checkDeleted(
        unownedDts,
        (name: Ref.DottedName, _: Ast.DDataType) => UpgradeError.MissingDataCon(name),
        filter = (_: Ref.DottedName, dt: Ast.DDataType) => dt.serializable,
      )
      _ <- tryAll(existingDatatypes, checkDatatype(moduleWithMetadata, _))
    } yield ()
  }

  private def checkContinuedIfaces(
      ifaces: Map[Ref.DottedName, Upgrading[(Ast.DDataType, Ast.DefInterface)]]
  ): Try[Unit] = {
    tryAll(
      ifaces,
      (arg: (Ref.DottedName, Upgrading[(Ast.DDataType, Ast.DefInterface)])) => {
        val (name, _) = arg
        // TODO (dylant-da): Re-enable this line if the -Wupgrade-interfaces
        // flag on the compiler goes away and interface upgrades become an
        // always-error
        // fail(UpgradeError.TriedToUpgradeIface(name))
        val _ = UpgradeError.TriedToUpgradeIface(name)
        Try(())
      },
    ).map(_ => ())
  }

  private def checkContinuedExceptions(
      exceptions: Map[Ref.DottedName, Upgrading[(Ast.DDataType, Ast.DefException)]]
  ): Try[Unit] = {
    tryAll(
      exceptions,
      (arg: (Ref.DottedName, Upgrading[(Ast.DDataType, Ast.DefException)])) => {
        val (name, _) = arg
        // TODO (dylant-da): Re-enable this line if the -Wupgrade-exceptions
        // flag on the compiler goes away and exception upgrades become an
        // always-error
        // fail(UpgradeError.TriedToUpgradeException(name))
        val _ = UpgradeError.TriedToUpgradeException(name)
        Try(())
      },
    ).map(_ => ())
  }

  private def checkDeletedInstances(
      deletedInstances: Map[(Ref.DottedName, TypeConName), (Ast.Template, Ast.TemplateImplements)]
  ): Try[Unit] =
    deletedInstances.headOption match {
      case Some(((tpl, iface), _)) => fail(UpgradeError.MissingImplementation(tpl, iface))
      case None => Success(())
    }

  private def checkAddedInstances(
      newInstances: Map[(Ref.DottedName, TypeConName), (Ast.Template, Ast.TemplateImplements)]
  ): Try[Unit] =
    newInstances.headOption match {
      case Some(((tpl, iface), _)) => fail(UpgradeError.ForbiddenNewInstance(tpl, iface))
      case None => Success(())
    }

  private def checkTemplate(
      templateAndName: (Ref.DottedName, Upgrading[Ast.Template])
  ): Try[Unit] = {
    val (templateName, template) = templateAndName
    for {
      (existingChoices, _newChoices) <- checkDeleted(
        template.map(_.choices),
        (name: Ref.ChoiceName, _: Ast.TemplateChoice) => UpgradeError.MissingChoice(name),
      )

      _ <- tryAll(existingChoices.values, checkChoice(_))
      _ <- checkKey(templateName, template.map(_.key))
    } yield ()
  }

  private def checkIdentifiers(past: Ref.Identifier, present: Ref.Identifier): Boolean = {
    val compatibleNames = past.qualifiedName == present.qualifiedName
    val compatiblePackages =
      (getIfUpgradeable(past.packageId), getIfUpgradeable(present.packageId)) match {
        // The two packages have LF versions < 1.17.
        // They must be the exact same package as LF < 1.17 don't support upgrades.
        case (None, None) => past.packageId == present.packageId
        // The two packages have LF versions >= 1.17.
        // The present package must be a valid upgrade of the past package. Since we validate uploaded packages in
        // topological order, the package version ordering is a proxy for the "upgrades" relationship.
        case (Some((pastName, pastVersion)), Some((presentName, presentVersion))) =>
          pastName == presentName && pastVersion <= presentVersion
        // LF versions < 1.17 and >= 1.17 are not comparable.
        case (_, _) => false
      }
    compatibleNames && compatiblePackages
  }

  @tailrec
  private def checkTypeList(envPast: Env, envPresent: Env, trips: List[(Type, Type)]): Boolean =
    trips match {
      case Nil => true
      case (t1, t2) :: trips =>
        (t1, t2) match {
          case (TVar(x1), TVar(x2)) =>
            envPast.binderDepth(x1) == envPresent.binderDepth(x2) &&
            checkTypeList(envPast, envPresent, trips)
          case (TNat(n1), TNat(n2)) =>
            n1 == n2 && checkTypeList(envPast, envPresent, trips)
          case (TTyCon(c1), TTyCon(c2)) =>
            checkIdentifiers(c1, c2) && checkTypeList(envPast, envPresent, trips)
          case (TApp(f1, a1), TApp(f2, a2)) =>
            checkTypeList(envPast, envPresent, (f1, f2) :: (a1, a2) :: trips)
          case (TBuiltin(b1), TBuiltin(b2)) =>
            b1 == b2 && checkTypeList(envPast, envPresent, trips)
          case _ =>
            false
        }
    }

  private def checkType(typ: Upgrading[Closure[Ast.Type]]): Boolean = {
    checkTypeList(typ.past.env, typ.present.env, List((typ.past.value, typ.present.value)))
  }

  private def unifyIdentifier(id: Ref.Identifier): Ref.Identifier =
    Ref.Identifier(Ref.PackageId.assertFromString("0"), id.qualifiedName)

  private def unifyUpgradedRecordOrigin(origin: UpgradedRecordOrigin): UpgradedRecordOrigin =
    origin match {
      case _: TemplateBody => origin
      case TemplateChoiceInput(template, choice) =>
        TemplateChoiceInput(template, choice)
      case VariantConstructor(datatype, variant) =>
        VariantConstructor(datatype, unifyIdentifier(variant))
      case TopLevel(datatype: Ref.DottedName) =>
        TopLevel(datatype)
    }

  private def checkKey(
      templateName: Ref.DottedName,
      key: Upgrading[Option[Ast.TemplateKey]],
  ): Try[Unit] = {
    key match {
      case Upgrading(None, None) => Success(());
      case Upgrading(Some(pastKey), Some(presentKey)) => {
        val keyPastPresent = Upgrading(pastKey.typ, presentKey.typ)
        if (!checkType(Upgrading(Closure(Env(), pastKey.typ), Closure(Env(), presentKey.typ))))
          fail(UpgradeError.TemplateChangedKeyType(templateName, keyPastPresent))
        else
          Success(())
      }
      case Upgrading(Some(pastKey @ _), None) =>
        fail(UpgradeError.TemplateRemovedKey(templateName, pastKey))
      case Upgrading(None, Some(presentKey @ _)) =>
        fail(UpgradeError.TemplateAddedKey(templateName, presentKey))
    }
  }

  private def checkChoice(choice: Upgrading[Ast.TemplateChoice]): Try[Unit] = {
    val returnType = choice.map(_.returnType)
    if (checkType(returnType.map(Closure(Env(), _)))) {
      Success(())
    } else {
      fail(UpgradeError.ChoiceChangedReturnType(choice.present.name, returnType))
    }
  }

  private def checkDatatype(
      moduleWithMetadata: Upgrading[ModuleWithMetadata],
      nameAndDatatype: (Ref.DottedName, Upgrading[Ast.DDataType]),
  ): Try[Unit] = {
    val (name, datatype: Upgrading[Ast.DDataType]) = nameAndDatatype
    val origin = moduleWithMetadata.map(_.dataTypeOrigin(name))
    datatype.map(_.serializable) match {
      case Upgrading(true /* past */, false /* present */ ) =>
        fail(UpgradeError.DatatypeBecameUnserializable(origin.present))
      case Upgrading(false /* past */, true /* present */ ) =>
        Success(())
      case Upgrading(false /* past */, false /* present */ ) =>
        Success(())
      case Upgrading(true, true) =>
        if (unifyUpgradedRecordOrigin(origin.present) != unifyUpgradedRecordOrigin(origin.past)) {
          fail(UpgradeError.RecordChangedOrigin(name, origin))
        } else {
          val env = datatype.map(dt => Env().extend(dt.params.map(_._1)))

          val paramsLengthMatch = datatype.map(_.params.length).fold(_ == _)
          val allKindsMatch = datatype.map(_.params.map(_._2)).fold(_ == _)

          for {
            _ <- failIf(!paramsLengthMatch, UpgradeError.DifferentParamsCount(origin.present))
            _ <- failIf(!allKindsMatch, UpgradeError.DifferentParamsKinds(origin.present))

            _ <- datatype.map(_.cons) match {
              case Upgrading(past: Ast.DataRecord, present: Ast.DataRecord) =>
                checkFields(
                  origin.present,
                  Upgrading(Closure(env.past, past), Closure(env.present, present)),
                )
              case Upgrading(past: Ast.DataVariant, present: Ast.DataVariant) =>
                val upgrade = Upgrading(past, present)
                val variants: Upgrading[Map[Ast.VariantConName, Ast.Type]] =
                  upgrade.map(variant => Map.from(variant.variants.iterator))
                for {
                  (existing, new_) <- checkDeleted(
                    variants,
                    (_: Ast.VariantConName, _: Ast.Type) =>
                      UpgradeError.VariantRemovedVariant(origin.present),
                  )

                  changedTypes = existing.filter { case (field @ _, typ) =>
                    !checkType(env.zip(typ, Closure.apply _))
                  }
                  _ <-
                    if (changedTypes.nonEmpty)
                      fail(UpgradeError.VariantChangedVariantType(origin.present))
                    else Success(())

                  changedVariantNames: ImmArray[(Ast.VariantConName, Ast.VariantConName)] = {
                    val variantNames: Upgrading[ImmArray[Ast.VariantConName]] =
                      upgrade.map(_.variants.map(_._1))
                    variantNames.past.zip(variantNames.present).filter { case (past, present) =>
                      past != present
                    }
                  }
                  _ <- failIf(
                    changedVariantNames.nonEmpty,
                    UpgradeError.VariantVariantsOrderChanged(origin.present),
                  )
                } yield ()
              case Upgrading(past: Ast.DataEnum, present: Ast.DataEnum) =>
                val upgrade = Upgrading(past, present)
                val enums: Upgrading[Map[Ast.EnumConName, Unit]] =
                  upgrade.map(enums =>
                    Map.from(enums.constructors.iterator.map(enum => (enum, ())))
                  )
                for {
                  (_, new_) <- checkDeleted(
                    enums,
                    (_: Ast.EnumConName, _: Unit) => UpgradeError.EnumRemovedVariant(origin.present),
                  )
                  changedVariantNames: ImmArray[(Ast.EnumConName, Ast.EnumConName)] = {
                    val variantNames: Upgrading[ImmArray[Ast.EnumConName]] =
                      upgrade.map(_.constructors)
                    variantNames.past.zip(variantNames.present).filter { case (past, present) =>
                      past != present
                    }
                  }
                  _ <- failIf(
                    changedVariantNames.nonEmpty,
                    UpgradeError.EnumVariantsOrderChanged(origin.present),
                  )
                } yield ()
              case Upgrading(Ast.DataInterface, Ast.DataInterface) => Try(())
              case other => fail(UpgradeError.MismatchDataConsVariety(name, other))
            }
          } yield ()
        }
    }
  }

  private def checkFields(
      origin: UpgradedRecordOrigin,
      recordClosures: Upgrading[Closure[Ast.DataRecord]],
  ): Try[Unit] = {
    val env = recordClosures.map(_.env)
    val fields: Upgrading[Map[Ast.FieldName, Ast.Type]] =
      recordClosures.map(rec => Map.from(rec.value.fields.iterator))
    def fieldTypeOptional(typ: Ast.Type): Boolean =
      typ match {
        case Ast.TApp(Ast.TBuiltin(Ast.BTOptional), _) => true
        case _ => false
      }

    val (_deleted, _existing, _new_) = extractDelExistNew(fields)
    for {
      // Much like in the Haskell impl, first we check for missing fields
      _ <- failIf(_deleted.nonEmpty, UpgradeError.RecordFieldsMissing(origin, _deleted))

      // Then we check for changed types
      changedTypes = _existing.filter { case (field @ _, typ) =>
        !checkType(env.zip(typ, Closure.apply _))
      }
      _ <- failIf(
        changedTypes.nonEmpty,
        UpgradeError.RecordFieldsExistingChanged(origin, changedTypes),
      )

      // Then we check for new non-optional types, and vary the message if its a variant
      newNonOptionalTypes = _new_.find { case (field @ _, typ) => !fieldTypeOptional(typ) }
      _ <- failIf(
        newNonOptionalTypes.nonEmpty,
        origin match {
          case _: VariantConstructor => {
            UpgradeError.VariantAddedVariantField(origin)
          }
          case _ => {
            UpgradeError.RecordFieldsNewNonOptional(origin)
          }
        },
      )

      // Finally, reordered field names
      changedFieldNames: ImmArray[(Ast.FieldName, Ast.FieldName)] = {
        val fieldNames: Upgrading[ImmArray[Ast.FieldName]] =
          recordClosures.map(_.value.fields.map(_._1))
        fieldNames.past.zip(fieldNames.present).filter { case (past, present) => past != present }
      }
      _ <- failIf(changedFieldNames.nonEmpty, UpgradeError.RecordFieldsOrderChanged(origin))
    } yield ()
  }
}
