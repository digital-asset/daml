// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package validation

import com.daml.lf.data.Ref
import com.daml.lf.language.Ast

import scala.util.{Failure, Success, Try}
import com.daml.lf.data.ImmArray
import com.daml.lf.language.Ast.{
  TApp,
  TBuiltin,
  TForall,
  TNat,
  TStruct,
  TSynApp,
  TTyCon,
  TVar,
  Type,
  TypeVarName,
}
import com.daml.lf.language.LanguageVersion

import scala.annotation.tailrec
import scala.collection.immutable.Map

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
      s"EUpgradeMismatchDataConsVariety $dataConName"
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
    binderDepthLhs: Map[TypeVarName, Int] = Map.empty,
    binderDepthRhs: Map[TypeVarName, Int] = Map.empty,
) {
  def extend(varName1: TypeVarName, varName2: TypeVarName) = Env(
    currentDepth + 1,
    binderDepthLhs.updated(varName1, currentDepth),
    binderDepthRhs.updated(varName2, currentDepth),
  )
}

object TypecheckUpgrades {

  sealed abstract class UploadPhaseCheck
  object DarCheck extends UploadPhaseCheck {
    override def toString: String = "dar-check"
  }
  object MinimalDarCheck extends UploadPhaseCheck {
    override def toString: String = "minimal-dar-check"
  }
  object MaximalDarCheck extends UploadPhaseCheck {
    override def toString: String = "maximal-dar-check"
  }

  private def failIf(predicate: Boolean, err: => UpgradeError.Error): Try[Unit] =
    if (predicate)
      fail(err)
    else
      Success(())

  protected def fail[A](err: UpgradeError.Error): Try[A] =
    Failure(UpgradeError(err))

  private def extractDelExistNew[K, V](
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

  private def checkDeleted[K, V](
      arg: Upgrading[Map[K, V]],
      handler: (K, V) => UpgradeError.Error,
  ): Try[(Map[K, Upgrading[V]], Map[K, V])] = {
    val (deletedV, existingV, newV) = extractDelExistNew(arg)
    deletedV.headOption match {
      case Some((k, v)) => fail(handler(k, v))
      case _ => Success((existingV, newV))
    }
  }

  private def checkLfVersions(
      arg: Upgrading[LanguageVersion]
  ): Try[Unit] = {
    import Ordering.Implicits._
    if (arg.past > arg.present)
      fail(UpgradeError.DecreasingLfVersion(arg.past, arg.present))
    else
      Success(())
  }

  private def tryAll[A, B](t: Iterable[A], f: A => Try[B]): Try[Seq[B]] =
    Try(t.map(f(_).get).toSeq)

  def typecheckUpgrades(
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)],
      present: (
          Ref.PackageId,
          Ast.Package,
      ),
      pastPackageId: Ref.PackageId,
      mbPastPkg: Option[Ast.Package],
  ): Try[Unit] = {
    mbPastPkg match {
      case None =>
        fail(UpgradeError.CouldNotResolveUpgradedPackageId(Upgrading(pastPackageId, present._1)));
      case Some(pastPkg) =>
        val (presentPackageId, presentPkg) = present
        val tc = TypecheckUpgrades(
          packageMap
            + (presentPackageId -> (presentPkg.name.get, presentPkg.metadata.get.version))
            + (pastPackageId -> (pastPkg.name.get, pastPkg.metadata.get.version)),
          Upgrading((pastPackageId, pastPkg), present),
        )
        tc.check()
    }
  }
}

case class TypecheckUpgrades(
    packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)],
    packages: Upgrading[
      (Ref.PackageId, Ast.Package)
    ],
) {
  import TypecheckUpgrades._

  private lazy val _package: Upgrading[Ast.Package] = packages.map(_._2)

  private def check(): Try[Unit] = {
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
      Map[Ref.DottedName, Ast.DDataType],
  ) = {
    val datatypes: Map[Ref.DottedName, Ast.DDataType] = module.definitions.collect({
      case (name, dt: Ast.DDataType) => (name, dt)
    })
    val (ifaces, other) = datatypes.partitionMap({ case (tcon, dt) =>
      lookupInterface(module, tcon, dt)
    })
    (ifaces.toMap, other.filter(_._2.serializable).toMap)
  }

  private def lookupInterface(
      module: Ast.Module,
      tcon: Ref.DottedName,
      dt: Ast.DDataType,
  ): Either[
    (Ref.DottedName, (Ast.DDataType, Ast.DefInterface)),
    (Ref.DottedName, Ast.DDataType),
  ] = {
    module.interfaces.get(tcon) match {
      case None => Right((tcon, dt))
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
    val (pastIfaceDts, pastUnownedDts) = splitModuleDts(module.past)
    val (presentIfaceDts, presentUnownedDts) = splitModuleDts(module.present)
    val ifaceDts = Upgrading(past = pastIfaceDts, present = presentIfaceDts)
    val unownedDts = Upgrading(past = pastUnownedDts, present = presentUnownedDts)

    val moduleWithMetadata = module.map(ModuleWithMetadata)
    for {
      (existingTemplates, _new) <- checkDeleted(
        module.map(_.templates),
        (name: Ref.DottedName, _: Ast.Template) => UpgradeError.MissingTemplate(name),
      )
      _ <- tryAll(existingTemplates, checkTemplate(_))

      (_ifaceDel, ifaceExisting, _ifaceNew) = extractDelExistNew(ifaceDts)
      _ <- checkContinuedIfaces(ifaceExisting)

      (_instanceExisting, _instanceNew) <-
        checkDeleted(
          module.map(flattenInstances(_)),
          (tplImpl: (Ref.DottedName, Ref.TypeConName), _: (Ast.Template, Ast.TemplateImplements)) =>
            {
              val (tpl, impl) = tplImpl
              UpgradeError.MissingImplementation(tpl, impl)
            },
        )

      (existingDatatypes, _new) <- checkDeleted(
        unownedDts,
        (name: Ref.DottedName, _: Ast.DDataType) => UpgradeError.MissingDataCon(name),
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
        fail(UpgradeError.TriedToUpgradeIface(name))
      },
    ).map(_ => ())
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
      (packageMap.get(past.packageId), packageMap.get(present.packageId)) match {
        // The two packages have LF versions < 1.16.
        // They must be the exact same package as LF < 1.16 don't support upgrades.
        case (None, None) => past.packageId == present.packageId
        // The two packages have LF versions >= 1.16.
        // The present package must be a valid upgrade of the past package. Since we validate uploaded packages in
        // topological order, the package version ordering is a proxy for the "upgrades" relationship.
        case (Some((pastName, pastVersion)), Some((presentName, presentVersion))) =>
          pastName == presentName && pastVersion <= presentVersion
        // LF versions < 1.16 and >= 1.16 are not comparable.
        case (_, _) => false
      }
    compatibleNames && compatiblePackages
  }

  @tailrec
  private def alphaEquivList(trips: List[(Env, Type, Type)]): Boolean = trips match {
    case Nil => true
    case (env, t1, t2) :: trips =>
      (t1, t2) match {
        case (TVar(x1), TVar(x2)) =>
          env.binderDepthLhs.get(x1).toLeft(t1) == env.binderDepthRhs.get(x2).toLeft(t2) &&
          alphaEquivList(trips)
        case (TNat(n1), TNat(n2)) =>
          n1 == n2 && alphaEquivList(trips)
        case (TTyCon(c1), TTyCon(c2)) =>
          checkIdentifiers(c1, c2) && alphaEquivList(trips)
        case (TApp(f1, a1), TApp(f2, a2)) =>
          alphaEquivList((env, f1, f2) :: (env, a1, a2) :: trips)
        case (TBuiltin(b1), TBuiltin(b2)) =>
          b1 == b2 && alphaEquivList(trips)
        case (TForall((varName1, kind1), b1), TForall((varName2, kind2), b2)) =>
          kind1 == kind2 && {
            val envExtended = env.extend(varName1, varName2)
            alphaEquivList((envExtended, b1, b2) :: trips)
          }
        case (TStruct(fs1), TStruct(fs2)) =>
          (fs1.names sameElements fs2.names) && {
            val more = (fs1.values zip fs2.values).map { case (x1, x2) => (env, x1, x2) }
            alphaEquivList(more ++: trips)
          }
        case (TSynApp(f, xs), TSynApp(g, ys)) =>
          // We treat type synonyms nominally here. If alpha equivalence
          // fails, we expand all of them and try again.
          checkIdentifiers(f, g) && xs.length == ys.length && {
            val more = (xs.iterator zip ys.iterator).map { case (x1, x2) => (env, x1, x2) }
            alphaEquivList(more ++: trips)
          }
        case _ =>
          false
      }
  }

  def alphaEquiv(t1: Type, t2: Type): Boolean = alphaEquivList(List((Env(), t1, t2)))

  private def checkType(typ: Upgrading[Ast.Type]): Boolean = {
    alphaEquiv(typ.past, typ.present)
  }

  // TODO: https://github.com/digital-asset/daml/pull/18377
  // For simplicity's sake, we check all names against one another as if they
  // are in the same package, because package ids do not tell us the package
  // name - in the future we should resolve package ids to their package names
  // so that we can rule out upgrades between packages that don't have the same
  // name.
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
        val key = Upgrading(pastKey.typ, presentKey.typ)
        if (!checkType(key))
          fail(UpgradeError.TemplateChangedKeyType(templateName, key))
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
    if (checkType(returnType)) {
      Success(())
    } else {
      fail(UpgradeError.ChoiceChangedReturnType(choice.present.name, returnType))
    }
  }

  private def checkDatatype(
      moduleWithMetadata: Upgrading[ModuleWithMetadata],
      nameAndDatatype: (Ref.DottedName, Upgrading[Ast.DDataType]),
  ): Try[Unit] = {
    val (name, datatype) = nameAndDatatype
    val origin = moduleWithMetadata.map(_.dataTypeOrigin(name))
    if (unifyUpgradedRecordOrigin(origin.present) != unifyUpgradedRecordOrigin(origin.past)) {
      fail(UpgradeError.RecordChangedOrigin(name, origin))
    } else {
      datatype.map(_.cons) match {
        case Upgrading(past: Ast.DataRecord, present: Ast.DataRecord) =>
          checkFields(origin.present, Upgrading(past, present))
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

            changedTypes = existing.filter { case (field @ _, typ) => !checkType(typ) }
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
            upgrade.map(enums => Map.from(enums.constructors.iterator.map(enum => (enum, ()))))
          for {
            (_, new_) <- checkDeleted(
              enums,
              (_: Ast.EnumConName, _: Unit) => UpgradeError.EnumRemovedVariant(origin.present),
            )
            changedVariantNames: ImmArray[(Ast.EnumConName, Ast.EnumConName)] = {
              val variantNames: Upgrading[ImmArray[Ast.EnumConName]] = upgrade.map(_.constructors)
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
    }
  }

  private def checkFields(
      origin: UpgradedRecordOrigin,
      records: Upgrading[Ast.DataRecord],
  ): Try[Unit] = {
    val fields: Upgrading[Map[Ast.FieldName, Ast.Type]] =
      records.map(rec => Map.from(rec.fields.iterator))
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
      changedTypes = _existing.filter { case (field @ _, typ) => !checkType(typ) }
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
        val fieldNames: Upgrading[ImmArray[Ast.FieldName]] = records.map(_.fields.map(_._1))
        fieldNames.past.zip(fieldNames.present).filter { case (past, present) => past != present }
      }
      _ <- failIf(changedFieldNames.nonEmpty, UpgradeError.RecordFieldsOrderChanged(origin))
    } yield ()
  }
}
