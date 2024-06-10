// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package validation

import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import scala.util.{Try, Success, Failure}
import com.daml.lf.validation.AlphaEquiv
import com.daml.lf.data.ImmArray
import com.daml.lf.language.LanguageVersion

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
      present: (
          Ref.PackageId,
          Ast.Package,
          Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)],
      ),
      pastPackageId: Ref.PackageId,
      mbPastPkg: Option[(Ast.Package, Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)])],
  ): Try[Unit] = {
    mbPastPkg match {
      case None =>
        fail(UpgradeError.CouldNotResolveUpgradedPackageId(Upgrading(pastPackageId, present._1)));
      case Some((pastPkg, pastPkgDeps)) =>
        val tc = TypecheckUpgrades(Upgrading((pastPackageId, pastPkg, pastPkgDeps), present))
        tc.check()
    }
  }

  def typecheckUpgrades(
      present: (
          Ref.PackageId,
          Ast.Package,
      ),
      pastPackageId: Ref.PackageId,
      mbPastPkg: Option[Ast.Package],
  ): Try[Unit] = {
    val emptyPackageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] = Map.empty
    val presentWithNoDeps = (present._1, present._2, emptyPackageMap)
    val mbPastPkgWithNoDeps = mbPastPkg.map((_, emptyPackageMap))
    TypecheckUpgrades.typecheckUpgrades(presentWithNoDeps, pastPackageId, mbPastPkgWithNoDeps)
  }
}

case class TypecheckUpgrades(
    packages: Upgrading[
      (Ref.PackageId, Ast.Package, Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)])
    ]
) {
  import TypecheckUpgrades._

  private lazy val _package: Upgrading[Ast.Package] = packages.map(_._2)
  private lazy val dependencies
      : Upgrading[Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)]] = packages.map(_._3)

  private def check(): Try[Unit] = {
    for {
      _ <- checkLfVersions(_package.map(_.languageVersion))
      (upgradedModules, newModules @ _) <-
        checkDeleted(
          _package.map(_.modules),
          (name: Ref.ModuleName, _: Ast.Module) => UpgradeError.MissingModule(name),
        )
      _ <- checkDeps()
      _ <- tryAll(upgradedModules.values, checkModule(_))
    } yield ()
  }

  private def checkDeps(): Try[Unit] = {
    val (_new @ _, existing, _deleted @ _) = extractDelExistNew(dependencies.map(_.values.toMap))
    tryAll(existing, checkDep).map(_ => ())
  }

  private def checkDep(dep: (Ref.PackageName, Upgrading[Ref.PackageVersion])): Try[Unit] = {
    val (depName, depVersions) = dep
    failIf(
      depVersions.present < depVersions.past,
      UpgradeError.DependencyHasLowerVersionDespiteUpgrade(
        depName,
        depVersions.present,
        depVersions.past,
      ),
    )
  }

  private def checkModule(module: Upgrading[Ast.Module]): Try[Unit] = {
    def datatypes(module: Ast.Module): Map[Ref.DottedName, Ast.DDataType] =
      module.definitions.flatMap(_ match {
        case (k, v: Ast.DDataType) => Some((k, v));
        case _ => None;
      })

    val moduleWithMetadata = module.map(ModuleWithMetadata)
    for {
      (existingTemplates, _new) <- checkDeleted(
        module.map(_.templates),
        (name: Ref.DottedName, _: Ast.Template) => UpgradeError.MissingTemplate(name),
      )
      _ <- tryAll(existingTemplates, checkTemplate(_))

      (existingDatatypes, _new) <- checkDeleted(
        module.map(datatypes(_)),
        (name: Ref.DottedName, _: Ast.DDataType) => UpgradeError.MissingDataCon(name),
      )
      _ <- tryAll(existingDatatypes, checkDatatype(moduleWithMetadata, _))
    } yield ()
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

  private def checkType(typ: Upgrading[Ast.Type]): Boolean = {
    AlphaEquiv.alphaEquiv(unifyTypes(typ.past), unifyTypes(typ.present))
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

  private def unifyTypes(typ: Ast.Type): Ast.Type = {
    typ match {
      case Ast.TNat(n) => Ast.TNat(n)
      case Ast.TSynApp(n, args) => Ast.TSynApp(unifyIdentifier(n), args.map(unifyTypes(_)))
      case Ast.TVar(n) => Ast.TVar(n)
      case Ast.TTyCon(con) => Ast.TTyCon(unifyIdentifier(con))
      case Ast.TBuiltin(bt) => Ast.TBuiltin(bt)
      case Ast.TApp(fun, arg) => Ast.TApp(unifyTypes(fun), unifyTypes(arg))
      case Ast.TForall(v, body) => Ast.TForall(v, unifyTypes(body))
      case Ast.TStruct(fields) => Ast.TStruct(fields.mapValues(unifyTypes(_)))
    }
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
    if (!datatype.past.serializable || !datatype.present.serializable) {
      Success(())
    } else if (
      unifyUpgradedRecordOrigin(origin.present) != unifyUpgradedRecordOrigin(origin.past)
    ) {
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
