// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package validation

import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import scala.util.{Try, Success, Failure}
import com.daml.lf.validation.AlphaEquiv
import com.daml.lf.data.ImmArray

case class Upgrading[A](past: A, present: A) {
  def map[B](f: A => B): Upgrading[B] = Upgrading(f(past), f(present))
  def fold[B](f: (A, A) => B): B = f(past, present)
  def zip[B, C](that: Upgrading[B], f: (A, B) => C) =
    Upgrading(f(this.past, that.past), f(this.present, that.present))
}

sealed abstract class UpgradeError extends Throwable {
  def message(): String;
}

final case class CouldNotResolveUpgradedPackageId(packageId: Upgrading[Ref.PackageId])
    extends UpgradeError {
  override def message(): String =
    s"Package ${packageId.present} claims to upgrade package with id ${packageId.past}, but that package cannot be found."
}

final case class MissingModule(name: Ref.ModuleName) extends UpgradeError {
  override def message(): String =
    s"Module $name appears in package that is being upgraded, but does not appear in this package."
}

final case class MissingTemplate(name: Ref.DottedName) extends UpgradeError {
  override def message(): String =
    s"Template $name appears in package that is being upgraded, but does not appear in this package."
}

final case class MissingDataCon(name: Ref.DottedName) extends UpgradeError {
  override def message(): String =
    s"Datatype $name appears in package that is being upgraded, but does not appear in this package."
}

final case class MissingChoice(name: Ref.ChoiceName) extends UpgradeError {
  override def message(): String =
    s"Choice $name appears in package that is being upgraded, but does not appear in this package."
}

final case class TemplateChangedKeyType(key: Upgrading[Ast.Type]) extends UpgradeError {
  override def message(): String = s"TemplateChangedKeyType"
}

final case class TemplateRemovedKey(key: Ast.TemplateKey) extends UpgradeError {
  override def message(): String = s"TemplateRemovedKey"
}

final case class ChoiceChangedReturnType(typ: Upgrading[Ast.Type]) extends UpgradeError {
  override def message(): String = s"ChoiceChangedReturnType"
}

final case class RecordChangedOrigin(origin: Upgrading[UpgradedRecordOrigin]) extends UpgradeError {
  override def message(): String = "RecordChangedOrigin"
}

final case class MismatchDataConsVariety(variety: Upgrading[Ast.DataCons]) extends UpgradeError {
  override def message(): String = "MismatchDataConsVariety"
}

final case class RecordFieldsMissing(fields: Map[Ast.FieldName, Ast.Type]) extends UpgradeError {
  override def message(): String = "RecordFieldsMissing"
}

final case class RecordFieldsExistingChanged(fields: Map[Ast.FieldName, Upgrading[Ast.Type]]) extends UpgradeError {
  override def message(): String = "RecordFieldsExistingChanged"
}

final case class RecordFieldsNewNonOptional() extends UpgradeError {
  override def message(): String = "RecordFieldsNewNonOptional"
}

final case class RecordFieldsOrderChanged() extends UpgradeError {
  override def message(): String = "RecordFieldsOrderChanged"
}

final case class VariantAddedVariant(origin: UpgradedRecordOrigin) extends UpgradeError {
  override def message(): String = "VariantAddedVariant"
}

final case class VariantRemovedVariant(origin: UpgradedRecordOrigin) extends UpgradeError {
  override def message(): String = "VariantRemovedVariant"
}

final case class VariantChangedVariantType(origin: UpgradedRecordOrigin) extends UpgradeError {
  override def message(): String = "VariantChangedVariantType"
}

final case class VariantAddedVariantField(origin: UpgradedRecordOrigin) extends UpgradeError {
  override def message(): String = "VariantAddedVariantField"
}

final case class EnumAddedVariant(origin: UpgradedRecordOrigin) extends UpgradeError {
  override def message(): String = "EnumAddedVariant"
}

final case class EnumRemovedVariant(origin: UpgradedRecordOrigin) extends UpgradeError {
  override def message(): String = "EnumRemovedVariant"
}

final case class UnknownUpgradeError(msg: String) extends UpgradeError {
  override def message(): String = this.msg
}

sealed abstract class UpgradedRecordOrigin

final case class TemplateBody(template: Ref.DottedName) extends UpgradedRecordOrigin
final case class TemplateChoiceInput(template: Ref.DottedName, choice: Ref.ChoiceName)
    extends UpgradedRecordOrigin
final case class VariantConstructor(template: Ref.DottedName, choice: Ref.TypeConName)
    extends UpgradedRecordOrigin
final case object TopLevel extends UpgradedRecordOrigin

object TypecheckUpgrades {
  def typecheckUpgrades(
      present: (Ref.PackageId, Ast.Package),
      pastPackageId: Ref.PackageId,
      mbPastPkg: Option[Ast.Package],
  ): Try[Unit] = {
    mbPastPkg match {
      case None => Failure(CouldNotResolveUpgradedPackageId(Upgrading(pastPackageId, present._1)));
      case Some(pastPkg) =>
        val tc = this(Upgrading((pastPackageId, pastPkg), present))
        tc.check()
    }
  }
}

case class ModuleWithMetadata(module: Ast.Module) {
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
      fullName = dataTypeName.segments.init.slowSnoc(recordName)
    } yield (Ref.DottedName.unsafeFromNames(fullName), (dataTypeName, variantName))

  private def leftMostApp(typ: Ast.Type): Option[Ref.TypeConName] = {
    typ match {
      case Ast.TApp(func, arg@_) => leftMostApp(func)
      case Ast.TTyCon(typeConName) => Some(typeConName)
      case _ => None
    }
  }

}

case class TypecheckUpgrades(packagesAndIds: Upgrading[(Ref.PackageId, Ast.Package)]) {
  private lazy val packageId: Upgrading[Ref.PackageId] = packagesAndIds.map(_._1)
  private lazy val _package: Upgrading[Ast.Package] = packagesAndIds.map(_._2)

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

  private def checkDeleted[K, V, T <: Throwable](
      arg: Upgrading[Map[K, V]],
      handler: (K, V) => T,
  ): Try[(Map[K, Upgrading[V]], Map[K, V])] = {
    val (deletedV, existingV, newV) = extractDelExistNew(arg)
    deletedV.headOption match {
      case Some((k, v)) => Failure(handler(k, v))
      case _ => Success((existingV, newV))
    }
  }

  private def tryAll[A, B](t: Iterable[A], f: A => Try[B]): Try[Seq[B]] =
    Try(t.map(f(_).get).toSeq)

  private def check(): Try[Unit] = {
    for {
      (upgradedModules, newModules @ _) <-
        checkDeleted(
          _package.map(_.modules),
          (name: Ref.ModuleName, _: Ast.Module) => MissingModule(name),
        )
      _ <- tryAll(upgradedModules.values, checkModule(_))
    } yield ()
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
        (name: Ref.DottedName, _: Ast.Template) => MissingTemplate(name)
      )
      _ <- tryAll(existingTemplates.values, checkTemplate(_))

      (existingDatatypes, _new) <- checkDeleted(
        module.map(datatypes(_)),
        (name: Ref.DottedName, _: Ast.DDataType) => MissingDataCon(name),
      )
      _ <- Try { existingDatatypes.map { case (name, dt) => checkDatatype(moduleWithMetadata, name, dt).get } }
    } yield ()
  }

  private def checkTemplate(template: Upgrading[Ast.Template]): Try[Unit] = {
    for {
      (existingChoices, _newChoices) <- checkDeleted(
        template.map(_.choices),
        (name: Ref.ChoiceName, _: Ast.TemplateChoice) => MissingChoice(name),
      )

      _ <- tryAll(existingChoices.values, checkChoice(_))
      _ <- checkKey(template.map(_.key))
    } yield ()
  }

  private def checkType(typ: Upgrading[Ast.Type]): Boolean = {
    AlphaEquiv.alphaEquiv(unifyPackageIds(typ.past), unifyPackageIds(typ.present))
  }

  // TODO: Consider whether we should strip package ids from all packages in the
  // upgrade set, not just within the pair
  private def unifyPackageIds(typ: Ast.Type): Ast.Type = {
    def stripIdentifier(id: Ref.Identifier): Ref.Identifier =
      if (id.packageId == packageId.present) {
        Ref.Identifier(packageId.past, id.qualifiedName)
      } else {
        id
      }

    typ match {
      case Ast.TNat(n) => Ast.TNat(n)
      case Ast.TSynApp(n, args) => Ast.TSynApp(stripIdentifier(n), args.map(unifyPackageIds(_)))
      case Ast.TVar(n) => Ast.TVar(n)
      case Ast.TTyCon(con) => Ast.TTyCon(stripIdentifier(con))
      case Ast.TBuiltin(bt) => Ast.TBuiltin(bt)
      case Ast.TApp(fun, arg) => Ast.TApp(unifyPackageIds(fun), unifyPackageIds(arg))
      case Ast.TForall(v, body) => Ast.TForall(v, unifyPackageIds(body))
      case Ast.TStruct(fields) => Ast.TStruct(fields.mapValues(unifyPackageIds(_)))
    }
  }

  private def checkKey(key: Upgrading[Option[Ast.TemplateKey]]): Try[Unit] = {
    key match {
      case Upgrading(None, None) => Success(());
      case Upgrading(Some(pastKey), Some(presentKey)) => {
        val key = Upgrading(pastKey.typ, presentKey.typ)
        if (!checkType(key))
          Failure(TemplateChangedKeyType(key))
        else
          Success(())
      }
      case Upgrading(Some(pastKey @ _), None) =>
        Failure(TemplateRemovedKey(pastKey))
      case Upgrading(None, Some(presentKey @ _)) =>
        Success(
          ()
        ) // TODO: Should emit a warning, but we don't currently have a framework for warnings
    }
  }

  private def checkChoice(choice: Upgrading[Ast.TemplateChoice]): Try[Unit] = {
    val returnType = choice.map(_.returnType)
    if (checkType(returnType)) {
      Success(())
    } else {
      Failure(ChoiceChangedReturnType(returnType))
    }
  }

  private def dataTypeOrigin(
      moduleWithMetadata: ModuleWithMetadata,
      name: Ref.DottedName,
  ): UpgradedRecordOrigin = {
    moduleWithMetadata.module.templates.get(name) match {
      case Some(template @ _) => TemplateBody(name)
      case None => {
        moduleWithMetadata.choiceNameMap.get(name) match {
          case Some((templateName, choiceName)) =>
            TemplateChoiceInput(templateName, choiceName)
          case None =>
            moduleWithMetadata.variantNameMap.get(name) match {
              case Some((templateName, variantName)) =>
                VariantConstructor(templateName, variantName);
              case _ => TopLevel
            }
        }
      }
    }
  }

  private def failIf(predicate: Boolean, err: => UpgradeError): Try[Unit] =
    if (predicate)
      Failure(err)
    else
      Success(())

  private def checkDatatype(
      moduleWithMetadata: Upgrading[ModuleWithMetadata],
      name: Ref.DottedName,
      datatype: Upgrading[Ast.DDataType],
  ): Try[Unit] = {
    val origin = moduleWithMetadata.map(m => dataTypeOrigin(m, name))
    if (origin.present != origin.past) {
      Failure(RecordChangedOrigin(origin))
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
              (_: Ast.VariantConName, _: Ast.Type) => VariantRemovedVariant(origin.present),
            )

            _ <- failIf(new_.nonEmpty, VariantAddedVariant(origin.present))

            changedTypes = existing.filter { case (field @ _, typ) => !checkType(typ) }
            _ <- if (changedTypes.nonEmpty) Failure(VariantChangedVariantType(origin.present)) else Success(())
          } yield ()
        case Upgrading(past: Ast.DataEnum, present: Ast.DataEnum) =>
          val upgrade = Upgrading(past, present)
          val enums: Upgrading[Map[Ast.EnumConName, Unit]] =
            upgrade.map(enums => Map.from(enums.constructors.iterator.map(enum => (enum, ()))))
          for {
            (_, new_) <- checkDeleted(
              enums,
              (_: Ast.EnumConName, _: Unit) => EnumRemovedVariant(origin.present)
            )

            _ <- if (new_.nonEmpty) Failure(EnumAddedVariant(origin.present)) else Success(())
          } yield ()
        case Upgrading(Ast.DataInterface, Ast.DataInterface) => Try(())
        case other => Failure(MismatchDataConsVariety(other))
      }
    }
  }

  private def checkFields(origin: UpgradedRecordOrigin, records: Upgrading[Ast.DataRecord]): Try[Unit] = {
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
      _ <- failIf(_deleted.nonEmpty, RecordFieldsMissing(_deleted))

      // Then we check for changed types
      changedTypes = _existing.filter { case (field @ _, typ) => !checkType(typ) }
      _ <- failIf(changedTypes.nonEmpty, RecordFieldsExistingChanged(changedTypes))

      // Then we check for new non-optional types, and vary the message if its a variant
      newNonOptionalTypes = _new_.find { case (field @ _, typ) => !fieldTypeOptional(typ) }
      _ <- origin match {
        case _: VariantConstructor =>
          failIf(_new_.nonEmpty, VariantAddedVariantField(origin))
        case _ =>
          failIf(newNonOptionalTypes.nonEmpty, RecordFieldsNewNonOptional())
      }

      // Finally, reordered field names
      changedFieldNames: ImmArray[(Ast.FieldName, Ast.FieldName)] = {
        val fieldNames: Upgrading[ImmArray[Ast.FieldName]] = records.map(_.fields.map(_._1))
        fieldNames.past.zip(fieldNames.present).filter { case (past, present) => past != present }
      }
      _ <- failIf(changedFieldNames.nonEmpty, RecordFieldsOrderChanged())
    } yield ()
  }
}
