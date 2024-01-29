// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package validation

import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import scala.util.{Try, Success, Failure}
import com.daml.lf.validation.AlphaEquiv

case class Upgrading[A](past: A, present: A) {
  def map[B](f: A => B): Upgrading[B] = Upgrading(f(past), f(present))
  def fold[B](f: (A, A) => B): B = f(past, present)
  def zip[B, C](that: Upgrading[B], f: (A, B) => C) =
    Upgrading(f(this.past, that.past), f(this.present, that.present))
}

sealed abstract class UpgradeError extends Throwable {
  def message(): String;
}

final case class CouldNotResolveUpgradedPackageId(upgrading: Upgrading[Ref.PackageId])
    extends UpgradeError {
  override def message(): String = "CouldNotResolveUpgradedPackageId"
}

final case class UnknownUpgradeError(msg: String) extends UpgradeError {
  override def message(): String = this.msg
}

sealed abstract class UpgradedRecordOrigin

final case class TemplateBody(template: Ref.DottedName) extends UpgradedRecordOrigin
final case class TemplateChoiceInput(template: Ref.DottedName, choice: Ref.ChoiceName)
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
      handler: V => T,
  ): Try[(Map[K, Upgrading[V]], Map[K, V])] = {
    val (deletedV, existingV, newV) = extractDelExistNew(arg)
    deletedV.headOption match {
      case Some((k @ _, v)) => Failure(handler(v))
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
          (m: Ast.Module) => UnknownUpgradeError(s"MissingModule(${m.name})"),
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

    val moduleWithChoiceNameMap = module.map(module => (module, getChoiceNameMap(module)))
    for {
      (existingTemplates, _new) <- checkDeleted(
        module.map(_.templates),
        (_: Ast.Template) => UnknownUpgradeError(s"MissingTemplate(t)"),
      )
      _ <- tryAll(existingTemplates.values, checkTemplate(_))

      (existingDatatypes, _new) <- checkDeleted(
        module.map(datatypes(_)),
        (_: Ast.DDataType) => UnknownUpgradeError(s"MissingDataCon(t)"),
      )
      _ <- Try { existingDatatypes.map { case (name, dt) => checkDatatype(moduleWithChoiceNameMap, name, dt).get } }
    } yield ()
  }

  private def checkTemplate(template: Upgrading[Ast.Template]): Try[Unit] = {
    for {
      (existingChoices, _newChoices) <- checkDeleted(
        template.map(_.choices),
        (_: Ast.TemplateChoice) => UnknownUpgradeError(s"MissingChoice(t)"),
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
        if (!checkType(Upgrading(pastKey.typ, presentKey.typ)))
          Failure(UnknownUpgradeError(s"TemplateChangedKeyType"))
        else
          Success(())
      }
      case Upgrading(Some(pastKey @ _), None) =>
        Failure(UnknownUpgradeError(s"TemplateRemovedKey"))
      case Upgrading(None, Some(presentKey @ _)) =>
        Success(
          ()
        ) // TODO: Should emit a warning, but we don't currently have a framework for warnings
    }
  }

  private def checkChoice(choice: Upgrading[Ast.TemplateChoice]): Try[Unit] = {
    if (checkType(choice.map(_.returnType))) {
      Success(())
    } else {
      Failure(UnknownUpgradeError("ChoiceChangedReturnType"))
    }
  }

  type ChoiceNameMap = Map[Ref.DottedName, (Ref.DottedName, Ref.ChoiceName)]

  private def getChoiceNameMap(module: Ast.Module): ChoiceNameMap =
    for {
      (templateName, template) <- module.templates
      (choiceName, choice) <- template.choices
    } yield {
      val prefix = templateName.segments.init
      val fullName = prefix.slowSnoc(choiceName)
      (Ref.DottedName.unsafeFromNames(fullName), (templateName, choiceName))
    }

  private def dataTypeOrigin(
      moduleWithChoiceNameMap: (Ast.Module, ChoiceNameMap),
      name: Ref.DottedName,
  ): UpgradedRecordOrigin = {
    moduleWithChoiceNameMap._1.templates.get(name) match {
      case Some(template @ _) => TemplateBody(name);
      case None => {
        moduleWithChoiceNameMap._2.get(name) match {
          case Some((templateName, choiceName)) =>
            TemplateChoiceInput(templateName, choiceName);
          case _ => TopLevel;
        }
      }
    }
  }

  private def checkDatatype(
      moduleWithChoiceNameMap: Upgrading[(Ast.Module, ChoiceNameMap)],
      name: Ref.DottedName,
      datatype: Upgrading[Ast.DDataType],
  ): Try[Unit] = {
    val origin = moduleWithChoiceNameMap.map(dataTypeOrigin(_, name))
    if (origin.present != origin.past) {
      Failure(UnknownUpgradeError("RecordChangedOrigin"))
    } else {
      datatype.map(_.cons) match {
        case Upgrading(past: Ast.DataRecord, present: Ast.DataRecord) =>
          checkFields(Upgrading(past, present))
        case Upgrading(_: Ast.DataVariant, _: Ast.DataVariant) => Try(())
        case Upgrading(_: Ast.DataEnum, _: Ast.DataEnum) => Try(())
        case Upgrading(Ast.DataInterface, Ast.DataInterface) => Try(())
        case _ => Failure(UnknownUpgradeError(s"MismatchDataConsVariety"))
      }
    }
  }

  private def checkFields(records: Upgrading[Ast.DataRecord]): Try[Unit] = {
    val fields: Upgrading[Map[Ast.FieldName, Ast.Type]] =
      records.map(rec => Map.from(rec.fields.iterator))
    def fieldTypeOptional(typ: Ast.Type): Boolean =
      typ match {
        case Ast.TApp(Ast.TBuiltin(Ast.BTOptional), _) => true
        case _ => false
      }

    val (_deleted, _existing, _new_) = extractDelExistNew(fields)
    if (_deleted.nonEmpty) {
      Failure(UnknownUpgradeError("RecordFieldsMissing"))
    } else if (!_existing.forall({ case (field @ _, typ) => checkType(typ) })) {
      Failure(UnknownUpgradeError("RecordFieldsExistingChanged"))
    } else if (_new_.find({ case (field @ _, typ) => !fieldTypeOptional(typ) }).nonEmpty) {
      Failure(UnknownUpgradeError("RecordFieldsNewNonOptional"))
    } else {
      Success(())
    }
  }
}
