// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

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

  private def tryAll[A, B](t: Iterable[A], f: A => Try[B]): Try[Seq[B]] =
    Try(t.map(f(_).get).toSeq)

  def typecheckUpgrades(
      present: (Ref.PackageId, Ast.Package),
      pastPackageId: Ref.PackageId,
      mbPastPkg: Option[Ast.Package],
  ): Try[Unit] = {
    mbPastPkg match {
      case None =>
        fail(UpgradeError.CouldNotResolveUpgradedPackageId(Upgrading(pastPackageId, present._1)));
      case Some(pastPkg) =>
        val tc = TypecheckUpgrades(Upgrading((pastPackageId, pastPkg), present))
        tc.check()
    }
  }
}

case class TypecheckUpgrades(packagesAndIds: Upgrading[(Ref.PackageId, Ast.Package)]) {
  import TypecheckUpgrades._

  private lazy val _package: Upgrading[Ast.Package] = packagesAndIds.map(_._2)

  private def check(): Try[Unit] = {
    for {
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
    (ifaces.toMap, other.toMap)
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
      (implName, impl) <- template.implements.toMap
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
