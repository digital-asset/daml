// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package language

import com.daml.lf.data.TemplateOrInterface
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._

private[lf] class PackageInterface(signatures: PartialFunction[PackageId, PackageSignature]) {

  import PackageInterface._

  private[this] def lookupPackage(
      pkgId: PackageId,
      context: => Reference,
  ): Either[LookupError, PackageSignature] =
    signatures.lift(pkgId).toRight(LookupError.NotFound(Reference.Package(pkgId), context))

  def lookupPackage(pkgId: PackageId): Either[LookupError, PackageSignature] =
    lookupPackage(pkgId, Reference.Package(pkgId))

  private[this] def lookupModule(
      pkgId: PackageId,
      modName: ModuleName,
      context: => Reference,
  ): Either[LookupError, ModuleSignature] =
    lookupPackage(pkgId, context).flatMap(
      _.modules
        .get(modName)
        .toRight(LookupError.NotFound(Reference.Module(pkgId, modName), context))
    )

  def lookupModule(pkgId: PackageId, modName: ModuleName): Either[LookupError, ModuleSignature] =
    lookupModule(pkgId, modName, Reference.Module(pkgId, modName))

  private[this] def lookupDefinition(
      name: TypeConName,
      context: => Reference,
  ): Either[LookupError, DefinitionSignature] =
    lookupModule(name.packageId, name.qualifiedName.module, context).flatMap(
      _.definitions
        .get(name.qualifiedName.name)
        .toRight(LookupError.NotFound(Reference.Definition(name), context))
    )

  def lookupDefinition(name: TypeConName): Either[LookupError, DefinitionSignature] =
    lookupDefinition(name, Reference.Definition(name))

  // Throws a Definition LookupError, if name does not maps to a Definition.
  // Throws a TypeSyn LookupError, if name map to a Definition which is not a DTypeSyn.
  private[this] def lookupTypeSyn(
      name: TypeSynName,
      context: => Reference,
  ): Either[LookupError, DTypeSyn] =
    lookupDefinition(name, context).flatMap {
      case typeSyn: DTypeSyn => Right(typeSyn)
      case _ => Left(LookupError.NotFound(Reference.TypeSyn(name), context))
    }

  def lookupTypeSyn(name: TypeSynName): Either[LookupError, DTypeSyn] =
    lookupTypeSyn(name, Reference.TypeSyn(name))

  // Throws a Definition LookupError, if name does not maps to a Definition.
  // Throws a TypeSyn LookupError, if name map to a Definition which is not a DDataType.
  private[this] def lookupDataType(
      name: TypeConName,
      context: => Reference,
  ): Either[LookupError, DDataType] =
    lookupDefinition(name, context).flatMap {
      case dataType: DDataType => Right(dataType)
      case _ => Left(LookupError.NotFound(Reference.DataType(name), context))
    }

  def lookupDataType(name: TypeConName): Either[LookupError, DDataType] =
    lookupDataType(name, Reference.DataType(name))

  private[this] def lookupDataRecord(
      tyCon: TypeConName,
      context: => Reference,
  ): Either[LookupError, DataRecordInfo] =
    lookupDataType(tyCon, context).flatMap { dataType =>
      dataType.cons match {
        case record: DataRecord => Right(DataRecordInfo(dataType, record))
        case _ => Left(LookupError.NotFound(Reference.DataRecord(tyCon), context))
      }
    }

  def lookupDataRecord(tyCon: TypeConName): Either[LookupError, DataRecordInfo] =
    lookupDataRecord(tyCon, Reference.DataRecord(tyCon))

  private[this] def lookupRecordFieldInfo(
      tyCon: TypeConName,
      fieldName: FieldName,
      context: => Reference,
  ): Either[LookupError, RecordFieldInfo] =
    lookupDataRecord(tyCon, context).flatMap { recordDataInfo =>
      recordDataInfo.dataRecord.fieldInfo.get(fieldName) match {
        case Some((typ, index)) => Right(RecordFieldInfo(recordDataInfo, typ, index))
        case None =>
          Left(LookupError.NotFound(Reference.DataRecordField(tyCon, fieldName), context))
      }
    }

  def lookupRecordFieldInfo(
      tyCon: TypeConName,
      fieldName: FieldName,
  ): Either[LookupError, RecordFieldInfo] =
    lookupRecordFieldInfo(tyCon, fieldName, Reference.DataRecordField(tyCon, fieldName))

  private[this] def lookupDataVariant(
      tyCon: TypeConName,
      context: => Reference,
  ): Either[LookupError, DataVariantInfo] =
    lookupDataType(tyCon, context).flatMap(dataType =>
      dataType.cons match {
        case cons: DataVariant => Right(DataVariantInfo(dataType, cons))
        case _ => Left(LookupError.NotFound(Reference.DataVariant(tyCon), context))
      }
    )

  def lookupDataVariant(tyCon: TypeConName): Either[LookupError, DataVariantInfo] =
    lookupDataVariant(tyCon, Reference.DataVariant(tyCon))

  private[this] def lookupVariantConstructor(
      tyCon: TypeConName,
      consName: VariantConName,
      context: => Reference,
  ): Either[LookupError, VariantConstructorInfo] =
    lookupDataVariant(tyCon, context).flatMap(variantInfo =>
      variantInfo.dataVariant.constructorInfo.get(consName) match {
        case Some((typ, rank)) => Right(VariantConstructorInfo(variantInfo, typ, rank))
        case None =>
          Left(LookupError.NotFound(Reference.DataVariantConstructor(tyCon, consName), context))
      }
    )

  def lookupVariantConstructor(
      tyCon: TypeConName,
      consName: VariantConName,
  ): Either[LookupError, VariantConstructorInfo] =
    lookupVariantConstructor(tyCon, consName, Reference.DataVariantConstructor(tyCon, consName))

  private[this] def lookupDataEnum(
      tyCon: TypeConName,
      context: => Reference,
  ): Either[LookupError, DataEnumInfo] =
    lookupDataType(tyCon, context).flatMap { dataType =>
      dataType.cons match {
        case cons: DataEnum => Right(DataEnumInfo(dataType, cons))
        case _ => Left(LookupError.NotFound(Reference.DataEnum(tyCon), context))
      }
    }

  def lookupDataEnum(tyCon: TypeConName): Either[LookupError, DataEnumInfo] =
    lookupDataEnum(tyCon, Reference.DataEnum(tyCon))

  private[this] def lookupEnumConstructor(
      tyCon: TypeConName,
      consName: EnumConName,
      context: => Reference,
  ): Either[LookupError, Int] =
    lookupDataEnum(tyCon, context).flatMap { dataEnumInfo =>
      dataEnumInfo.dataEnum.constructorRank.get(consName) match {
        case Some(rank) => Right(rank)
        case None =>
          Left(LookupError.NotFound(Reference.DataVariantConstructor(tyCon, consName), context))
      }
    }

  def lookupEnumConstructor(tyCon: TypeConName, consName: EnumConName): Either[LookupError, Int] =
    lookupEnumConstructor(tyCon, consName, Reference.DataEnumConstructor(tyCon, consName))

  private[this] def lookupTemplate(
      name: TypeConName,
      context: => Reference,
  ): Either[LookupError, TemplateSignature] =
    lookupModule(name.packageId, name.qualifiedName.module, context).flatMap(
      _.templates
        .get(name.qualifiedName.name)
        .toRight(LookupError.NotFound(Reference.Template(name), context))
    )

  def lookupTemplate(name: TypeConName): Either[LookupError, TemplateSignature] =
    lookupTemplate(name, Reference.Template(name))

  private[this] def lookupInterface(
      name: TypeConName,
      context: => Reference,
  ): Either[LookupError, DefInterfaceSignature] =
    lookupModule(name.packageId, name.qualifiedName.module, context).flatMap(
      _.interfaces
        .get(name.qualifiedName.name)
        .toRight(LookupError.NotFound(Reference.Interface(name), context))
    )

  def lookupInterface(name: TypeConName): Either[LookupError, DefInterfaceSignature] =
    lookupInterface(name, Reference.Interface(name))

  /** Look up a template's choice by name.
    * This purposefully does not return choices inherited via interfaces.
    * Use lookupChoice for a more flexible lookup.
    */
  private[this] def lookupTemplateChoice(
      tmpName: TypeConName,
      chName: ChoiceName,
      context: => Reference,
  ): Either[LookupError, TemplateChoiceSignature] =
    lookupTemplate(tmpName, context).flatMap(template =>
      template.choices
        .get(chName)
        .toRight(LookupError.NotFound(Reference.TemplateChoice(tmpName, chName), context))
    )

  /** Look up a template's own choice. Does not return choices inherited via interfaces. */
  def lookupTemplateChoice(
      tmpName: TypeConName,
      chName: ChoiceName,
  ): Either[LookupError, TemplateChoiceSignature] =
    lookupTemplateChoice(tmpName, chName, Reference.TemplateChoice(tmpName, chName))

  private[lf] def lookupChoice(
      templateId: TypeConName,
      mbInterfaceId: Option[TypeConName],
      chName: ChoiceName,
  ): Either[LookupError, TemplateChoiceSignature] =
    mbInterfaceId match {
      case None => lookupTemplateChoice(templateId, chName)
      case Some(ifaceId) => lookupInterfaceChoice(ifaceId, chName)
    }

  private[this] def lookupInterfaceInstance(
      interfaceName: TypeConName,
      templateName: TypeConName,
      context: => Reference,
  ): Either[
    LookupError,
    PackageInterface.InterfaceInstanceInfo,
  ] = {
    val ref = Reference.InterfaceInstance(interfaceName, templateName)
    for {
      interface <- lookupInterface(interfaceName, context)
      template <- lookupTemplate(templateName, context)
      onInterface = interface.coImplements.get(templateName)
      onTemplate = template.implements.get(interfaceName)
      ok = { tOrI: TemplateOrInterface[Unit, Unit] =>
        PackageInterface.InterfaceInstanceInfo(tOrI, interfaceName, templateName, interface)
      }
      r <- (onInterface, onTemplate) match {
        case (Some(_), None) => Right(ok(TemplateOrInterface.Interface(())))
        case (None, Some(_)) => Right(ok(TemplateOrInterface.Template(())))
        case (None, None) => Left(LookupError.NotFound(ref, context))
        case (Some(_), Some(_)) => Left(LookupError.AmbiguousInterfaceInstance(ref, context))
      }
    } yield r
  }

  def lookupInterfaceInstance(
      interfaceName: TypeConName,
      templateName: TypeConName,
  ): Either[
    LookupError,
    PackageInterface.InterfaceInstanceInfo,
  ] =
    lookupInterfaceInstance(
      interfaceName,
      templateName,
      Reference.InterfaceInstance(interfaceName, templateName),
    )

  private[this] def lookupInterfaceChoice(
      ifaceName: TypeConName,
      chName: ChoiceName,
      context: => Reference,
  ): Either[LookupError, TemplateChoiceSignature] =
    lookupInterface(ifaceName, context).flatMap(
      _.choices
        .get(chName)
        .toRight(LookupError.NotFound(Reference.TemplateChoice(ifaceName, chName), context))
    )

  def lookupInterfaceChoice(
      ifaceName: TypeConName,
      chName: ChoiceName,
  ): Either[LookupError, TemplateChoiceSignature] =
    lookupInterfaceChoice(ifaceName, chName, Reference.InterfaceChoice(ifaceName, chName))

  private[lf] def lookupTemplateOrInterface(
      identier: TypeConName,
      context: => Reference,
  ): Either[LookupError, TemplateOrInterface[TemplateSignature, DefInterfaceSignature]] =
    lookupModule(identier.packageId, identier.qualifiedName.module, context).flatMap(mod =>
      mod.templates.get(identier.qualifiedName.name) match {
        case Some(template) => Right(TemplateOrInterface.Template(template))
        case None =>
          mod.interfaces.get(identier.qualifiedName.name) match {
            case Some(interface) => Right(TemplateOrInterface.Interface(interface))
            case None =>
              Left(LookupError.NotFound(Reference.TemplateOrInterface(identier), context))
          }
      }
    )

  def lookupTemplateOrInterface(
      name: TypeConName
  ): Either[LookupError, TemplateOrInterface[TemplateSignature, DefInterfaceSignature]] =
    lookupTemplateOrInterface(name, Reference.TemplateOrInterface(name))

  private[this] def lookupInterfaceMethod(
      ifaceName: TypeConName,
      methodName: MethodName,
      context: => Reference,
  ): Either[LookupError, InterfaceMethod] =
    lookupInterface(ifaceName, context).flatMap(
      _.methods
        .get(methodName)
        .toRight(LookupError.NotFound(Reference.Method(ifaceName, methodName), context))
    )

  def lookupInterfaceMethod(
      ifaceName: TypeConName,
      methodName: MethodName,
  ): Either[LookupError, InterfaceMethod] =
    lookupInterfaceMethod(ifaceName, methodName, Reference.Method(ifaceName, methodName))

  private[this] def lookupTemplateKey(
      name: TypeConName,
      context: => Reference,
  ): Either[LookupError, TemplateKeySignature] =
    lookupTemplate(name, context).flatMap(
      _.key.toRight(LookupError.NotFound(Reference.TemplateKey(name), context))
    )

  def lookupTemplateKey(name: TypeConName): Either[LookupError, TemplateKeySignature] =
    lookupTemplateKey(name, Reference.TemplateKey(name))

  private[this] def lookupValue(
      name: ValueRef,
      context: => Reference,
  ): Either[LookupError, DValueSignature] =
    lookupDefinition(name, context).flatMap {
      case valueDef: DValueSignature => Right(valueDef)
      case _ => Left(LookupError.NotFound(Reference.Value(name), context))
    }

  def lookupValue(name: ValueRef): Either[LookupError, DValueSignature] =
    lookupValue(name, Reference.Value(name))

  private[this] def lookupException(
      name: TypeConName,
      context: => Reference,
  ): Either[LookupError, DefExceptionSignature] =
    lookupModule(name.packageId, name.qualifiedName.module, context).flatMap(
      _.exceptions
        .get(name.qualifiedName.name)
        .toRight(LookupError.NotFound(Reference.Exception(name), context))
    )

  def lookupException(name: TypeConName): Either[LookupError, DefExceptionSignature] =
    lookupException(name, Reference.Exception(name))

  val packageLanguageVersion: PartialFunction[PackageId, LanguageVersion] =
    signatures andThen (_.languageVersion)

}

object PackageInterface {

  val Empty = new PackageInterface(PartialFunction.empty)

  def apply(packages: Map[PackageId, Package]): PackageInterface =
    new PackageInterface(Util.toSignatures(packages))

  case class DataRecordInfo(
      dataType: DDataType,
      dataRecord: DataRecord,
  ) {
    def subst(argTypes: Seq[Type]): Map[TypeVarName, Type] =
      (dataType.params.toSeq.view.map(_._1) zip argTypes).toMap
  }

  case class RecordFieldInfo(
      dataRecordInfo: DataRecordInfo,
      typDef: Ast.Type,
      index: Int,
  ) {
    def concreteType(argTypes: Seq[Type]): Type =
      Util.substitute(typDef, dataRecordInfo.subst(argTypes))
  }

  case class DataVariantInfo(
      dataType: DDataType,
      dataVariant: DataVariant,
  ) {
    def subst(argTypes: Seq[Type]): Map[TypeVarName, Type] =
      (dataType.params.toSeq.view.map(_._1) zip argTypes).toMap
  }

  case class VariantConstructorInfo(
      dataVariantInfo: DataVariantInfo,
      typDef: Type,
      rank: Int,
  ) {
    def concreteType(argTypes: Seq[Type]): Type =
      Util.substitute(typDef, dataVariantInfo.subst(argTypes))
  }

  case class DataEnumInfo(
      dataType: DDataType,
      dataEnum: DataEnum,
  )

  // ChoiceInfo defined the output of lookupChoice(iden, chName)
  // There is 3 cases:
  // - iden refers to an interface that defines a choice chName
  // - iden refers to a template that defines a choice chName
  // - iden refers to a template that inherits from a interface than defined chName
  sealed trait ChoiceInfo extends Serializable with Product {
    val choice: TemplateChoiceSignature
  }

  object ChoiceInfo {

    final case class Template(choice: TemplateChoiceSignature) extends ChoiceInfo

    final case class Inherited(ifaceId: TypeConName, choice: TemplateChoiceSignature)
        extends ChoiceInfo

  }

  final case class InterfaceInstanceInfo(
      val parentTemplateOrInterface: TemplateOrInterface[Unit, Unit],
      val interfaceId: TypeConName,
      val templateId: TypeConName,
      val interfaceSignature: DefInterfaceSignature,
  ) {
    val parent: TemplateOrInterface[TypeConName, TypeConName] =
      parentTemplateOrInterface match {
        case TemplateOrInterface.Template(_) => TemplateOrInterface.Template(templateId)
        case TemplateOrInterface.Interface(_) => TemplateOrInterface.Interface(interfaceId)
      }

    val ref: Reference =
      Reference.ConcreteInterfaceInstance(
        parentTemplateOrInterface,
        Reference.InterfaceInstance(interfaceId, templateId),
      )
  }
}
