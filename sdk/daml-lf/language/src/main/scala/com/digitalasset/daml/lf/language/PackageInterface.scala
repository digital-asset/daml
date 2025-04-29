// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package language

import com.digitalasset.daml.lf.data.TemplateOrInterface
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._

private[digitalasset] class PackageInterface(
    val signatures: PartialFunction[PackageId, PackageSignature]
) {

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
      tycon: TypeConId,
      context: => Reference,
  ): Either[LookupError, DefinitionSignature] =
    lookupModule(tycon.packageId, tycon.qualifiedName.module, context).flatMap(
      _.definitions
        .get(tycon.qualifiedName.name)
        .toRight(LookupError.NotFound(Reference.Definition(tycon), context))
    )

  def lookupDefinition(tycon: TypeConId): Either[LookupError, DefinitionSignature] =
    lookupDefinition(tycon, Reference.Definition(tycon))

  // Throws a Definition LookupError, if name does not maps to a Definition.
  // Throws a TypeSyn LookupError, if name map to a Definition which is not a DTypeSyn.
  private[this] def lookupTypeSyn(
      tycon: TypeSynId,
      context: => Reference,
  ): Either[LookupError, DTypeSyn] =
    lookupDefinition(tycon, context).flatMap {
      case typeSyn: DTypeSyn => Right(typeSyn)
      case _ => Left(LookupError.NotFound(Reference.TypeSyn(tycon), context))
    }

  def lookupTypeSyn(tycon: TypeSynId): Either[LookupError, DTypeSyn] =
    lookupTypeSyn(tycon, Reference.TypeSyn(tycon))

  // Throws a Definition LookupError, if name does not maps to a Definition.
  // Throws a TypeSyn LookupError, if name map to a Definition which is not a DDataType.
  private[this] def lookupDataType(
      tycon: TypeConId,
      context: => Reference,
  ): Either[LookupError, DDataType] =
    lookupDefinition(tycon, context).flatMap {
      case dataType: DDataType => Right(dataType)
      case _ => Left(LookupError.NotFound(Reference.DataType(tycon), context))
    }

  def lookupDataType(tycon: TypeConId): Either[LookupError, DDataType] =
    lookupDataType(tycon, Reference.DataType(tycon))

  private[this] def lookupDataRecord(
      tyCon: TypeConId,
      context: => Reference,
  ): Either[LookupError, DataRecordInfo] =
    lookupDataType(tyCon, context).flatMap { dataType =>
      dataType.cons match {
        case record: DataRecord => Right(DataRecordInfo(dataType, record))
        case _ => Left(LookupError.NotFound(Reference.DataRecord(tyCon), context))
      }
    }

  def lookupDataRecord(tyCon: TypeConId): Either[LookupError, DataRecordInfo] =
    lookupDataRecord(tyCon, Reference.DataRecord(tyCon))

  private[this] def lookupRecordFieldInfo(
      tyCon: TypeConId,
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
      tyCon: TypeConId,
      fieldName: FieldName,
  ): Either[LookupError, RecordFieldInfo] =
    lookupRecordFieldInfo(tyCon, fieldName, Reference.DataRecordField(tyCon, fieldName))

  private[this] def lookupDataVariant(
      tyCon: TypeConId,
      context: => Reference,
  ): Either[LookupError, DataVariantInfo] =
    lookupDataType(tyCon, context).flatMap(dataType =>
      dataType.cons match {
        case cons: DataVariant => Right(DataVariantInfo(dataType, cons))
        case _ => Left(LookupError.NotFound(Reference.DataVariant(tyCon), context))
      }
    )

  def lookupDataVariant(tyCon: TypeConId): Either[LookupError, DataVariantInfo] =
    lookupDataVariant(tyCon, Reference.DataVariant(tyCon))

  private[this] def lookupVariantConstructor(
      tyCon: TypeConId,
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
      tyCon: TypeConId,
      consName: VariantConName,
  ): Either[LookupError, VariantConstructorInfo] =
    lookupVariantConstructor(tyCon, consName, Reference.DataVariantConstructor(tyCon, consName))

  private[this] def lookupDataEnum(
      tyCon: TypeConId,
      context: => Reference,
  ): Either[LookupError, DataEnumInfo] =
    lookupDataType(tyCon, context).flatMap { dataType =>
      dataType.cons match {
        case cons: DataEnum => Right(DataEnumInfo(dataType, cons))
        case _ => Left(LookupError.NotFound(Reference.DataEnum(tyCon), context))
      }
    }

  def lookupDataEnum(tyCon: TypeConId): Either[LookupError, DataEnumInfo] =
    lookupDataEnum(tyCon, Reference.DataEnum(tyCon))

  private[this] def lookupEnumConstructor(
      tyCon: TypeConId,
      consName: EnumConName,
      context: => Reference,
  ): Either[LookupError, Int] =
    lookupDataEnum(tyCon, context).flatMap { dataEnumInfo =>
      dataEnumInfo.dataEnum.constructorRank.get(consName) match {
        case Some(rank) => Right(rank)
        case None =>
          Left(LookupError.NotFound(Reference.DataEnumConstructor(tyCon, consName), context))
      }
    }

  def lookupEnumConstructor(tyCon: TypeConId, consName: EnumConName): Either[LookupError, Int] =
    lookupEnumConstructor(tyCon, consName, Reference.DataEnumConstructor(tyCon, consName))

  private[this] def lookupTemplate(
      tycon: TypeConId,
      context: => Reference,
  ): Either[LookupError, TemplateSignature] =
    lookupModule(tycon.packageId, tycon.qualifiedName.module, context).flatMap(
      _.templates
        .get(tycon.qualifiedName.name)
        .toRight(LookupError.NotFound(Reference.Template(tycon.toRef), context))
    )

  def lookupTemplate(tycon: TypeConId): Either[LookupError, TemplateSignature] =
    lookupTemplate(tycon, Reference.Template(tycon.toRef))

  private[this] def lookupInterface(
      tycon: TypeConId,
      context: => Reference,
  ): Either[LookupError, DefInterfaceSignature] =
    lookupModule(tycon.packageId, tycon.qualifiedName.module, context).flatMap(
      _.interfaces
        .get(tycon.qualifiedName.name)
        .toRight(LookupError.NotFound(Reference.Interface(tycon), context))
    )

  def lookupInterface(tycon: TypeConId): Either[LookupError, DefInterfaceSignature] =
    lookupInterface(tycon, Reference.Interface(tycon))

  /** Look up a template's choice by name.
    * This purposefully does not return choices inherited via interfaces.
    * Use lookupChoice for a more flexible lookup.
    */
  private[this] def lookupTemplateChoice(
      tmplId: TypeConId,
      chName: ChoiceName,
      context: => Reference,
  ): Either[LookupError, TemplateChoiceSignature] =
    lookupTemplate(tmplId, context).flatMap(template =>
      template.choices
        .get(chName)
        .toRight(LookupError.NotFound(Reference.TemplateChoice(tmplId, chName), context))
    )

  /** Look up a template's own choice. Does not return choices inherited via interfaces. */
  def lookupTemplateChoice(
      tmplId: TypeConId,
      chName: ChoiceName,
  ): Either[LookupError, TemplateChoiceSignature] =
    lookupTemplateChoice(tmplId, chName, Reference.TemplateChoice(tmplId, chName))

  private[lf] def lookupChoice(
      templateId: TypeConId,
      mbInterfaceId: Option[TypeConId],
      chName: ChoiceName,
  ): Either[LookupError, TemplateChoiceSignature] =
    mbInterfaceId match {
      case None => lookupTemplateChoice(templateId, chName)
      case Some(ifaceId) => lookupInterfaceChoice(ifaceId, chName)
    }

  private[this] def lookupInterfaceInstance(
      interfaceId: TypeConId,
      templateId: TypeConId,
      context: => Reference,
  ): Either[
    LookupError,
    TemplateImplementsSignature,
  ] = for {
    template <- lookupTemplate(templateId, context)
    inst <- template.implements
      .get(interfaceId)
      .toRight(
        LookupError.NotFound(Reference.InterfaceInstance(interfaceId, templateId), context)
      )
  } yield inst

  def lookupInterfaceInstance(
      interfaceId: TypeConId,
      templateId: TypeConId,
  ): Either[
    LookupError,
    TemplateImplementsSignature,
  ] =
    lookupInterfaceInstance(
      interfaceId,
      templateId,
      Reference.InterfaceInstance(interfaceId, templateId),
    )

  private[this] def lookupInterfaceChoice(
      ifaceId: TypeConId,
      chName: ChoiceName,
      context: => Reference,
  ): Either[LookupError, TemplateChoiceSignature] =
    lookupInterface(ifaceId, context).flatMap(
      _.choices
        .get(chName)
        .toRight(LookupError.NotFound(Reference.InterfaceChoice(ifaceId, chName), context))
    )

  def lookupInterfaceChoice(
      ifaceId: TypeConId,
      chName: ChoiceName,
  ): Either[LookupError, TemplateChoiceSignature] =
    lookupInterfaceChoice(ifaceId, chName, Reference.InterfaceChoice(ifaceId, chName))

  private[lf] def lookupTemplateOrInterface(
      identier: TypeConId,
      context: => Reference,
  ): Either[LookupError, TemplateOrInterface[TemplateSignature, DefInterfaceSignature]] =
    lookupModule(identier.packageId, identier.qualifiedName.module, context).flatMap(mod =>
      mod.templates.get(identier.qualifiedName.name) match {
        case Some(template) => Right(TemplateOrInterface.Template(template))
        case None =>
          mod.interfaces.get(identier.qualifiedName.name) match {
            case Some(interface) => Right(TemplateOrInterface.Interface(interface))
            case None =>
              Left(LookupError.NotFound(Reference.TemplateOrInterface(identier.toRef), context))
          }
      }
    )

  def lookupTemplateOrInterface(
      tycon: TypeConId
  ): Either[LookupError, TemplateOrInterface[TemplateSignature, DefInterfaceSignature]] =
    lookupTemplateOrInterface(tycon, Reference.TemplateOrInterface(tycon.toRef))

  private[this] def lookupInterfaceMethod(
      ifaceId: TypeConId,
      methodName: MethodName,
      context: => Reference,
  ): Either[LookupError, InterfaceMethod] =
    lookupInterface(ifaceId, context).flatMap(
      _.methods
        .get(methodName)
        .toRight(LookupError.NotFound(Reference.Method(ifaceId, methodName), context))
    )

  def lookupInterfaceMethod(
      ifaceId: TypeConId,
      methodName: MethodName,
  ): Either[LookupError, InterfaceMethod] =
    lookupInterfaceMethod(ifaceId, methodName, Reference.Method(ifaceId, methodName))

  private[this] def lookupTemplateKey(
      tycon: TypeConId,
      context: => Reference,
  ): Either[LookupError, TemplateKeySignature] =
    lookupTemplate(tycon, context).flatMap(
      _.key.toRight(LookupError.NotFound(Reference.TemplateKey(tycon), context))
    )

  def lookupTemplateKey(tycon: TypeConId): Either[LookupError, TemplateKeySignature] =
    lookupTemplateKey(tycon, Reference.TemplateKey(tycon))

  private[this] def lookupValue(
      name: ValueRef,
      context: => Reference,
  ): Either[LookupError, DValueSignature] =
    lookupDefinition(name, context).flatMap {
      case valueDef: DValueSignature => Right(valueDef)
      case _ => Left(LookupError.NotFound(Reference.Value(name), context))
    }

  def lookupValue(tycon: ValueRef): Either[LookupError, DValueSignature] =
    lookupValue(tycon, Reference.Value(tycon))

  private[this] def lookupException(
      tycon: TypeConId,
      context: => Reference,
  ): Either[LookupError, DefExceptionSignature] =
    lookupModule(tycon.packageId, tycon.qualifiedName.module, context).flatMap(
      _.exceptions
        .get(tycon.qualifiedName.name)
        .toRight(LookupError.NotFound(Reference.Exception(tycon), context))
    )

  def lookupException(tycon: TypeConId): Either[LookupError, DefExceptionSignature] =
    lookupException(tycon, Reference.Exception(tycon))

  def lookupPackageLanguageVersion(pkgId: PackageId): Either[LookupError, LanguageVersion] =
    lookupPackage(pkgId).map(_.languageVersion)

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

    final case class Inherited(ifaceId: TypeConId, choice: TemplateChoiceSignature)
        extends ChoiceInfo

  }
}
