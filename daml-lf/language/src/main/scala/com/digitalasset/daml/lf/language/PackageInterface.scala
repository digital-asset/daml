// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package language

import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._

private[lf] class PackageInterface(signatures: PartialFunction[PackageId, PackageSignature]) {

  import PackageInterface._

  private[this] def lookupPackage(
      pkgId: PackageId,
      context: => Reference,
  ): Either[LookupError, PackageSignature] =
    signatures.lift(pkgId).toRight(LookupError(Reference.Package(pkgId), context))

  def lookupPackage(pkgId: PackageId): Either[LookupError, PackageSignature] =
    lookupPackage(pkgId, Reference.Package(pkgId))

  private[this] def lookupModule(
      pkgId: PackageId,
      modName: ModuleName,
      context: => Reference,
  ): Either[LookupError, ModuleSignature] =
    lookupPackage(pkgId, context).flatMap(
      _.modules.get(modName).toRight(LookupError(Reference.Module(pkgId, modName), context))
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
        .toRight(LookupError(Reference.Definition(name), context))
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
      case _ => Left(LookupError(Reference.TypeSyn(name), context))
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
      case _ => Left(LookupError(Reference.DataType(name), context))
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
        case _ => Left(LookupError(Reference.DataRecord(tyCon), context))
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
        case None => Left(LookupError(Reference.DataRecordField(tyCon, fieldName), context))
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
        case _ => Left(LookupError(Reference.DataVariant(tyCon), context))
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
        case None => Left(LookupError(Reference.DataVariantConstructor(tyCon, consName), context))
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
        case _ => Left(LookupError(Reference.DataEnum(tyCon), context))
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
        case None => Left(LookupError(Reference.DataVariantConstructor(tyCon, consName), context))
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
        .toRight(LookupError(Reference.Template(name), context))
    )

  def lookupTemplate(name: TypeConName): Either[LookupError, TemplateSignature] =
    lookupTemplate(name, Reference.Template(name))

  private[this] def lookupInterface(
      name: TypeConName,
      context: => Reference,
  ): Either[LookupError, DefInterface] =
    lookupModule(name.packageId, name.qualifiedName.module, context).flatMap(
      _.interfaces
        .get(name.qualifiedName.name)
        .toRight(LookupError(Reference.Interface(name), context))
    )

  def lookupInterface(name: TypeConName): Either[LookupError, DefInterface] =
    lookupInterface(name, Reference.Interface(name))

  private[this] def lookupChoice(
      tmpName: TypeConName,
      chName: ChoiceName,
      context: => Reference,
  ): Either[LookupError, TemplateChoiceSignature] =
    lookupTemplate(tmpName, context).flatMap(
      _.choices.get(chName).toRight(LookupError(Reference.Choice(tmpName, chName), context))
    )

  def lookupChoice(
      tmpName: TypeConName,
      chName: ChoiceName,
  ): Either[LookupError, TemplateChoiceSignature] =
    lookupChoice(tmpName, chName, Reference.Choice(tmpName, chName))

  private[this] def lookupInterfaceChoice(
      tmpName: TypeConName,
      chName: ChoiceName,
      context: => Reference,
  ): Either[LookupError, InterfaceChoice] =
    lookupInterface(tmpName, context).flatMap(
      _.choices.get(chName).toRight(LookupError(Reference.Choice(tmpName, chName), context))
    )

  def lookupInterfaceChoice(
      tmpName: TypeConName,
      chName: ChoiceName,
  ): Either[LookupError, InterfaceChoice] =
    lookupInterfaceChoice(tmpName, chName, Reference.Choice(tmpName, chName))

  private[this] def lookupTemplateKey(
      name: TypeConName,
      context: => Reference,
  ): Either[LookupError, TemplateKeySignature] =
    lookupTemplate(name, context).flatMap(
      _.key.toRight(LookupError(Reference.TemplateKey(name), context))
    )

  def lookupTemplateKey(name: TypeConName): Either[LookupError, TemplateKeySignature] =
    lookupTemplateKey(name, Reference.TemplateKey(name))

  private[this] def lookupValue(
      name: ValueRef,
      context: => Reference,
  ): Either[LookupError, DValueSignature] =
    lookupDefinition(name, context).flatMap {
      case valueDef: DValueSignature => Right(valueDef)
      case _ => Left(LookupError(Reference.Value(name), context))
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
        .toRight(LookupError(Reference.Exception(name), context))
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

}
