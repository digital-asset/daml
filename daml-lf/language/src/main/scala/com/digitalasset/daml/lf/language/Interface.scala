// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package language

import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._

private[lf] class Interface(signatures: PartialFunction[PackageId, PackageSignature]) {

  import Interface._

  def lookupPackage(pkgId: PackageId): Either[LookupError, PackageSignature] =
    signatures.lift(pkgId).toRight(LookupError.Package(pkgId))

  def lookupModule(
      pkgId: PackageId,
      modName: ModuleName,
  ): Either[LookupError, ModuleSignature] =
    lookupPackage(pkgId).flatMap(_.modules.get(modName).toRight(LookupError.Module(pkgId, modName)))

  def lookupDefinition(name: TypeConName): Either[LookupError, DefinitionSignature] =
    lookupModule(name.packageId, name.qualifiedName.module).flatMap(
      _.definitions.get(name.qualifiedName.name).toRight(LookupError.Definition(name))
    )

  // Throws a Definition LookupError, if name does not maps to a Definition.
  // Throws a TypeSyn LookupError, if name map to a Definition which is not a DTypeSyn.
  def lookupTypeSyn(name: TypeSynName): Either[LookupError, DTypeSyn] =
    lookupDefinition(name).flatMap {
      case typeSyn: DTypeSyn => Right(typeSyn)
      case _ => Left(LookupError.TypeSyn(name))
    }

  // Throws a Definition LookupError, if name does not maps to a Definition.
  // Throws a TypeSyn LookupError, if name map to a Definition which is not a DDataType.
  def lookupDataType(name: TypeConName): Either[LookupError, DDataType] =
    lookupDefinition(name).flatMap {
      case dataType: DDataType => Right(dataType)
      case _ => Left(LookupError.DataType(name))
    }

  def lookupDataRecord(
      tyCon: TypeConName
  ): Either[LookupError, DataRecordInfo] =
    lookupDataType(tyCon).flatMap { dataType =>
      dataType.cons match {
        case record: DataRecord => Right(DataRecordInfo(dataType, record))
        case _ => Left(LookupError.DataRecord(tyCon))
      }
    }

  def lookupRecordFieldInfo(
      tyCon: TypeConName,
      fieldName: FieldName,
  ): Either[LookupError, RecordFieldInfo] =
    lookupDataRecord(tyCon).flatMap { recordDataInfo =>
      recordDataInfo.dataRecord.fieldInfo.get(fieldName) match {
        case Some((typ, index)) => Right(RecordFieldInfo(recordDataInfo, typ, index))
        case None => Left(LookupError.DataRecordField(tyCon, fieldName))
      }
    }

  def lookupDataVariant(
      tyCon: TypeConName
  ): Either[LookupError, DataVariantInfo] =
    lookupDataType(tyCon).flatMap(dataType =>
      dataType.cons match {
        case cons: DataVariant => Right(DataVariantInfo(dataType, cons))
        case _ => Left(LookupError.DataVariant(tyCon))
      }
    )

  def lookupVariantConstructor(
      tyCon: TypeConName,
      consName: VariantConName,
  ): Either[LookupError, VariantConstructorInfo] =
    lookupDataVariant(tyCon).flatMap(variantInfo =>
      variantInfo.dataVariant.constructorInfo.get(consName) match {
        case Some((typ, rank)) => Right(VariantConstructorInfo(variantInfo, typ, rank))
        case None => Left(LookupError.DataVariantConstructor(tyCon, consName))
      }
    )

  def lookupDataEnum(
      tyCon: TypeConName
  ): Either[LookupError, DataEnumInfo] =
    lookupDataType(tyCon).flatMap { dataType =>
      dataType.cons match {
        case cons: DataEnum => Right(DataEnumInfo(dataType, cons))
        case _ => Left(LookupError.DataEnum(tyCon))
      }
    }

  def lookupEnumConstructor(tyCon: TypeConName, consName: EnumConName): Either[LookupError, Int] =
    lookupDataEnum(tyCon).flatMap { dataEnumInfo =>
      dataEnumInfo.dataEnum.constructorRank.get(consName) match {
        case Some(rank) => Right(rank)
        case None => Left(LookupError.DataVariantConstructor(tyCon, consName))
      }
    }

  def lookupTemplate(name: TypeConName): Either[LookupError, TemplateSignature] =
    lookupModule(name.packageId, name.qualifiedName.module).flatMap(
      _.templates.get(name.qualifiedName.name).toRight(LookupError.Template(name))
    )

  def lookupChoice(
      tmpName: TypeConName,
      chName: ChoiceName,
  ): Either[LookupError, TemplateChoiceSignature] =
    lookupTemplate(tmpName).flatMap(
      _.choices.get(chName).toRight(LookupError.Choice(tmpName, chName))
    )

  def lookupTemplateKey(name: TypeConName): Either[LookupError, TemplateKeySignature] =
    lookupTemplate(name).flatMap(_.key.toRight(LookupError.TemplateKey(name)))

  def lookupValue(name: ValueRef): Either[LookupError, DValueSignature] =
    lookupDefinition(name).flatMap {
      case valueDef: DValueSignature => Right(valueDef)
      case _ => Left(LookupError.Value(name))
    }

  def lookupException(name: TypeConName): Either[LookupError, DefExceptionSignature] =
    lookupModule(name.packageId, name.qualifiedName.module).flatMap(
      _.exceptions.get(name.qualifiedName.name).toRight(LookupError.Exception(name))
    )

  val packageLanguageVersion: PartialFunction[PackageId, LanguageVersion] =
    signatures andThen (_.languageVersion)

}

object Interface {

  val Empty = new Interface(PartialFunction.empty)

  def apply(packages: Map[PackageId, Package]): Interface =
    new Interface(Util.toSignatures(packages))

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
