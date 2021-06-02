// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package language

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._

private[lf] case class Interface(signatures: PartialFunction[PackageId, GenPackage[_]]) {

  def lookupPackage(pkgId: PackageId): Either[LookupError, GenPackage[_]] =
    signatures.lift(pkgId).toRight(LookupError.Package(pkgId))

  def lookupModule(
      pkgId: PackageId,
      modName: ModuleName,
  ): Either[LookupError, GenModule[_]] =
    lookupPackage(pkgId).flatMap(_.modules.get(modName).toRight(LookupError.Module(pkgId, modName)))

  def lookupDefinition(name: TypeConName): Either[LookupError, GenDefinition[_]] =
    lookupModule(name.packageId, name.qualifiedName.module).flatMap(
      _.definitions.get(name.qualifiedName.name).toRight(LookupError.DataType(name))
    )

  def lookupTypeSyn(name: TypeSynName): Either[LookupError, DTypeSyn] =
    lookupDefinition(name).flatMap {
      case typeSyn: DTypeSyn => Right(typeSyn)
      case _ => Left(LookupError.TypeSyn(name))
    }

  def lookupDataType(name: TypeConName): Either[LookupError, DDataType] =
    lookupDefinition(name).flatMap {
      case dataType: DDataType => Right(dataType)
      case _ => Left(LookupError.DataType(name))
    }

  def lookupDataRecord(
      tyCon: TypeConName
  ): Either[LookupError, (ImmArray[(TypeVarName, Kind)], DataRecord)] =
    lookupDataType(tyCon).flatMap { dataType =>
      dataType.cons match {
        case record: DataRecord => Right(dataType.params -> record)
        case _ => Left(LookupError.DataRecord(tyCon))
      }
    }

  case class RecordFieldInfo(
      typeParams: ImmArray[(TypeVarName, Kind)],
      typ: Type,
      index: Int,
  )

  def lookupRecordFieldInfo(
      tyCon: TypeConName,
      fieldName: FieldName,
  ): Either[LookupError, RecordFieldInfo] =
    lookupDataRecord(tyCon).flatMap { case (typeParams, record) =>
      record.fieldInfo.get(fieldName) match {
        case Some((typ, index)) => Right(RecordFieldInfo(typeParams, typ, index))
        case None => Left(LookupError.DataRecordField(tyCon, fieldName))
      }
    }

  def lookupDataVariant(
      tyCon: TypeConName
  ): Either[LookupError, (ImmArray[(TypeVarName, Kind)], DataVariant)] =
    lookupDataType(tyCon).flatMap { dataType =>
      dataType.cons match {
        case cons: DataVariant => Right(dataType.params -> cons)
        case _ => Left(LookupError.DataVariant(tyCon))
      }
    }

  case class VariantConstructorInfo(
      typeParams: ImmArray[(TypeVarName, Kind)],
      typDef: Type,
      rank: Int,
  ) {
    def concreteType(argTypes: Seq[Type]): Type =
      Util.substitute(typDef, typeParams.toSeq.view.map(_._1) zip argTypes)
  }

  def lookupVariantConstructor(
      tyCon: TypeConName,
      consName: VariantConName,
  ): Either[LookupError, VariantConstructorInfo] =
    lookupDataVariant(tyCon).flatMap { case (typParams, data) =>
      data.constructorInfo.get(consName) match {
        case Some((typ, rank)) => Right(VariantConstructorInfo(typParams, typ, rank))
        case None => Left(LookupError.DataVariantConstructor(tyCon, consName))
      }
    }

  def lookupDataEnum(
      tyCon: TypeConName
  ): Either[LookupError, (ImmArray[(TypeVarName, Kind)], DataEnum)] =
    lookupDataType(tyCon).flatMap { dataType =>
      dataType.cons match {
        case cons: DataEnum => Right(dataType.params -> cons)
        case _ => Left(LookupError.DataEnum(tyCon))
      }
    }

  def lookupEnumConstructor(tyCon: TypeConName, consName: EnumConName): Either[LookupError, Int] =
    lookupDataEnum(tyCon).flatMap { case (_, data) =>
      data.constructorRank.get(consName) match {
        case Some(rank) => Right(rank)
        case None => Left(LookupError.DataVariantConstructor(tyCon, consName))
      }
    }

  def lookupTemplate(name: TypeConName): Either[LookupError, GenTemplate[_]] =
    lookupModule(name.packageId, name.qualifiedName.module).flatMap(
      _.templates.get(name.qualifiedName.name).toRight(LookupError.Template(name))
    )

  def lookupChoice(
      tmpName: TypeConName,
      chName: ChoiceName,
  ): Either[LookupError, GenTemplateChoice[_]] =
    lookupTemplate(tmpName).flatMap(
      _.choices.get(chName).toRight(LookupError.Choice(tmpName, chName))
    )

  def lookupTemplateKey(name: TypeConName): Either[LookupError, GenTemplateKey[_]] =
    lookupTemplate(name).flatMap(_.key.toRight(LookupError.TemplateKey(name)))

  def lookupValue(name: ValueRef): Either[LookupError, GenDValue[_]] =
    lookupDefinition(name).flatMap {
      case valueDef: GenDValue[_] => Right(valueDef)
      case _ => Left(LookupError.Value(name))
    }

  def lookupException(name: TypeConName): Either[LookupError, GenDefException[_]] =
    lookupModule(name.packageId, name.qualifiedName.module).flatMap(
      _.exceptions.get(name.qualifiedName.name).toRight(LookupError.Exception(name))
    )

  val packageLanguageVersion: PartialFunction[PackageId, LanguageVersion] =
    signatures andThen (_.languageVersion)

}

object Interface {
  val Empty = Interface(PartialFunction.empty)
}
