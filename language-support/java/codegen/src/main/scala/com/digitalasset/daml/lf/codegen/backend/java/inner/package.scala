// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java

import java.util

import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.codegen.ContractId
import com.daml.ledger.javaapi.data.{DamlGenMap, DamlList, DamlOptional, DamlTextMap}
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.daml.lf.iface._
import com.squareup.javapoet._

import scala.collection.JavaConverters._

package object inner {

  private[inner] def generateArgumentList(fields: IndexedSeq[String]): CodeBlock =
    CodeBlock.join(fields.map(CodeBlock.of("$L", _)).asJava, ", ")

  private[inner] def newNameGenerator = Iterator.from(0).map(n => s"v$$$n")

  case class FieldInfo(damlName: String, damlType: Type, javaName: String, javaType: TypeName)

  type Fields = IndexedSeq[FieldInfo]

  private[inner] def getFieldsWithTypes(
      fields: IndexedSeq[FieldWithType],
      packagePrefixes: Map[PackageId, String]): Fields =
    fields.map(getFieldWithType(_, packagePrefixes))

  private[inner] def getFieldWithType(
      fwt: FieldWithType,
      packagePrefixes: Map[PackageId, String]): FieldInfo =
    FieldInfo(
      fwt._1,
      fwt._2,
      JavaEscaper.escapeString(fwt._1),
      toJavaTypeName(fwt._2, packagePrefixes))

  private[inner] def toJavaTypeName(
      damlType: Type,
      packagePrefixes: Map[PackageId, String]): TypeName =
    damlType match {
      case TypeCon(TypeConName(ident), Seq()) =>
        ClassName.bestGuess(fullyQualifiedName(ident, packagePrefixes)).box()
      case TypeCon(TypeConName(ident), typeParameters) =>
        ParameterizedTypeName.get(
          ClassName.bestGuess(fullyQualifiedName(ident, packagePrefixes)),
          typeParameters.map(toJavaTypeName(_, packagePrefixes)): _*)
      case TypePrim(PrimTypeBool, _) => ClassName.get(classOf[java.lang.Boolean])
      case TypePrim(PrimTypeInt64, _) => ClassName.get(classOf[java.lang.Long])
      case TypeNumeric(_) => ClassName.get(classOf[java.math.BigDecimal])
      case TypePrim(PrimTypeText, _) => ClassName.get(classOf[java.lang.String])
      case TypePrim(PrimTypeDate, _) => ClassName.get(classOf[java.time.LocalDate])
      case TypePrim(PrimTypeTimestamp, _) => ClassName.get(classOf[java.time.Instant])
      case TypePrim(PrimTypeParty, _) => ClassName.get(classOf[java.lang.String])
      case TypePrim(PrimTypeContractId, ImmArraySeq(templateType)) =>
        toJavaTypeName(templateType, packagePrefixes) match {
          case templateClass: ClassName => templateClass.nestedClass("ContractId")
          case typeVariableName: TypeVariableName =>
            ParameterizedTypeName.get(ClassName.get(classOf[ContractId[_]]), typeVariableName)
          case unexpected => sys.error(s"Unexpected type [$unexpected] for DAML type [$damlType]")
        }
      case TypePrim(PrimTypeList, typeParameters) =>
        ParameterizedTypeName
          .get(
            ClassName.get(classOf[java.util.List[_]]),
            typeParameters.map(toJavaTypeName(_, packagePrefixes)): _*)
      case TypePrim(PrimTypeOptional, typeParameters) =>
        ParameterizedTypeName
          .get(
            ClassName.get(classOf[java.util.Optional[_]]),
            typeParameters.map(toJavaTypeName(_, packagePrefixes)): _*)
      case TypePrim(PrimTypeTextMap, typeParameters) =>
        ParameterizedTypeName
          .get(
            ClassName.get(classOf[java.util.Map[String, _]]),
            ClassName.get(classOf[java.lang.String]) +:
              typeParameters.map(toJavaTypeName(_, packagePrefixes)): _*)
      case TypePrim(PrimTypeGenMap, typeParameters) =>
        ParameterizedTypeName
          .get(
            ClassName.get(classOf[java.util.Map[_, _]]),
            typeParameters.map(toJavaTypeName(_, packagePrefixes)): _*)
      case TypePrim(PrimTypeUnit, _) => ClassName.get(classOf[javaapi.data.Unit])
      case TypeVar(name) => TypeVariableName.get(JavaEscaper.escapeString(name))
    }

  private[inner] def toAPITypeName(damlType: Type): TypeName =
    damlType match {
      case TypePrim(PrimTypeBool, _) => ClassName.get(classOf[javaapi.data.Bool])
      case TypePrim(PrimTypeInt64, _) => ClassName.get(classOf[javaapi.data.Int64])
      case TypeNumeric(_) => ClassName.get(classOf[javaapi.data.Numeric])
      case TypePrim(PrimTypeText, _) => ClassName.get(classOf[javaapi.data.Text])
      case TypePrim(PrimTypeDate, _) => ClassName.get(classOf[javaapi.data.Date])
      case TypePrim(PrimTypeTimestamp, _) => ClassName.get(classOf[javaapi.data.Timestamp])
      case TypePrim(PrimTypeParty, _) => ClassName.get(classOf[javaapi.data.Party])
      case TypePrim(PrimTypeContractId, _) =>
        ClassName.get(classOf[javaapi.data.ContractId])
      case TypePrim(PrimTypeList, _) =>
        ClassName.get(classOf[DamlList])
      case TypePrim(PrimTypeOptional, _) =>
        ClassName.get(classOf[DamlOptional])
      case TypePrim(PrimTypeTextMap, _) =>
        ClassName.get(classOf[DamlTextMap])
      case TypePrim(PrimTypeGenMap, _) =>
        ClassName.get(classOf[DamlGenMap])
      case TypePrim(PrimTypeUnit, _) =>
        ClassName.get(classOf[javaapi.data.Unit])
      case TypeCon(_, _) | TypeVar(_) =>
        sys.error("Assumption error: toAPITypeName should not be called for type constructors!")
    }

  def fullyQualifiedName(
      identifier: Identifier,
      packagePrefixes: Map[PackageId, String]): String = {
    val Identifier(_, QualifiedName(module, name)) = identifier

    // consider all but the last name segment to be part of the java package name
    val packageSegments = module.segments.slowAppend(name.segments).toSeq.dropRight(1)
    // consider the last name segment to be the java class name
    val className = name.segments.toSeq.takeRight(1)

    val packageName = packageSegments.map(_.toLowerCase)
    val packagePrefix = packagePrefixes.getOrElse(identifier.packageId, "")

    (Vector(packagePrefix) ++ packageName ++ className)
      .filter(_.nonEmpty)
      .map(JavaEscaper.escapeString)
      .mkString(".")
  }

  def distinctTypeVars(
      fields: Fields,
      typeVars: IndexedSeq[String]): IndexedSeq[IndexedSeq[String]] = {
    val escapedNestedTypeVars = escapedNestedTypeVarNames(fields)
    if (escapedNestedTypeVars.sorted == typeVars.sorted) Vector(typeVars)
    else Vector(escapedNestedTypeVars, typeVars)
  }

  def distinctTypeVars(tpe: Type, typeVars: IndexedSeq[String]): IndexedSeq[IndexedSeq[String]] = {
    val escapedNestedTypeVars = escapedNestedTypeVarNames(tpe)
    if (escapedNestedTypeVars.sorted == typeVars.sorted) Vector(typeVars)
    else Vector(escapedNestedTypeVars, typeVars)
  }

  // Ensures that different sets of nested unbound type variables
  // do not repeat with the call to `distinct` at the end
  def escapedNestedTypeVarNames(fields: Fields): IndexedSeq[String] =
    fields
      .map(_.damlType)
      .flatMap(escapedNestedTypeVarNames)
      .distinct

  // We traverse nested unbound type parameters. At the end we need to make sure
  // that we that we have a unique set of parameters without duplicates.
  // This can happen if there are two fields of type `a`, but we only need to carry
  // this particular type parameter forward once, hence the usage of a set.
  def escapedNestedTypeVarNames(tpe: Type): IndexedSeq[String] = {
    def go(typeParams: Set[String], tpe: Type): Set[String] = {
      tpe match {
        case TypeVar(x) => typeParams + JavaEscaper.escapeString(x)
        case TypePrim(_, args) => args.foldLeft(typeParams)(go)
        case TypeCon(_, args) => args.foldLeft(typeParams)(go)
        case TypeNumeric(_) => Set.empty
      }
    }
    go(Set.empty, tpe).toVector
  }

  implicit class TypeNameExtensions(name: TypeName) {
    def rawType: ClassName = name match {
      case p: ParameterizedTypeName => p.rawType
      case c: ClassName => c
      case _ => sys.error(s"Assumption error! calling rawType on unexpected typename: $name")
    }

    def typeParameters: util.List[TypeVariableName] = name match {
      case p: ParameterizedTypeName =>
        val typeVars = p.typeArguments.asScala.collect { case tv: TypeVariableName => tv }
        if (typeVars.length != p.typeArguments.size()) {
          sys.error(s"Assumption error! Unexpected type arguments: ${p.typeArguments}")
        }
        typeVars.asJava
      case _: ClassName => List.empty[TypeVariableName].asJava
      case _ => sys.error(s"Assumption error! Calling typeParameters on unexpected typename: $name")
    }
  }

  implicit class ClassNameExtensions(name: ClassName) {
    def parameterized(typeParams: IndexedSeq[String]): TypeName = {
      if (typeParams.isEmpty) name
      else ParameterizedTypeName.get(name, typeParams.map(TypeVariableName.get): _*)
    }

    def asWildcardType(typeParams: IndexedSeq[String]): TypeName = {
      if (typeParams.isEmpty) name
      else
        ParameterizedTypeName.get(
          name,
          typeParams.map(_ => WildcardTypeName.subtypeOf(classOf[Object])): _*)
    }
  }

}
