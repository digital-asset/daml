// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java.inner
import java.util.stream.Collectors

import com.daml.ledger.javaapi
import com.digitalasset.daml.lf.codegen.backend.java.{JavaEscaper, Types}
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.iface._
import com.squareup.javapoet._
import com.typesafe.scalalogging.Logger
import javax.lang.model.element.Modifier
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Produces an overload of a method or constructor that uses unboxed
  * versions of DAML-LF primitives, if any is passed
  *
  * e.g.
  *   - f(Int64) => Some(f(long))
  *   - f(Unit) => Some(f())
  *   - f(java.lang.String) => None
  *
  * The generated overload will always return the same type as the
  * method it's overloading, meaning that the return type will be
  * boxed even if it's a DAML-LF primitive
  */
@SuppressWarnings(Array("org.wartremover.warts.Option2Iterable"))
object ToValueGenerator {

  private val logger: Logger = Logger(LoggerFactory.getLogger(getClass.getName))

  import Types._

  def generateToValueForRecordLike(
      typeParams: IndexedSeq[String],
      fields: Fields,
      packagePrefixes: Map[PackageId, String],
      returnType: TypeName,
      returnStatement: String => CodeBlock): MethodSpec = {
    val arrayOfFields =
      ParameterizedTypeName.get(classOf[java.util.ArrayList[_]], classOf[javaapi.data.Record.Field])

    val typeExtractors = ToValueExtractorParameters.generate(typeParams)

    val toValueMethod = MethodSpec
      .methodBuilder("toValue")
      .addModifiers(Modifier.PUBLIC)
      .returns(returnType)
      .addParameters(typeExtractors.asJava)
      .addStatement(
        "$T fields = new $T($L)",
        arrayOfFields,
        arrayOfFields,
        new Integer(fields.length))

    for (FieldInfo(damlName, damlType, javaName, _) <- fields) {
      val anonNameGen = newNameGenerator
      toValueMethod.addStatement(
        "fields.add(new $T($S, $L))",
        classOf[javaapi.data.Record.Field],
        damlName,
        generateToValueConverter(
          damlType,
          CodeBlock.of("this.$L", javaName),
          () => anonNameGen.next(),
          packagePrefixes)
      )
    }
    toValueMethod.addStatement(returnStatement("fields"))
    toValueMethod.build()
  }

  def generateToValueConverter(
      damlType: Type,
      accessor: CodeBlock,
      args: () => String,
      packagePrefixes: Map[PackageId, String]): CodeBlock = {
    damlType match {
      case TypeVar(tvName) =>
        CodeBlock.of("toValue$L.apply($L)", JavaEscaper.escapeString(tvName), accessor)
      case TypePrim(
          PrimTypeBool | PrimTypeInt64 | PrimTypeDecimal | PrimTypeText | PrimTypeParty,
          _
          ) =>
        CodeBlock.of("new $T($L)", toAPITypeName(damlType), accessor)
      case TypePrim(PrimTypeTimestamp, _) =>
        CodeBlock.of("$T.fromInstant($L)", toAPITypeName(damlType), accessor)
      case TypePrim(PrimTypeDate, _) =>
        CodeBlock.of("new $T((int) $L.toEpochDay())", toAPITypeName(damlType), accessor)
      case TypePrim(PrimTypeUnit, _) =>
        CodeBlock.of("$T.getInstance()", classOf[javaapi.data.Unit])
      case TypePrim(PrimTypeList, ImmArraySeq(param)) =>
        val arg = args()
        val extractor = CodeBlock.of(
          "$L -> $L",
          arg,
          generateToValueConverter(param, CodeBlock.of("$L", arg), args, packagePrefixes)
        )
        CodeBlock.of(
          "new $T($L.stream().map($L).collect($T.<Value>toList()))",
          apiList,
          accessor,
          extractor,
          classOf[Collectors]
        )

      case TypePrim(PrimTypeOptional, ImmArraySeq(param)) =>
        val arg = args()
        val wrapped =
          generateToValueConverter(param, CodeBlock.of("$L", arg), args, packagePrefixes)
        val extractor = CodeBlock.of("$L -> $L", arg, wrapped)
        CodeBlock.of(
          // new DamlOptional(jutilOptionalParamName.map(i -> new Int64(i)))
          //       $T        (        $L            .map(       $L        ))
          "new $T($L.map($L))",
          apiOptional,
          accessor,
          extractor
        )

      case TypePrim(PrimTypeMap, ImmArraySeq(param)) =>
        val arg = args()
        val extractor = CodeBlock.of(
          "$L -> $L",
          arg,
          generateToValueConverter(param, CodeBlock.of("$L.getValue()", arg), args, packagePrefixes)
        )
        CodeBlock.of(
          "new $T($L.entrySet().stream().collect($T.<java.util.Map.Entry<String,$L>,String,Value>toMap(java.util.Map.Entry::getKey, $L)))",
          apiMap,
          accessor,
          classOf[Collectors],
          toJavaTypeName(param, packagePrefixes),
          extractor
        )

      case TypePrim(PrimTypeContractId, _) | TypeCon(_, Seq()) =>
        CodeBlock.of("$L.toValue()", accessor)

      case TypeCon(constructor, typeParameters) =>
        val extractorParams = typeParameters.map { ta =>
          val arg = args()
          val wrapped = generateToValueConverter(ta, CodeBlock.of("$L", arg), args, packagePrefixes)
          val extractor = CodeBlock.of("$L -> $L", arg, wrapped)
          extractor
        }
        CodeBlock.of(
          "$L.toValue($L)",
          accessor,
          CodeBlock.join(extractorParams.asJava, ",")
        )
    }
  }

  private def initBuilder(method: MethodSpec, codeBlock: CodeBlock): MethodSpec.Builder =
    if (method.isConstructor) {
      MethodSpec
        .constructorBuilder()
        .addStatement("this($L)", codeBlock)
    } else if (method.returnType == TypeName.VOID) {
      MethodSpec
        .methodBuilder(method.name)
        .addStatement("$L($L)", method.name, codeBlock)
    } else {
      MethodSpec
        .methodBuilder(method.name)
        .returns(method.returnType)
        .addStatement("return $L($L)", method.name, codeBlock)
    }

}
