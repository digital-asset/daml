// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.lf.codegen.backend.java.{JavaEscaper, Types}
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.typesig._
import com.squareup.javapoet._
import javax.lang.model.element.Modifier

import scala.jdk.CollectionConverters._

/** Produces an overload of a method or constructor that uses unboxed
  * versions of Daml-LF primitives, if any is passed
  *
  * e.g.
  *   - f(Int64) => Some(f(long))
  *   - f(Unit) => Some(f())
  *   - f(java.lang.String) => None
  *
  * The generated overload will always return the same type as the
  * method it's overloading, meaning that the return type will be
  * boxed even if it's a Daml-LF primitive
  */
object ToValueGenerator {

  import Types._

  def generateToValueForRecordLike(
      typeParams: IndexedSeq[String],
      fields: Fields,
      returnType: TypeName,
      returnStatement: String => CodeBlock,
  )(implicit packagePrefixes: PackagePrefixes): MethodSpec = {
    val arrayOfFields =
      ParameterizedTypeName.get(
        classOf[java.util.ArrayList[_]],
        classOf[javaapi.data.DamlRecord.Field],
      )

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
        Integer.valueOf(fields.length),
      )

    for (FieldInfo(damlName, damlType, javaName, _) <- fields) {
      toValueMethod.addStatement(
        "fields.add(new $T($S, $L))",
        classOf[javaapi.data.DamlRecord.Field],
        damlName,
        generateToValueConverter(
          damlType,
          CodeBlock.of("this.$L", javaName),
          newNameGenerator,
        ),
      )
    }
    toValueMethod.addStatement(returnStatement("fields"))
    toValueMethod.build()
  }

  def generateToValueConverter(damlType: Type, accessor: CodeBlock, args: Iterator[String])(implicit
      packagePrefixes: PackagePrefixes
  ): CodeBlock = {
    damlType match {
      case TypeVar(tvName) =>
        CodeBlock.of("toValue$L.apply($L)", JavaEscaper.escapeString(tvName), accessor)
      case TypeNumeric(_) | TypePrim(PrimTypeInt64 | PrimTypeText | PrimTypeParty, _) =>
        CodeBlock.of("new $T($L)", toAPITypeName(damlType), accessor)
      case TypePrim(PrimTypeBool, _) => CodeBlock.of("$T.of($L)", toAPITypeName(damlType), accessor)
      case TypePrim(PrimTypeTimestamp, _) =>
        CodeBlock.of("$T.fromInstant($L)", toAPITypeName(damlType), accessor)
      case TypePrim(PrimTypeDate, _) =>
        CodeBlock.of("new $T((int) $L.toEpochDay())", toAPITypeName(damlType), accessor)
      case TypePrim(PrimTypeUnit, _) =>
        CodeBlock.of("$T.getInstance()", classOf[javaapi.data.Unit])
      case TypePrim(PrimTypeList, ImmArraySeq(param)) =>
        val arg = args.next()
        val extractor = CodeBlock.of(
          "$L -> $L",
          arg,
          generateToValueConverter(param, CodeBlock.of("$L", arg), args),
        )
        CodeBlock.of(
          "$L.stream().collect($T.toDamlList($L))",
          accessor,
          apiCollectors,
          extractor,
        )

      case TypePrim(PrimTypeOptional, ImmArraySeq(param)) =>
        val arg = args.next()
        val wrapped =
          generateToValueConverter(param, CodeBlock.of("$L", arg), args)
        val extractor = CodeBlock.of("$L -> $L", arg, wrapped)
        CodeBlock.of(
          "$T.of($L.map($L))",
          apiOptional,
          accessor,
          extractor,
        )

      case TypePrim(PrimTypeTextMap, ImmArraySeq(param)) =>
        val arg = args.next()
        val extractor = CodeBlock.of(
          "$L -> $L",
          arg,
          generateToValueConverter(param, CodeBlock.of("$L.getValue()", arg), args),
        )
        CodeBlock.of(
          "$L.entrySet()$Z.stream()$Z.collect($T.toDamlTextMap($T::getKey, $L))$W",
          accessor,
          apiCollectors,
          classOf[java.util.Map.Entry[_, _]],
          extractor,
        )

      case TypePrim(PrimTypeGenMap, ImmArraySeq(keyType, valueType)) =>
        val arg = args.next()
        val keyExtractor = CodeBlock.of(
          "$L -> $L",
          arg,
          generateToValueConverter(keyType, CodeBlock.of("$L.getKey()", arg), args),
        )
        val valueExtractor = CodeBlock.of(
          "$L -> $L",
          arg,
          generateToValueConverter(
            valueType,
            CodeBlock.of("$L.getValue()", arg),
            args,
          ),
        )
        CodeBlock.of(
          "$L.entrySet()$Z.stream()$Z.collect($T.toDamlGenMap($L, $L))",
          accessor,
          apiCollectors,
          keyExtractor,
          valueExtractor,
        )

      case TypePrim(PrimTypeContractId, _) | TypeCon(_, Seq()) =>
        CodeBlock.of("$L.toValue()", accessor)

      case TypeCon(_, typeParameters) =>
        val extractorParams = typeParameters.map { ta =>
          val arg = args.next()
          val wrapped = generateToValueConverter(ta, CodeBlock.of("$L", arg), args)
          val extractor = CodeBlock.of("$L -> $L", arg, wrapped)
          extractor
        }
        CodeBlock.of(
          "$L.toValue($L)",
          accessor,
          CodeBlock.join(extractorParams.asJava, ",$W"),
        )
      case ty => throw new IllegalArgumentException(s"Invalid Daml datatype: $ty")
    }
  }

}
