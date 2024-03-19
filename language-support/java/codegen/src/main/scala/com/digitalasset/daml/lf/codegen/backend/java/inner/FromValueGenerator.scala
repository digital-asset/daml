// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.codegen.{
  ContractCompanion,
  ValueDecoder,
  PrimitiveValueDecoders,
}
import com.daml.lf.codegen.backend.java.JavaEscaper
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.typesig._
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging

import javax.lang.model.element.Modifier
import scala.jdk.CollectionConverters._

private[inner] object FromValueGenerator extends StrictLogging {

  def generateValueDecoderForRecordLike(
      fields: Fields,
      className: TypeName,
      typeParameters: IndexedSeq[String],
      methodName: String,
      recordValueExtractor: (String, String) => CodeBlock,
      isPublic: Boolean = true,
  )(implicit packagePrefixes: PackagePrefixes): MethodSpec = {
    logger.debug(s"Generating value decoder method $methodName")

    val converterParams = FromValueExtractorParameters
      .generate(typeParameters)
      .valueDecoderParameterSpecs

    def isOptional(t: Type) =
      t match {
        case TypePrim(PrimTypeOptional, _) => true
        case _ => false
      }

    val optionalFieldsSize = fields.reverse.takeWhile(f => isOptional(f.damlType)).size

    val fromValueCode = CodeBlock
      .builder()
      .add(recordValueExtractor("value$", "recordValue$"))
      .addStatement(
        "$T fields$$ = $T.recordCheck($L,$L,$WrecordValue$$)",
        ParameterizedTypeName
          .get(classOf[java.util.List[_]], classOf[javaapi.data.DamlRecord.Field]),
        classOf[PrimitiveValueDecoders],
        fields.size,
        optionalFieldsSize,
      )

    fields.iterator.zip(accessors).foreach { case (FieldInfo(_, damlType, javaName, _), accessor) =>
      fromValueCode.addStatement(
        generateFieldExtractor(damlType, javaName, accessor)
      )
    }

    fromValueCode
      .addStatement(
        "return new $T($L)",
        className,
        generateArgumentList(fields.map(_.javaName)),
      )

    MethodSpec
      .methodBuilder(methodName)
      .addModifiers(if (isPublic) Modifier.PUBLIC else Modifier.PRIVATE, Modifier.STATIC)
      .returns(ParameterizedTypeName.get(ClassName.get(classOf[ValueDecoder[_]]), className))
      .addTypeVariables(className.typeParameters)
      .addParameters(converterParams.asJava)
      .addException(classOf[IllegalArgumentException])
      .beginControlFlow("return $L ->", "value$")
      .addCode(fromValueCode.build())
      // put empty string in endControlFlow in order to have semicolon
      .endControlFlow("")
      .build()
  }

  def generateContractCompanionValueDecoder(
      className: TypeName,
      typeParameters: IndexedSeq[String],
  ): MethodSpec = {
    logger.debug("Generating method for getting value decoder in template class")
    val converterParams = FromValueExtractorParameters
      .generate(typeParameters)
      .valueDecoderParameterSpecs

    MethodSpec
      .methodBuilder("valueDecoder")
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .returns(ParameterizedTypeName.get(ClassName.get(classOf[ValueDecoder[_]]), className))
      .addTypeVariables(className.typeParameters)
      .addParameters(converterParams.asJava)
      .addException(classOf[IllegalArgumentException])
      .addStatement("return $T.valueDecoder(COMPANION)", classOf[ContractCompanion[_, _, _]])
      .build()
  }

  private def accessors =
    Iterator.from(0).map(i => CodeBlock.of("fields$$.get($L).getValue()", Integer.valueOf(i)))

  def variantCheck(constructorName: String, inputVar: String, outputVar: String): CodeBlock = {
    CodeBlock
      .builder()
      .addStatement(
        "$T $L =$W$T.variantCheck($S,$W$L)",
        classOf[javaapi.data.Value],
        outputVar,
        classOf[PrimitiveValueDecoders],
        constructorName,
        inputVar,
      )
      .build()
  }

  def generateFieldExtractor(fieldType: Type, field: String, accessor: CodeBlock)(implicit
      packagePrefixes: PackagePrefixes
  ): CodeBlock =
    CodeBlock.of(
      "$T $L =$W$L",
      toJavaTypeName(fieldType),
      field,
      extractor(fieldType, field, accessor, newNameGenerator),
    )

  private[this] val extractors =
    Map[PrimType, String](
      (PrimTypeBool, "fromBool"),
      (PrimTypeInt64, "fromInt64"),
      (PrimTypeText, "fromText"),
      (PrimTypeTimestamp, "fromTimestamp"),
      (PrimTypeParty, "fromParty"),
      (PrimTypeUnit, "fromUnit"),
      (PrimTypeDate, "fromDate"),
    )

  // If [[typeName]] is defined in the primitive extractors map, create an extractor for it
  private[this] def primitive(
      damlType: PrimType,
      apiType: TypeName,
      field: String,
  ): Option[Extractor] =
    extractors
      .get(damlType)
      .map { extractor =>
        logger.debug(s"Generating primitive extractor for $field of type $apiType")
        Extractor.Decoder(
          CodeBlock.of("$T.$L", classOf[PrimitiveValueDecoders], extractor)
        )
      }

  private def orElseThrow(typeName: TypeName, field: String) =
    CodeBlock.of(
      ".orElseThrow(() -> new IllegalArgumentException($S))",
      s"Expected $field to be of type $typeName",
    )

  private[this] sealed abstract class Extractor extends Product with Serializable {
    import Extractor._

    /** An expression of type `T` where `T` is the decoding target */
    def extract(accessor: CodeBlock): CodeBlock = this match {
      case Decoder(decoder) => CodeBlock.of("$L$Z.decode($L)", decoder, accessor)
      case FromFreeVar(decodeAccessor) => decodeAccessor(accessor)
    }

    /** An expression of type `ValueDecoder<T>` for some `T` */
    def asDecoder(args: Iterator[String]): CodeBlock = this match {
      case Decoder(decoder) => decoder
      case FromFreeVar(decodeAccessor) =>
        val lambdaBinding = args.next()
        CodeBlock.of(
          "$L ->$>$W$L$<",
          lambdaBinding,
          decodeAccessor(CodeBlock.of("$L", lambdaBinding)),
        )
    }
  }

  private[this] object Extractor {

    /** Decode the expression passed in as an argument. */
    final case class FromFreeVar(decodeAccessor: CodeBlock => CodeBlock) extends Extractor

    /** Produce a point-free ValueDecoder. */
    final case class Decoder(decoder: CodeBlock) extends Extractor
  }

  /** Generates extractor for types that are not immediately covered by primitive extractors
    * @param damlType The type of the field being accessed
    * @param field The name of the field being accessed
    * @param accessor The [[CodeBlock]] that defines how to access the item in the first place
    * @param args An iterator providing argument names for nested calls without shadowing
    * @return A [[CodeBlock]] that defines the extractor for the whole composite type
    */
  private[inner] def extractor(
      damlType: Type,
      field: String,
      accessor: CodeBlock,
      args: Iterator[String],
  )(implicit
      packagePrefixes: PackagePrefixes
  ): CodeBlock =
    extractorRec(damlType, field, args) extract accessor

  private[this] def extractorRec(damlType: Type, field: String, args: Iterator[String])(implicit
      packagePrefixes: PackagePrefixes
  ): Extractor = {

    lazy val apiType = toAPITypeName(damlType)
    lazy val javaType = toJavaTypeName(damlType)
    logger.debug(s"Generating composite extractor for $field of type $javaType")

    import Extractor._

    // shorten recursive calls
    // we always want a ValueDecoder when recurring
    def go(recDamlType: Type): CodeBlock =
      extractorRec(recDamlType, field, args) asDecoder args

    def oneTypeArgPrim(primFun: String, param: Type): Extractor =
      Decoder(
        CodeBlock.of(
          "$T.$L($>$Z$L$<)",
          classOf[PrimitiveValueDecoders],
          primFun,
          go(param),
        )
      )

    damlType match {
      // Case #1: the type is actually a type parameter: we assume the calling code defines a
      // suitably named function that takes the result of accessing the underlying data (as
      // defined by the accessor
      // TODO: review aforementioned assumption
      case TypeVar(tvName) =>
        Decoder(CodeBlock.of("fromValue$L", JavaEscaper.escapeString(tvName)))

      case TypePrim(PrimTypeList, ImmArraySeq(param)) =>
        oneTypeArgPrim("fromList", param)

      case TypePrim(PrimTypeOptional, ImmArraySeq(param)) =>
        oneTypeArgPrim("fromOptional", param)

      case TypePrim(PrimTypeContractId, ImmArraySeq(TypeVar(name))) =>
        Decoder(
          CodeBlock.of(
            "$T.fromContractId(fromValue$L)",
            classOf[PrimitiveValueDecoders],
            JavaEscaper.escapeString(name),
          )
        )
      case TypePrim(PrimTypeContractId, ImmArraySeq(_)) =>
        FromFreeVar(accessor =>
          CodeBlock.of(
            "new $T($L.asContractId()$L.getValue())",
            javaType,
            accessor,
            orElseThrow(apiType, field),
          )
        )
      case TypePrim(PrimTypeTextMap, ImmArraySeq(param)) =>
        oneTypeArgPrim("fromTextMap", param)

      case TypePrim(PrimTypeGenMap, ImmArraySeq(keyType, valueType)) =>
        Decoder(
          CodeBlock.of(
            "$T.fromGenMap($>$Z$L,$W$L$<)",
            classOf[PrimitiveValueDecoders],
            go(keyType),
            go(valueType),
          )
        )

      case TypeNumeric(_) =>
        Decoder(CodeBlock.of("$T.fromNumeric", classOf[PrimitiveValueDecoders]))

      case TypePrim(prim, _) =>
        primitive(prim, apiType, field).getOrElse(
          sys.error(s"Unhandled primitive type $prim")
        )

      case TypeCon(_, ImmArraySeq()) =>
        Decoder(CodeBlock.of("$T.valueDecoder()", javaType))

      case TypeCon(_, typeParameters) =>
        val (targs, valueDecoders) = typeParameters.map {
          case targ @ TypeVar(tvName) =>
            toJavaTypeName(targ) -> CodeBlock.of(
              "fromValue$L",
              JavaEscaper.escapeString(tvName),
            )
          case targ @ TypeCon(_, ImmArraySeq()) =>
            toJavaTypeName(targ) -> CodeBlock.of(
              "$T.valueDecoder()",
              toJavaTypeName(targ),
            )
          case targ =>
            toJavaTypeName(targ) -> go(targ)
        }.unzip

        val targsCode = CodeBlock.join(targs.map(CodeBlock.of("$L", _)).asJava, ",$W")
        Decoder(
          CodeBlock
            .builder()
            .add(CodeBlock.of("$T.<$L>valueDecoder(", javaType.rawType, targsCode))
            .add(CodeBlock.join(valueDecoders.asJava, ",$W"))
            .add(CodeBlock.of(")"))
            .build()
        )
    }
  }
}
