// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi.data.codegen.ValueDecoder
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.typesig._
import com.squareup.javapoet._
import javax.lang.model.element.Modifier
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

final class FromValueGeneratorSpec extends AnyFlatSpec with Matchers {

  behavior of "FromValueGenerator.generateValueDecoderForRecordLike"

  private implicit val packagePrefixes: PackagePrefixes = PackagePrefixes(Map.empty)

  private val simpleRecordValueExtractor: (String, String) => CodeBlock = (inVar, outVar) =>
    CodeBlock
      .builder()
      .addStatement(
        "$T $L = $L",
        classOf[com.daml.ledger.javaapi.data.Value],
        outVar,
        inVar,
      )
      .build()

  it should "generate a method with the correct name" in {
    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
      "testDecoder",
      simpleRecordValueExtractor,
    )

    method.name shouldBe "testDecoder"
  }

  it should "be public and static by default" in {
    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
      "testDecoder",
      simpleRecordValueExtractor,
    )

    method.modifiers.asScala should contain.only(Modifier.PUBLIC, Modifier.STATIC)
  }

  it should "be private and static when isPublic is false" in {
    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
      "testDecoder",
      simpleRecordValueExtractor,
      isPublic = false,
    )

    method.modifiers.asScala should contain.only(Modifier.PRIVATE, Modifier.STATIC)
  }

  it should "return a ValueDecoder parameterized with the class name" in {
    val className = ClassName.bestGuess("TestClass")
    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      className,
      IndexedSeq.empty,
      "testDecoder",
      simpleRecordValueExtractor,
    )

    method.returnType shouldEqual ParameterizedTypeName.get(
      ClassName.get(classOf[ValueDecoder[_]]),
      className,
    )
  }

  it should "throw IllegalArgumentException" in {
    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
      "testDecoder",
      simpleRecordValueExtractor,
    )

    method.exceptions should contain only TypeName.get(classOf[IllegalArgumentException])
  }

  it should "have no parameters for a record with no type parameters" in {
    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
      "testDecoder",
      simpleRecordValueExtractor,
    )

    method.parameters shouldBe empty
  }

  it should "have decoder parameters for type parameters" in {
    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq("T", "U"),
      "testDecoder",
      simpleRecordValueExtractor,
    )

    method.parameters.asScala should have size 2
    method.parameters.asScala.map(_.name) should contain.inOrderOnly("valueDecoderT", "valueDecoderU")
  }

  it should "generate code that uses ValueDecoder.create with two-parameter lambda" in {
    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
      "testDecoder",
      simpleRecordValueExtractor,
    )

    val code = method.code.toString
    code should include("ValueDecoder.create((value$, policy$) ->")
  }

  it should "generate field extraction code for simple fields" in {
    val fields = getFieldsWithTypes(
      ImmArraySeq(
        Ref.Name.assertFromString("boolField") -> TypePrim(PrimTypeBool, ImmArraySeq.empty),
        Ref.Name.assertFromString("textField") -> TypePrim(PrimTypeText, ImmArraySeq.empty),
      )
    )

    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      fields,
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
      "testDecoder",
      simpleRecordValueExtractor,
    )

    val code = method.code.toString
    code should include("Boolean boolField")
    code should include("String textField")
    code should include("new TestClass(boolField, textField)")
  }

  it should "handle optional fields correctly" in {
    val fields = getFieldsWithTypes(
      ImmArraySeq(
        Ref.Name.assertFromString("requiredField") -> TypePrim(PrimTypeText, ImmArraySeq.empty),
        Ref.Name.assertFromString("optionalField") -> TypePrim(
          PrimTypeOptional,
          ImmArraySeq(TypePrim(PrimTypeText, ImmArraySeq.empty)),
        ),
      )
    )

    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      fields,
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
      "testDecoder",
      simpleRecordValueExtractor,
    )

    val code = method.code.toString
    // Should call checkAndPrepareRecord with field count and optional field count
    code should include("checkAndPrepareRecord(2, 1")
    code should include("Optional<String> optionalField")
  }

  it should "handle numeric types" in {
    val fields = getFieldsWithTypes(
      ImmArraySeq(
        Ref.Name.assertFromString("int64Field") -> TypePrim(PrimTypeInt64, ImmArraySeq.empty),
        Ref.Name.assertFromString("numericField") -> TypeNumeric(
          Ref.Name.assertFromString("10")
        ),
      )
    )

    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      fields,
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
      "testDecoder",
      simpleRecordValueExtractor,
    )

    val code = method.code.toString
    code should include("Long int64Field")
    code should include("BigDecimal numericField")
  }

  it should "handle date and timestamp types" in {
    val fields = getFieldsWithTypes(
      ImmArraySeq(
        Ref.Name.assertFromString("dateField") -> TypePrim(PrimTypeDate, ImmArraySeq.empty),
        Ref.Name.assertFromString("timestampField") -> TypePrim(
          PrimTypeTimestamp,
          ImmArraySeq.empty,
        ),
      )
    )

    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      fields,
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
      "testDecoder",
      simpleRecordValueExtractor,
    )

    val code = method.code.toString
    code should include("LocalDate dateField")
    code should include("Instant timestampField")
  }

  it should "handle party and unit types" in {
    val fields = getFieldsWithTypes(
      ImmArraySeq(
        Ref.Name.assertFromString("partyField") -> TypePrim(PrimTypeParty, ImmArraySeq.empty),
        Ref.Name.assertFromString("unitField") -> TypePrim(PrimTypeUnit, ImmArraySeq.empty),
      )
    )

    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      fields,
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
      "testDecoder",
      simpleRecordValueExtractor,
    )

    val code = method.code.toString
    code should include("String partyField")
    code should include("Unit unitField")
  }

  it should "handle list types" in {
    val fields = getFieldsWithTypes(
      ImmArraySeq(
        Ref.Name.assertFromString("listField") -> TypePrim(
          PrimTypeList,
          ImmArraySeq(TypePrim(PrimTypeText, ImmArraySeq.empty)),
        )
      )
    )

    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      fields,
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
      "testDecoder",
      simpleRecordValueExtractor,
    )

    val code = method.code.toString
    code should include("List<String> listField")
  }

  it should "include type variables in method signature" in {
    val className = ClassName.bestGuess("TestClass")
    val parameterizedClassName =
      ParameterizedTypeName.get(className, TypeVariableName.get("T"), TypeVariableName.get("U"))

    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      parameterizedClassName,
      IndexedSeq("T", "U"),
      "testDecoder",
      simpleRecordValueExtractor,
    )

    method.typeVariables.asScala should have size 2
    method.typeVariables.asScala.map(_.name) should contain.inOrderOnly("T", "U")
  }

  it should "use custom record value extractor" in {
    val customExtractor: (String, String) => CodeBlock = (inVar, outVar) =>
      CodeBlock
        .builder()
        .addStatement("// Custom extraction logic")
        .addStatement(
          "$T $L = customExtract($L)",
          classOf[com.daml.ledger.javaapi.data.Value],
          outVar,
          inVar,
        )
        .build()

    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
      "testDecoder",
      customExtractor,
    )

    val code = method.code.toString
    code should include("Custom extraction logic")
    code should include("customExtract(value$)")
  }

  it should "generate return statement with new instance" in {
    val fields = getFieldsWithTypes(
      ImmArraySeq(
        Ref.Name.assertFromString("field1") -> TypePrim(PrimTypeBool, ImmArraySeq.empty),
        Ref.Name.assertFromString("field2") -> TypePrim(PrimTypeText, ImmArraySeq.empty),
      )
    )

    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      fields,
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
      "testDecoder",
      simpleRecordValueExtractor,
    )

    val code = method.code.toString
    code should include("return new TestClass(field1, field2)")
  }

  it should "handle empty record" in {
    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
      "testDecoder",
      simpleRecordValueExtractor,
    )

    val code = method.code.toString
    code should include("checkAndPrepareRecord(0, 0")
    code should include("return new TestClass()")
  }

  it should "correctly count trailing optional fields" in {
    val fields = getFieldsWithTypes(
      ImmArraySeq(
        Ref.Name.assertFromString("field1") -> TypePrim(PrimTypeText, ImmArraySeq.empty),
        Ref.Name.assertFromString("field2") -> TypePrim(
          PrimTypeOptional,
          ImmArraySeq(TypePrim(PrimTypeText, ImmArraySeq.empty)),
        ),
        Ref.Name.assertFromString("field3") -> TypePrim(
          PrimTypeOptional,
          ImmArraySeq(TypePrim(PrimTypeInt64, ImmArraySeq.empty)),
        ),
      )
    )

    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      fields,
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
      "testDecoder",
      simpleRecordValueExtractor,
    )

    val code = method.code.toString
    // Should have 3 fields total, with 2 trailing optional fields
    code should include("checkAndPrepareRecord(3, 2")
  }

  it should "not count non-trailing optional fields in optional count" in {
    val fields = getFieldsWithTypes(
      ImmArraySeq(
        Ref.Name.assertFromString("field1") -> TypePrim(
          PrimTypeOptional,
          ImmArraySeq(TypePrim(PrimTypeText, ImmArraySeq.empty)),
        ),
        Ref.Name.assertFromString("field2") -> TypePrim(PrimTypeText, ImmArraySeq.empty),
        Ref.Name.assertFromString("field3") -> TypePrim(
          PrimTypeOptional,
          ImmArraySeq(TypePrim(PrimTypeInt64, ImmArraySeq.empty)),
        ),
      )
    )

    val method = FromValueGenerator.generateValueDecoderForRecordLike(
      fields,
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
      "testDecoder",
      simpleRecordValueExtractor,
    )

    val code = method.code.toString
    code should include("checkAndPrepareRecord(3, 1")
  }
}

