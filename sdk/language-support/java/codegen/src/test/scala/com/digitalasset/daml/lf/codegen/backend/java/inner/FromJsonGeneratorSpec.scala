// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi.data.codegen.json.{JsonLfDecoder, JsonLfReader}
import com.daml.ledger.javaapi.data.codegen.UnknownTrailingFieldPolicy
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.typesig._
import com.squareup.javapoet._
import javax.lang.model.element.Modifier
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

final class FromJsonGeneratorSpec extends AnyFlatSpec with Matchers {

  private implicit val packagePrefixes: PackagePrefixes = PackagePrefixes(Map.empty)

  behavior of "FromJsonGenerator.fromJsonStringWithPolicy"

  it should "generate a method named 'fromJson'" in {
    val method = FromJsonGenerator.forRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
    )

    val fromJsonWithPolicy = method.find(_.parameters.asScala.exists(_.`type` == TypeName.get(classOf[UnknownTrailingFieldPolicy])))
    fromJsonWithPolicy shouldBe defined
    fromJsonWithPolicy.get.name shouldBe "fromJson"
  }

  it should "be public and static" in {
    val methods = FromJsonGenerator.forRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
    )

    val fromJsonWithPolicy = methods.find(_.parameters.asScala.exists(_.`type` == TypeName.get(classOf[UnknownTrailingFieldPolicy])))
    fromJsonWithPolicy shouldBe defined
    fromJsonWithPolicy.get.modifiers.asScala should contain.only(Modifier.PUBLIC, Modifier.STATIC)
  }

  it should "accept json string, policy, and decoder parameters for type params" in {
    val methods = FromJsonGenerator.forRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq("T", "U"),
    )

    val fromJsonWithPolicy = methods.find(_.parameters.asScala.exists(_.`type` == TypeName.get(classOf[UnknownTrailingFieldPolicy])))
    fromJsonWithPolicy shouldBe defined

    val params = fromJsonWithPolicy.get.parameters.asScala
    params should have size 4
    params.map(_.name) should contain.inOrderOnly("json", "decodeT", "decodeU", "policy")
    params.find(_.name == "json").get.`type` shouldEqual TypeName.get(classOf[String])
    params.find(_.name == "policy").get.`type` shouldEqual TypeName.get(classOf[UnknownTrailingFieldPolicy])
  }

  it should "return the parameterized class name" in {
    val className = ClassName.bestGuess("TestClass")
    val methods = FromJsonGenerator.forRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      className,
      IndexedSeq("T"),
    )

    val fromJsonWithPolicy = methods.find(_.parameters.asScala.exists(_.`type` == TypeName.get(classOf[UnknownTrailingFieldPolicy])))
    fromJsonWithPolicy shouldBe defined
    fromJsonWithPolicy.get.returnType shouldEqual ParameterizedTypeName.get(
      className,
      TypeVariableName.get("T"),
    )
  }

  it should "throw JsonLfDecoder.Error" in {
    val methods = FromJsonGenerator.forRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
    )

    val fromJsonWithPolicy = methods.find(_.parameters.asScala.exists(_.`type` == TypeName.get(classOf[UnknownTrailingFieldPolicy])))
    fromJsonWithPolicy shouldBe defined
    fromJsonWithPolicy.get.exceptions should contain only TypeName.get(classOf[JsonLfDecoder.Error])
  }

  it should "call jsonDecoder().decode() with policy parameter" in {
    val methods = FromJsonGenerator.forRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
    )

    val fromJsonWithPolicy = methods.find(_.parameters.asScala.exists(_.`type` == TypeName.get(classOf[UnknownTrailingFieldPolicy])))
    fromJsonWithPolicy shouldBe defined

    val code = fromJsonWithPolicy.get.code.toString
    code should include("jsonDecoder().decode(new JsonLfReader(json), policy)")
  }

  it should "include type variables for parameterized types" in {
    val methods = FromJsonGenerator.forRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq("T", "U"),
    )

    val fromJsonWithPolicy = methods.find(_.parameters.asScala.exists(_.`type` == TypeName.get(classOf[UnknownTrailingFieldPolicy])))
    fromJsonWithPolicy shouldBe defined
    fromJsonWithPolicy.get.typeVariables.asScala.map(_.name) should contain.inOrderOnly("T", "U")
  }

  behavior of "FromJsonGenerator.forRecordLike"

  it should "generate three methods: jsonDecoder, fromJson, and fromJson with policy" in {
    val methods = FromJsonGenerator.forRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
    )

    methods should have size 3
    methods.map(_.name) should contain.only("jsonDecoder", "fromJson")
    methods.count(_.name == "fromJson") shouldBe 2
  }

  it should "generate public static jsonDecoder method" in {
    val methods = FromJsonGenerator.forRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
    )

    val jsonDecoder = methods.find(_.name == "jsonDecoder")
    jsonDecoder shouldBe defined
    jsonDecoder.get.modifiers.asScala should contain.only(Modifier.PUBLIC, Modifier.STATIC)
  }

  it should "return JsonLfDecoder for the class" in {
    val className = ClassName.bestGuess("TestClass")
    val methods = FromJsonGenerator.forRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      className,
      IndexedSeq.empty,
    )

    val jsonDecoder = methods.find(_.name == "jsonDecoder")
    jsonDecoder shouldBe defined
    jsonDecoder.get.returnType shouldEqual ParameterizedTypeName.get(
      ClassName.get(classOf[JsonLfDecoder[_]]),
      className,
    )
  }

  it should "generate decoder for simple fields" in {
    val fields = getFieldsWithTypes(
      ImmArraySeq(
        Ref.Name.assertFromString("boolField") -> TypePrim(PrimTypeBool, ImmArraySeq.empty),
        Ref.Name.assertFromString("textField") -> TypePrim(PrimTypeText, ImmArraySeq.empty),
      )
    )

    val methods = FromJsonGenerator.forRecordLike(
      fields,
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
    )

    val jsonDecoder = methods.find(_.name == "jsonDecoder")
    jsonDecoder shouldBe defined

    val code = jsonDecoder.get.code.toString
    code should include("JsonLfDecoders.record")
    code should include("\"boolField\"")
    code should include("\"textField\"")
  }

  it should "handle optional fields with Optional.empty() default" in {
    val fields = getFieldsWithTypes(
      ImmArraySeq(
        Ref.Name.assertFromString("requiredField") -> TypePrim(PrimTypeText, ImmArraySeq.empty),
        Ref.Name.assertFromString("optionalField") -> TypePrim(
          PrimTypeOptional,
          ImmArraySeq(TypePrim(PrimTypeText, ImmArraySeq.empty)),
        ),
      )
    )

    val methods = FromJsonGenerator.forRecordLike(
      fields,
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
    )

    val jsonDecoder = methods.find(_.name == "jsonDecoder")
    jsonDecoder shouldBe defined

    val code = jsonDecoder.get.code.toString
    code should include("java.util.Optional.empty()")
  }

  it should "generate constructor lambda with cast arguments" in {
    val fields = getFieldsWithTypes(
      ImmArraySeq(
        Ref.Name.assertFromString("field1") -> TypePrim(PrimTypeBool, ImmArraySeq.empty),
        Ref.Name.assertFromString("field2") -> TypePrim(PrimTypeText, ImmArraySeq.empty),
      )
    )

    val methods = FromJsonGenerator.forRecordLike(
      fields,
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
    )

    val jsonDecoder = methods.find(_.name == "jsonDecoder")
    jsonDecoder shouldBe defined

    val code = jsonDecoder.get.code.toString
    code should include("Object[] args")
    code should include("new TestClass")
    code should include("JsonLfDecoders.cast(args[0])")
    code should include("JsonLfDecoders.cast(args[1])")
  }

  it should "use Arrays.asList for field names" in {
    val fields = getFieldsWithTypes(
      ImmArraySeq(
        Ref.Name.assertFromString("field1") -> TypePrim(PrimTypeBool, ImmArraySeq.empty),
        Ref.Name.assertFromString("field2") -> TypePrim(PrimTypeText, ImmArraySeq.empty),
      )
    )

    val methods = FromJsonGenerator.forRecordLike(
      fields,
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
    )

    val jsonDecoder = methods.find(_.name == "jsonDecoder")
    jsonDecoder shouldBe defined

    val code = jsonDecoder.get.code.toString
    code should include("Arrays.asList")
  }

  it should "handle parameterized types with decoder parameters" in {
    val methods = FromJsonGenerator.forRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq("T", "U"),
    )

    val jsonDecoder = methods.find(_.name == "jsonDecoder")
    jsonDecoder shouldBe defined

    val params = jsonDecoder.get.parameters.asScala
    params should have size 2
    params.map(_.name) should contain.inOrderOnly("decodeT", "decodeU")
  }

  it should "generate fromJson method without policy" in {
    val methods = FromJsonGenerator.forRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
    )

    val fromJsonMethods = methods.filter(_.name == "fromJson")
    fromJsonMethods should have size 2

    val fromJsonWithoutPolicy = fromJsonMethods.find(!_.parameters.asScala.exists(_.`type` == TypeName.get(classOf[UnknownTrailingFieldPolicy])))
    fromJsonWithoutPolicy shouldBe defined
    fromJsonWithoutPolicy.get.parameters.asScala.map(_.name) should contain only "json"
  }

  it should "handle empty records" in {
    val methods = FromJsonGenerator.forRecordLike(
      getFieldsWithTypes(ImmArraySeq.empty),
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
    )

    val jsonDecoder = methods.find(_.name == "jsonDecoder")
    jsonDecoder shouldBe defined

    val code = jsonDecoder.get.code.toString
    code should include("JsonLfDecoders.record")
    code should include("new TestClass()")
  }

  it should "handle complex types like lists" in {
    val fields = getFieldsWithTypes(
      ImmArraySeq(
        Ref.Name.assertFromString("listField") -> TypePrim(
          PrimTypeList,
          ImmArraySeq(TypePrim(PrimTypeText, ImmArraySeq.empty)),
        )
      )
    )

    val methods = FromJsonGenerator.forRecordLike(
      fields,
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
    )

    val jsonDecoder = methods.find(_.name == "jsonDecoder")
    jsonDecoder shouldBe defined

    val code = jsonDecoder.get.code.toString
    code should include("\"listField\"")
  }

  it should "use switch statement for field name mapping" in {
    val fields = getFieldsWithTypes(
      ImmArraySeq(
        Ref.Name.assertFromString("field1") -> TypePrim(PrimTypeBool, ImmArraySeq.empty),
        Ref.Name.assertFromString("field2") -> TypePrim(PrimTypeText, ImmArraySeq.empty),
      )
    )

    val methods = FromJsonGenerator.forRecordLike(
      fields,
      ClassName.bestGuess("TestClass"),
      IndexedSeq.empty,
    )

    val jsonDecoder = methods.find(_.name == "jsonDecoder")
    jsonDecoder shouldBe defined

    val code = jsonDecoder.get.code.toString
    code should include("switch (name)")
    code should include("case \"field1\":")
    code should include("case \"field2\":")
    code should include("default: return null")
  }
}

