// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.codegen.ValueDecoder
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.typesig.{PrimTypeBool, TypePrim}
import com.squareup.javapoet._

import javax.lang.model.element.Modifier
import org.scalatest.{OptionValues, TryValues}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

final class RecordLikeMethodsSpec
    extends AnyFlatSpec
    with Matchers
    with OptionValues
    with TryValues {

  behavior of "RecordMethods.constructor"

  it should "be a constructor" in {
    constructor should be a Symbol("constructor")
  }

  it should "be public" in {
    constructor.modifiers.asScala should contain only Modifier.PUBLIC
  }

  it should "take the expected parameter list" in {
    constructor.parameters should contain only ParameterSpec
      .builder(classOf[java.lang.Boolean], "bool")
      .build()
  }

  it should "not declare any checked exception" in {
    constructor.exceptions shouldBe empty
  }

  it should "not declare any annotation" in {
    constructor.annotations shouldBe empty
  }

  behavior of "RecordMethods.staticImports"

  import com.daml.ledger.javaapi.data.codegen.json.JsonLfEncoders

  it should "have the apply method" in {
    staticImports shouldBe Seq((ClassName.get(classOf[JsonLfEncoders]), "apply"))
  }

  behavior of "RecordMethods.toValueSpec"

  it should "be correctly named" in {
    toValue.name shouldBe "toValue"
  }

  it should "be public" in {
    toValue.modifiers.asScala should contain only Modifier.PUBLIC
  }

  it should "return a DamlRecord" in {
    toValue.returnType shouldEqual ClassName.get(classOf[javaapi.data.DamlRecord])
  }

  it should "take no parameter" in {
    toValue.parameters shouldBe empty
  }

  it should "not declare any checked exception" in {
    toValue.exceptions shouldBe empty
  }

  it should "not declare any annotation" in {
    toValue.annotations shouldBe empty
  }

  behavior of "RecordMethods.valueDecoderSpec"

  it should "be correctly named" in {
    valueDecoder.name shouldBe "valueDecoder"
  }

  it should "be public static" in {
    valueDecoder.modifiers.asScala should contain.only(Modifier.STATIC, Modifier.PUBLIC)
  }

  it should "return the outer class" in {
    valueDecoder.returnType shouldEqual ParameterizedTypeName.get(
      ClassName.get(classOf[ValueDecoder[_]]),
      name,
    )
  }

  it should "take no parameter" in {
    val parameters = valueDecoder.parameters.asScala.map(p => p.name -> p.`type`)
    parameters shouldBe empty
  }

  it should "throw an IllegalArgumentException" in {
    valueDecoder.exceptions should contain only TypeName.get(classOf[IllegalArgumentException])
  }

  it should "not declare any annotation" in {
    valueDecoder.annotations shouldBe empty
  }

  private val name = ClassName.bestGuess("Test")
  private val (methods, staticImports) = {
    implicit val packagePrefixes: PackagePrefixes = PackagePrefixes(Map.empty)
    RecordMethods(
      getFieldsWithTypes(
        ImmArraySeq(Ref.Name.assertFromString("bool") -> TypePrim(PrimTypeBool, ImmArraySeq.empty))
      ),
      name,
      IndexedSeq.empty,
    )
  }
  private val Vector(constructor, valueDecoder, toValue) = methods.take(3)

}
