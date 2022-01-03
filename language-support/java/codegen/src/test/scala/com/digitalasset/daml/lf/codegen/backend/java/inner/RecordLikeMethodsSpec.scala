// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref
import com.daml.lf.iface.{PrimTypeBool, TypePrim}
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

  behavior of "RecordMethods.fromValueSpec"

  it should "be correctly named" in {
    fromValue.name shouldBe "fromValue"
  }

  it should "be public static" in {
    fromValue.modifiers.asScala should contain.only(Modifier.STATIC, Modifier.PUBLIC)
  }

  it should "return the outer class" in {
    fromValue.returnType shouldEqual name
  }

  it should "take a single parameter 'value$' of type Value" in {
    val parameters = fromValue.parameters.asScala.map(p => p.name -> p.`type`)
    parameters should contain only "value$" -> TypeName.get(classOf[javaapi.data.Value])
  }

  it should "throw an IllegalArgumentException" in {
    fromValue.exceptions should contain only TypeName.get(classOf[IllegalArgumentException])
  }

  it should "not declare any annotation" in {
    fromValue.annotations shouldBe empty
  }

  private val name = ClassName.bestGuess("Test")
  private val methods = RecordMethods(
    getFieldsWithTypes(
      ImmArraySeq(Ref.Name.assertFromString("bool") -> TypePrim(PrimTypeBool, ImmArraySeq.empty)),
      Map(),
    ),
    name,
    IndexedSeq.empty,
    Map(),
  )
  private val Vector(constructor, fromValue, toValue) = methods.take(3)

}
