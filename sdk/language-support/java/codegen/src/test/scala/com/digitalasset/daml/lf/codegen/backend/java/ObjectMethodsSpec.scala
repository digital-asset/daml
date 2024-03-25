// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java

import com.squareup.javapoet.{ClassName, TypeName}
import javax.lang.model.element.Modifier
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import scala.jdk.CollectionConverters._

final class ObjectMethodsSpec extends AnyFlatSpec with Matchers {

  val Vector(equalsSpec, hashCodeSpec, toStringSpec) =
    ObjectMethods(ClassName.bestGuess("Test"), IndexedSeq.empty, IndexedSeq.empty)

  behavior of "ObjectMethods.equals"

  it should "generate 'equals' with the @Override annotation exclusively" in {
    equalsSpec.annotations should contain only Annotation.`override`
  }

  it should "generate 'equals' specifying no checked exception" in {
    equalsSpec.exceptions shouldBe empty
  }

  it should "generate 'equals' with the public modifier exclusively" in {
    equalsSpec.modifiers should contain only Modifier.PUBLIC
  }

  it should "generate 'equals' with the correct name" in {
    equalsSpec.name shouldEqual "equals"
  }

  it should "generate 'equals' with a single parameter 'object' of type Object" in {
    equalsSpec.parameters.asScala
      .map(p => p.name -> p.`type`) should contain only "object" -> ClassName.get(
      classOf[java.lang.Object]
    )
  }

  it should "generate 'equals' returning a boolean" in {
    equalsSpec.returnType shouldBe TypeName.BOOLEAN
  }

  it should "generate 'equals' without any type variable" in {
    equalsSpec.typeVariables shouldBe empty
  }

  behavior of "ObjectMethods.hashCode"

  it should "generate 'hashCode' with the @Override annotation exclusively" in {
    hashCodeSpec.annotations should contain only Annotation.`override`
  }

  it should "generate 'hashCode' specifying no checked exception" in {
    hashCodeSpec.exceptions shouldBe empty
  }

  it should "generate 'hashCode' with the public modifier exclusively" in {
    hashCodeSpec.modifiers should contain only Modifier.PUBLIC
  }

  it should "generate 'hashCode' with the correct name" in {
    hashCodeSpec.name shouldEqual "hashCode"
  }

  it should "generate 'hashCode' with no parameter" in {
    hashCodeSpec.parameters shouldBe empty
  }

  it should "generate 'hashCode' returning an int" in {
    hashCodeSpec.returnType shouldBe TypeName.INT
  }

  it should "generate 'hashCode' without any type variable" in {
    hashCodeSpec.typeVariables shouldBe empty
  }

  behavior of "ObjectMethods.toString"

  it should "generate 'toString' with the @Override annotation exclusively" in {
    toStringSpec.annotations should contain only Annotation.`override`
  }

  it should "generate 'toString' specifying no checked exception" in {
    toStringSpec.exceptions shouldBe empty
  }

  it should "generate 'toString' with the public modifier exclusively" in {
    toStringSpec.modifiers should contain only Modifier.PUBLIC
  }

  it should "generate 'toString' with the correct name" in {
    toStringSpec.name shouldEqual "toString"
  }

  it should "generate 'toString' with no parameter" in {
    toStringSpec.parameters shouldBe empty
  }

  it should "generate 'toString' returning a String" in {
    toStringSpec.returnType shouldBe ClassName.get(classOf[String])
  }

  it should "generate 'toString' without any type variable" in {
    toStringSpec.typeVariables shouldBe empty
  }

}
