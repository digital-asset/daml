// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import java.util.Optional

import com.daml.ledger.javaapi
import com.squareup.javapoet.{ClassName, ParameterizedTypeName, TypeName}
import javax.lang.model.element.Modifier
import org.scalatest.{OptionValues, TryValues}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

final class ContractClassBuilderSpec
    extends AnyFlatSpec
    with Matchers
    with OptionValues
    with TryValues {

  behavior of "ContractIdClass.Builder.generateFromIdAndRecord"

  it should "generate a public static method" in {
    fromIdAndRecord.modifiers.asScala should contain.only(Modifier.STATIC, Modifier.PUBLIC)
  }

  it should "generate a method returning the class itself" in {
    fromIdAndRecord.returnType shouldEqual className
  }

  it should "generate a method taking the expected parameters (with contract key)" in {
    val parameters = fromIdAndRecord.parameters.asScala.map(p => p.name -> p.`type`).toList
    parameters should contain theSameElementsInOrderAs Seq(
      "contractId" -> string,
      "record$" -> record,
      "agreementText" -> optionalString, // will be removed
      "key" -> optionalContractKey,
      "signatories" -> setOfStrings,
      "observers" -> setOfStrings,
    )
  }

  it should "generate a method taking the expected parameters (without contract key)" in {
    val parameters =
      fromIdAndRecordWithoutKey.parameters.asScala.map(p => p.name -> p.`type`).toList
    parameters should contain theSameElementsInOrderAs Seq(
      "contractId" -> string,
      "record$" -> record,
      "agreementText" -> optionalString, // will be removed
      "signatories" -> setOfStrings,
      "observers" -> setOfStrings,
    )
  }

  private[this] val className = ClassName.bestGuess("Test")
  private[this] val ckClassName = ClassName.bestGuess("Ck")
  private[this] val fromIdAndRecord =
    ContractClass.Builder.generateFromIdAndRecord(
      className,
      Some(ckClassName),
    )
  private[this] val fromIdAndRecordWithoutKey =
    ContractClass.Builder.generateFromIdAndRecord(className, None)
  private[this] val string = TypeName.get(classOf[String])
  private[this] val record = TypeName.get(classOf[javaapi.data.DamlRecord])
  private[this] val optionalString =
    ParameterizedTypeName.get(classOf[Optional[_]], classOf[String])
  private[this] val optionalContractKey =
    ParameterizedTypeName.get(ClassName.get(classOf[Optional[_]]), ckClassName)
  private[this] val setOfStrings =
    ParameterizedTypeName.get(classOf[java.util.Set[_]], classOf[String])

}
