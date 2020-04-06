// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import java.util.Optional

import com.daml.ledger.javaapi
import com.squareup.javapoet.{ClassName, ParameterizedTypeName, TypeName}
import javax.lang.model.element.Modifier
import org.scalatest.{FlatSpec, Matchers, OptionValues, TryValues}

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

@SuppressWarnings(Array("org.wartremover.warts.Any"))
final class TemplateClassSpec extends FlatSpec with Matchers with OptionValues with TryValues {

  behavior of "TemplateClass.generateFromIdAndRecord"

  it should "generate a public static method" in {
    fromIdAndRecord.modifiers.asScala should contain only (Modifier.STATIC, Modifier.PUBLIC)
  }

  it should "generate a method returning the class itself" in {
    fromIdAndRecord.returnType shouldEqual className
  }

  it should "generate a method taking the expected parameters (with contract key)" in {
    val parameters = fromIdAndRecord.parameters.asScala.map(p => p.name -> p.`type`).toList
    parameters should contain theSameElementsInOrderAs Seq(
      "contractId" -> string,
      "record$" -> record,
      "agreementText" -> optionalString,
      "key" -> optionalContractKey,
      "signatories" -> setOfStrings,
      "observers" -> setOfStrings
    )
  }

  it should "generate a method taking the expected parameters (without contract key)" in {
    val parameters =
      fromIdAndRecordWithoutKey.parameters.asScala.map(p => p.name -> p.`type`).toList
    parameters should contain theSameElementsInOrderAs Seq(
      "contractId" -> string,
      "record$" -> record,
      "agreementText" -> optionalString,
      "signatories" -> setOfStrings,
      "observers" -> setOfStrings)
  }

  private[this] val className = ClassName.bestGuess("Test")
  private[this] val templateClassName = ClassName.bestGuess("Template")
  private[this] val idClassName = ClassName.bestGuess("Id")
  private[this] val ckClassName = ClassName.bestGuess("Ck")
  private[this] val fromIdAndRecord =
    TemplateClass.generateFromIdAndRecord(
      className,
      templateClassName,
      idClassName,
      Some(ckClassName))
  private[this] val fromIdAndRecordWithoutKey =
    TemplateClass.generateFromIdAndRecord(className, templateClassName, idClassName, None)
  private[this] val string = TypeName.get(classOf[String])
  private[this] val record = TypeName.get(classOf[javaapi.data.Record])
  private[this] val optionalString =
    ParameterizedTypeName.get(classOf[Optional[_]], classOf[String])
  private[this] val optionalContractKey =
    ParameterizedTypeName.get(ClassName.get(classOf[Optional[_]]), ckClassName)
  private[this] val setOfStrings =
    ParameterizedTypeName.get(classOf[java.util.Set[_]], classOf[String])

}
