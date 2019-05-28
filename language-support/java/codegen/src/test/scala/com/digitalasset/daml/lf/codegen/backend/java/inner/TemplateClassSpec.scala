// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java.inner

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

  it should "generate a method taking exactly a template identifier and a record" in {
    val parameters = fromIdAndRecord.parameters.asScala.map(p => p.name -> p.`type`)
    parameters should contain only ("contractId" -> string, "record$" -> record, "agreementText" -> optionalString)
  }

  private[this] val className = ClassName.bestGuess("Test")
  private[this] val templateClassName = ClassName.bestGuess("Template")
  private[this] val idClassName = ClassName.bestGuess("Id")
  private[this] val fromIdAndRecord =
    TemplateClass.generateFromIdAndRecord(className, templateClassName, idClassName)
  private[this] val string = TypeName.get(classOf[String])
  private[this] val record = TypeName.get(classOf[javaapi.data.Record])
  private[this] val optionalString =
    ParameterizedTypeName.get(classOf[Optional[_]], classOf[String])

}
