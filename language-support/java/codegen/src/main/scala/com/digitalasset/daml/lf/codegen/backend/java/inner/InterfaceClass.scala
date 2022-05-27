// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi.data.codegen.InterfaceCompanion
import com.daml.lf.data.Ref.{PackageId, QualifiedName}
import com.daml.lf.iface.DefInterface
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging

import javax.lang.model.element.Modifier

object InterfaceClass extends StrictLogging {

  def generate(
      interfaceName: ClassName,
      interface: DefInterface.FWT,
      packagePrefixes: Map[PackageId, String],
      packageId: PackageId,
      interfaceId: QualifiedName,
  ): TypeSpec =
    TrackLineage.of("interface", interfaceName.simpleName()) {
      logger.info("Start")
      val interfaceType = TypeSpec
        .classBuilder(interfaceName)
        .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
        .addField(generateTemplateIdField(packageId, interfaceId))
        .addField(generateInterfaceCompanionField())
        .addType(
          ContractIdClass
            .builder(
              interfaceName,
              interface.choices,
              packagePrefixes,
            )
            .build()
        )
        .addType(generateInterfaceCompanionClass(interfaceName = interfaceName))
        .build()
      logger.debug("End")
      interfaceType
    }

  private val companionName = "INTERFACE"

  private def generateInterfaceCompanionField(): FieldSpec =
    FieldSpec
      .builder(
        ClassName bestGuess companionName,
        companionName,
        Modifier.FINAL,
        Modifier.PUBLIC,
        Modifier.STATIC,
      )
      .initializer("new $N()", companionName)
      .build()

  private def generateInterfaceCompanionClass(interfaceName: ClassName): TypeSpec = TypeSpec
    .classBuilder(companionName)
    .superclass(
      ParameterizedTypeName
        .get(ClassName get classOf[InterfaceCompanion[_]], interfaceName)
    )
    .addModifiers(Modifier.FINAL, Modifier.PUBLIC, Modifier.STATIC)
    .addMethod {
      // we define this explicitly to make it package-private
      MethodSpec.constructorBuilder().build()
    }
    .build()

  private def generateTemplateIdField(packageId: PackageId, name: QualifiedName): FieldSpec =
    ClassGenUtils.generateTemplateIdField(
      packageId,
      name.module.toString,
      name.name.toString,
    )
}
