// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi.data.codegen.InterfaceCompanion
import com.daml.lf.data.Ref.{PackageId, QualifiedName}
import com.daml.lf.iface, iface.DefInterface
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging
import scalaz.-\/

import javax.lang.model.element.Modifier

object InterfaceClass extends StrictLogging {

  def generate(
      interfaceName: ClassName,
      interface: DefInterface.FWT,
      packagePrefixes: Map[PackageId, String],
      typeDeclarations: Map[QualifiedName, iface.InterfaceType],
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
              ContractIdClass.For.Interface,
              packagePrefixes,
            )
            .build()
        )
        .addType(
          ContractIdClass.generateExercisesInterface(
            interface.choices,
            typeDeclarations,
            packageId,
            packagePrefixes,
          )
        )
        .addType(
          TemplateClass.generateCreateAndClass(-\/(ContractIdClass.For.Interface))
        )
        .addType(TemplateClass.generateByKeyClass(-\/(ContractIdClass.For.Interface)))
        .addType(generateInterfaceCompanionClass(interfaceName = interfaceName))
        .addMethod {
          // interface classes are not inhabited
          MethodSpec.constructorBuilder().addModifiers(Modifier.PRIVATE).build()
        }
        .build()
      logger.debug("End")
      interfaceType
    }

  private[inner] val companionName = "INTERFACE"

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
      MethodSpec
        .constructorBuilder()
        // intentionally package-private
        .addStatement("super($T.$N)", interfaceName, ClassGenUtils.templateIdFieldName)
        .build()
    }
    .build()

  private def generateTemplateIdField(packageId: PackageId, name: QualifiedName): FieldSpec =
    ClassGenUtils.generateTemplateIdField(
      packageId,
      name.module.toString,
      name.name.toString,
    )
}
