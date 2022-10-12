// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi.data.codegen.InterfaceCompanion
import com.daml.lf.codegen.backend.java.inner.TemplateClass.toChoiceNameField
import com.daml.lf.data.Ref.{ChoiceName, PackageId, QualifiedName}
import com.daml.lf.typesig
import typesig.DefInterface
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging
import scalaz.-\/

import java.util
import javax.lang.model.element.Modifier
import scala.jdk.CollectionConverters._

object InterfaceClass extends StrictLogging {

  def generate(
      interfaceName: ClassName,
      interfaceViewTypeName: ClassName,
      interface: DefInterface.FWT,
      packagePrefixes: Map[PackageId, String],
      typeDeclarations: Map[QualifiedName, typesig.PackageSignature.TypeDecl],
      packageId: PackageId,
      interfaceId: QualifiedName,
  ): TypeSpec =
    TrackLineage.of("interface", interfaceName.simpleName()) {
      logger.info("Start")
      val interfaceType = TypeSpec
        .classBuilder(interfaceName)
        .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
        .addField(generateTemplateIdField(packageId, interfaceId))
        .addFields(
          TemplateClass
            .generateChoicesMetadata(
              interfaceName,
              packagePrefixes,
              interface.choices,
              withPrefixes = false, // TODO: remove in #15154
            )
            .asJava
        )
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
          TemplateClass.generateCreateAndClass(interfaceName, -\/(ContractIdClass.For.Interface))
        )
        .addType(
          TemplateClass.generateByKeyClass(interfaceName, -\/(ContractIdClass.For.Interface))
        )
        .addType(
          generateInterfaceCompanionClass(
            interfaceName = interfaceName,
            choiceNames = interface.choices.keySet,
            interfaceViewTypeName = interfaceViewTypeName,
          )
        )
        .addMethod {
          // interface classes are not inhabited
          MethodSpec.constructorBuilder().addModifiers(Modifier.PRIVATE).build()
        }
        .build()
      logger.debug("End")
      interfaceType
    }

  private[inner] val companionName = "INTERFACE"
  private[inner] val companionClassName = "INTERFACE_"

  private def generateInterfaceCompanionField(): FieldSpec =
    FieldSpec
      .builder(
        ClassName bestGuess companionClassName,
        companionName,
        Modifier.FINAL,
        Modifier.PUBLIC,
        Modifier.STATIC,
      )
      .initializer("new $N()", companionClassName)
      .build()

  private def generateInterfaceCompanionClass(
      interfaceName: ClassName,
      choiceNames: Set[ChoiceName],
      interfaceViewTypeName: ClassName,
  ): TypeSpec = {
    val contractIdClassName = ClassName bestGuess "ContractId"
    TypeSpec
      .classBuilder(companionClassName)
      .superclass(
        ParameterizedTypeName
          .get(
            ClassName get classOf[InterfaceCompanion[_, _, _]],
            interfaceName,
            contractIdClassName,
            interfaceViewTypeName,
          )
      )
      .addModifiers(Modifier.FINAL, Modifier.PUBLIC, Modifier.STATIC)
      .addMethod {
        MethodSpec
          .constructorBuilder()
          // intentionally package-private
          .addStatement(
            "super($>$Z$S, $T.$N, $T::new, $T.$L(), $T.asList($L))$<$Z",
            interfaceName,
            interfaceName,
            ClassGenUtils.templateIdFieldName,
            contractIdClassName,
            interfaceViewTypeName,
            "valueDecoder",
            classOf[util.Arrays],
            CodeBlock
              .join(
                choiceNames
                  .map(choiceName => CodeBlock.of("$N", toChoiceNameField(choiceName)))
                  .asJava,
                ",$W",
              ),
          )
          .build()
      }
      .build()
  }

  private def generateTemplateIdField(packageId: PackageId, name: QualifiedName): FieldSpec =
    ClassGenUtils.generateTemplateIdField(
      packageId,
      name.module.toString,
      name.name.toString,
    )
}
