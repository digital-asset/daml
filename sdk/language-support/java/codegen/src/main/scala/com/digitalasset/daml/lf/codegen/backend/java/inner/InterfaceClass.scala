// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi.data.ContractFilter
import com.daml.ledger.javaapi.data.codegen.{Contract, InterfaceCompanion, ContractTypeCompanion}
import com.digitalasset.daml.lf.codegen.NodeWithContext.AuxiliarySignatures
import com.digitalasset.daml.lf.codegen.backend.java.inner.TemplateClass.toChoiceNameField
import com.digitalasset.daml.lf.data.Ref.{ChoiceName, PackageId, PackageName, QualifiedName}
import com.digitalasset.daml.lf.typesig.{DefInterface, PackageMetadata}
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging
import scalaz.-\/

import javax.lang.model.element.Modifier
import scala.jdk.CollectionConverters._

object InterfaceClass extends StrictLogging {

  def generate(
      interfaceName: ClassName,
      interfaceViewTypeName: ClassName,
      interface: DefInterface.FWT,
      typeDeclarations: AuxiliarySignatures,
      packageId: PackageId,
      interfaceId: QualifiedName,
      packageMetadata: PackageMetadata,
  )(implicit packagePrefixes: PackagePrefixes): TypeSpec =
    TrackLineage.of("interface", interfaceName.simpleName()) {
      logger.info("Start")
      val interfaceType = TypeSpec
        .classBuilder(interfaceName)
        .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
        .addFields(generateTemplateIdFields(packageId, packageMetadata.name, interfaceId).asJava)
        .addFields(generateInterfaceIdFields(packageId, packageMetadata.name, interfaceId).asJava)
        .addField(ClassGenUtils.generatePackageIdField(packageId))
        .addField(ClassGenUtils.generatePackageNameField(packageMetadata.name))
        .addField(ClassGenUtils.generatePackageVersionField(packageMetadata.version))
        .addFields(
          TemplateClass
            .generateChoicesMetadata(
              interfaceName,
              interface.choices,
            )
            .asJava
        )
        .addMethod(generateContractFilterMethod(interfaceName, interfaceViewTypeName))
        .addField(generateInterfaceCompanionField())
        .addType(
          ContractIdClass
            .builder(
              interfaceName,
              interface.choices,
              ContractIdClass.For.Interface,
            )
            .build()
        )
        .addType(
          ContractIdClass.generateExercisesInterface(
            interfaceName,
            interface.choices,
            typeDeclarations,
          )
        )
        .addType(
          TemplateClass.generateCreateAndClass(
            interfaceName,
            -\/(ContractIdClass.For.Interface),
          )
        )
        .addType(
          TemplateClass
            .generateByKeyClass(interfaceName, -\/(ContractIdClass.For.Interface))
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
    val contractIdClassName = nestedClassName(interfaceName, "ContractId")
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
            "super(new $T($T.$N, $T.$N, $T.$N),$>$Z$S, $T.$N, $T::new, $T.$L(),$W$T::fromJson,$T.of($L))$<$Z",
            nestedClassName(ClassName.get(classOf[ContractTypeCompanion[_, _, _, _]]), "Package"),
            interfaceName,
            ClassGenUtils.packageIdFieldName,
            interfaceName,
            ClassGenUtils.packageNameFieldName,
            interfaceName,
            ClassGenUtils.packageVersionFieldName,
            interfaceName,
            interfaceName,
            ClassGenUtils.templateIdFieldName,
            contractIdClassName,
            interfaceViewTypeName,
            "valueDecoder",
            interfaceViewTypeName,
            classOf[java.util.List[_]],
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

  private def generateTemplateIdFields(
      packageId: PackageId,
      pkgName: PackageName,
      name: QualifiedName,
  ): Seq[FieldSpec] =
    ClassGenUtils.generateTemplateIdFields(
      pkgId = packageId,
      pkgName = pkgName,
      moduleName = name.module.toString,
      name = name.name.toString,
    )

  private def generateInterfaceIdFields(
      packageId: PackageId,
      pkgName: PackageName,
      name: QualifiedName,
  ): Seq[FieldSpec] =
    ClassGenUtils.generateInterfaceIdFields(
      pkgId = packageId,
      pkgName = pkgName,
      moduleName = name.module.toString,
      name = name.name.toString,
    )

  private def generateContractFilterMethod(
      interfaceName: ClassName,
      interfaceViewTypeName: ClassName,
  ): MethodSpec =
    MethodSpec
      .methodBuilder("contractFilter")
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .returns(
        ParameterizedTypeName.get(
          ClassName.get(classOf[ContractFilter[_]]),
          ParameterizedTypeName.get(
            ClassName.get(classOf[Contract[_, _]]),
            nestedClassName(interfaceName, "ContractId"),
            interfaceViewTypeName,
          ),
        )
      )
      .addStatement("return $T.of(INTERFACE)", classOf[ContractFilter[_]])
      .build()
}
