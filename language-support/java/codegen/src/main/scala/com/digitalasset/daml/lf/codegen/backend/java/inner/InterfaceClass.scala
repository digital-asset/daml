// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.lf.codegen.backend.java.JavaEscaper
import com.daml.lf.data.Ref.{PackageId, QualifiedName}
import com.daml.lf.iface.{DefInterface}
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging

import javax.lang.model.element.Modifier

object InterfaceClass extends StrictLogging {

  def classNameForInterface(qualifiedName: QualifiedName) =
    ClassName.bestGuess {
      val QualifiedName(module, name) = qualifiedName
      // consider all but the last name segment to be part of the java package name
      val packageSegments = module.segments.slowAppend(name.segments).toSeq.dropRight(1)
      // consider the last name segment to be the java class name
      val className = name.segments.toSeq.takeRight(1)

      val packageName = packageSegments.map(_.toLowerCase)

      (packageName ++ className)
        .filter(_.nonEmpty)
        .map(JavaEscaper.escapeString)
        .mkString(".")
    }

  def generate(
      interfaceName: ClassName,
      interface: DefInterface.FWT,
      packagePrefixes: Map[PackageId, String],
      packageId: PackageId,
      interfaceId: QualifiedName,
  ): TypeSpec =
    TrackLineage.of("interface", interfaceName.simpleName()) {
      logger.info("Start")

      val templateType = TypeSpec
        .classBuilder(interfaceName)
        .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
        .addField(generateTemplateIdField(packageId, interfaceId))
        .addType(
          IdClass
            .builder(
              interfaceName,
              interface.choices,
              packagePrefixes,
            )
            .build()
        )
        .addType(
          ContractIdClass
            .builder(interfaceName, None, packagePrefixes)
            .build()
        )
        .build()
      logger.debug("End")
      templateType
    }

  private def generateTemplateIdField(
      packageId: PackageId,
      name: QualifiedName,
  ): FieldSpec = {
    FieldSpec
      .builder(
        ClassName.get(classOf[javaapi.data.Identifier]),
        "TEMPLATE_ID",
        Modifier.STATIC,
        Modifier.FINAL,
        Modifier.PUBLIC,
      )
      .initializer(
        "new $T($S, $S, $S)",
        classOf[javaapi.data.Identifier],
        packageId,
        name.module.toString,
        name.name,
      )
      .build()
  }
}
