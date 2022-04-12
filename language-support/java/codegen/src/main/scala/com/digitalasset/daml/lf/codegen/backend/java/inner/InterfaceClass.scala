// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

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
        .addType(
          ContractIdClass
            .builder(
              interfaceName,
              interface.choices,
              packagePrefixes,
            )
            .build()
        )
        .build()
      logger.debug("End")
      interfaceType
    }

  private def generateTemplateIdField(packageId: PackageId, name: QualifiedName): FieldSpec =
    ClassGenUtils.generateTemplateIdField(
      packageId,
      name.module.toString,
      name.name.toString,
    )
}
