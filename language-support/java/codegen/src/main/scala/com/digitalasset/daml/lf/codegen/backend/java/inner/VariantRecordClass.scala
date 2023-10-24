// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.lf.data.Ref.PackageId
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging
import javax.lang.model.element.Modifier

import scala.jdk.CollectionConverters._

private[inner] object VariantRecordClass extends StrictLogging {

  def generate(
      packageId: PackageId,
      typeParameters: IndexedSeq[String],
      fields: Fields,
      name: String,
      superclass: TypeName,
  )(implicit
      packagePrefixes: PackagePrefixes
  ): (TypeSpec, Seq[(ClassName, String)]) =
    TrackLineage.of("variant-record", name) {
      logger.info("Start")
      val className = ClassName.bestGuess(name)
      val (methods, staticImports) = VariantRecordMethods(
        name,
        fields,
        superclass,
        className.parameterized(typeParameters),
        typeParameters,
      )

      val typeSpec = TypeSpec
        .classBuilder(name)
        .addModifiers(Modifier.PUBLIC)
        .superclass(superclass)
        .addTypeVariables(typeParameters.map(TypeVariableName.get).asJava)
        .addFields((createPackageIdField(packageId) +: RecordFields(fields)).asJava)
        .addMethods(methods.asJava)
        .build()
      logger.debug("End")
      (typeSpec, staticImports)
    }
}
