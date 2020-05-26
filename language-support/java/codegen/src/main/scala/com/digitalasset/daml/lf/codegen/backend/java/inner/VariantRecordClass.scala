// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.lf.data.Ref.PackageId
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging
import javax.lang.model.element.Modifier

import scala.collection.JavaConverters._

private[inner] object VariantRecordClass extends StrictLogging {

  def generate(
      typeParameters: IndexedSeq[String],
      fields: Fields,
      name: String,
      superclass: TypeName,
      packagePrefixes: Map[PackageId, String]): TypeSpec.Builder =
    TrackLineage.of("variant-record", name) {
      logger.info("Start")
      val className = ClassName.bestGuess(name)
      val builder = TypeSpec
        .classBuilder(name)
        .addModifiers(Modifier.PUBLIC)
        .superclass(superclass)
        .addTypeVariables(typeParameters.map(TypeVariableName.get).asJava)
        .addFields(RecordFields(fields).asJava)
        .addMethods(
          VariantRecordMethods(
            name,
            fields,
            className.parameterized(typeParameters),
            typeParameters,
            packagePrefixes).asJava)
      logger.debug("End")
      builder
    }
}
