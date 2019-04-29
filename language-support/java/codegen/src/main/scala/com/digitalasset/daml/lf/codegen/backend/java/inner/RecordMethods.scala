// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.digitalasset.daml.lf.codegen.backend.java.ObjectMethods
import com.digitalasset.daml.lf.iface.PackageId
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging

private[inner] object RecordMethods extends StrictLogging {

  def apply(
      fields: Fields,
      className: ClassName,
      typeParameters: IndexedSeq[String],
      packagePrefixes: Map[PackageId, String]): Vector[MethodSpec] = {

    val constructor = ConstructorGenerator.generateConstructor(fields)

    val actualTypeParameters = findTypeParamsInFields(fields)

    val conversionMethods = Vector(actualTypeParameters, typeParameters).distinct.flatMap {
      params =>
        val fromValue = FromValueGenerator.generateFromValueForRecordLike(
          fields,
          className.parameterized(typeParameters),
          params,
          (inVar, outVar) =>
            CodeBlock.builder
              .addStatement(
                "$T $L = $L",
                classOf[javaapi.data.Value],
                outVar,
                inVar
              )
              .build(),
          packagePrefixes
        )
        val toValue = ToValueGenerator.generateToValueForRecordLike(
          params,
          fields,
          packagePrefixes,
          ClassName.get(classOf[javaapi.data.Record]),
          name => CodeBlock.of("return new $T($L)", classOf[javaapi.data.Record], name)
        )
        List(fromValue, toValue)
    }

    Vector(constructor) ++ conversionMethods ++
      ObjectMethods(className, fields.map(_.javaName))
  }
}
