// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.lf.codegen.backend.java.ObjectMethods
import com.daml.lf.data.Ref.PackageId
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging

private[inner] object VariantRecordMethods extends StrictLogging {

  def apply(
      constructorName: String,
      fields: Fields,
      className: TypeName,
      typeParameters: IndexedSeq[String],
      packagePrefixes: Map[PackageId, String]): Vector[MethodSpec] = {
    val constructor = ConstructorGenerator.generateConstructor(fields)

    val conversionMethods = distinctTypeVars(fields, typeParameters).flatMap { params =>
      val toValue = ToValueGenerator.generateToValueForRecordLike(
        params,
        fields,
        packagePrefixes,
        TypeName.get(classOf[javaapi.data.Variant]),
        name =>
          CodeBlock.of(
            "return new $T($S, new $T($L))",
            classOf[javaapi.data.Variant],
            constructorName,
            classOf[javaapi.data.Record],
            name)
      )
      val fromValue = FromValueGenerator.generateFromValueForRecordLike(
        fields,
        className,
        params,
        FromValueGenerator.variantCheck(constructorName, _, _),
        packagePrefixes)
      List(toValue, fromValue)
    }

    Vector(constructor) ++ conversionMethods ++
      ObjectMethods(className.rawType, typeParameters, fields.map(_.javaName))
  }

}
