// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.lf.codegen.backend.java.ObjectMethods
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging

private[inner] object VariantRecordMethods extends StrictLogging {

  def apply(
      constructorName: String,
      fields: Fields,
      className: TypeName,
      typeParameters: IndexedSeq[String],
  )(implicit
      packagePrefixes: PackagePrefixes
  ): (Vector[MethodSpec], Seq[(ClassName, String)]) = {
    val constructor = ConstructorGenerator.generateConstructor(fields)

    val conversionMethods = distinctTypeVars(fields, typeParameters) match {
      case IndexedSeq(params) =>
        List(
          toValue(constructorName, params, fields)
        )
      case IndexedSeq(usedParams, allParams) =>
        // usedParams is always subset of allParams
        List(
          toValue(constructorName, usedParams, fields),
          toValue(constructorName, allParams, fields),
        )
    }

    val (toJsonMethods, staticImports) =
      ToJsonGenerator.forVariantRecord(constructorName, fields, typeParameters)

    val methods: Vector[MethodSpec] = Vector(constructor) ++ conversionMethods ++
      ObjectMethods(className.rawType, typeParameters, fields.map(_.javaName)) ++ toJsonMethods

    (methods, staticImports)
  }

  private def toValue(constructorName: String, params: IndexedSeq[String], fields: Fields)(implicit
      packagePrefixes: PackagePrefixes
  ) = ToValueGenerator.generateToValueForRecordLike(
    params,
    fields,
    TypeName.get(classOf[javaapi.data.Variant]),
    name =>
      CodeBlock.of(
        "return new $T($S, new $T($L))",
        classOf[javaapi.data.Variant],
        constructorName,
        classOf[javaapi.data.DamlRecord],
        name,
      ),
  )

}
