// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java

import com.squareup.javapoet.{ClassName, MethodSpec, TypeName}
import com.typesafe.scalalogging.StrictLogging

import javax.lang.model.element.Modifier
import com.daml.lf.codegen.backend.java.inner.ClassNameExtensions

import java.util.Objects

private[codegen] object ObjectMethods extends StrictLogging {

  def apply(
      className: ClassName,
      typeParameters: IndexedSeq[String],
      fieldNames: IndexedSeq[String],
  ): Vector[MethodSpec] =
    Vector(
      generateEquals(className.asWildcardType(typeParameters), fieldNames),
      generateHashCode(fieldNames),
      generateToString(className, fieldNames, None),
    )

  def apply(
      className: ClassName,
      typeParameters: IndexedSeq[String],
      fieldNames: IndexedSeq[String],
      enclosingClassName: ClassName,
  ): Vector[MethodSpec] =
    Vector(
      generateEquals(className.asWildcardType(typeParameters), fieldNames),
      generateHashCode(fieldNames),
      generateToString(className, fieldNames, Some(enclosingClassName)),
    )

  private def initEqualsBuilder(className: TypeName): MethodSpec.Builder =
    MethodSpec
      .methodBuilder("equals")
      .addModifiers(Modifier.PUBLIC)
      .addAnnotation(classOf[java.lang.Override])
      .addParameter(classOf[java.lang.Object], "object")
      .returns(TypeName.BOOLEAN)
      .beginControlFlow("if (this == object)")
      .addStatement("return true")
      .endControlFlow()
      .beginControlFlow("if (object == null)")
      .addStatement("return false")
      .endControlFlow()
      .beginControlFlow("if (!(object instanceof $T))", className)
      .addStatement("return false")
      .endControlFlow()

  private def generateEquals(className: TypeName, fieldNames: IndexedSeq[String]): MethodSpec =
    if (fieldNames.isEmpty) {
      initEqualsBuilder(className).addStatement("return true").build()
    } else {
      initEqualsBuilder(className)
        .addStatement("$T other = ($T) object", className, className)
        .addStatement(
          s"return ${fieldNames.map(_ => "$T.equals(this.$L, other.$L)").mkString(" && ")}",
          fieldNames.flatMap(fieldName =>
            IndexedSeq[Any](classOf[Objects], fieldName, fieldName)
          ): _*
        )
        .build()
    }

  private def initHashCodeBuilder(): MethodSpec.Builder =
    MethodSpec
      .methodBuilder("hashCode")
      .addModifiers(Modifier.PUBLIC)
      .addAnnotation(classOf[java.lang.Override])
      .returns(TypeName.INT)

  @SuppressWarnings(
    Array("org.wartremover.warts.JavaSerializable", "org.wartremover.warts.Serializable")
  )
  def generateHashCode(fieldNames: IndexedSeq[String]): MethodSpec =
    initHashCodeBuilder()
      .addStatement(
        s"return $$T.hash(${List.fill(fieldNames.size)("this.$L").mkString(", ")})",
        IndexedSeq(classOf[java.util.Objects]) ++ fieldNames: _*
      )
      .build()

  private def initToStringBuilder(): MethodSpec.Builder =
    MethodSpec
      .methodBuilder("toString")
      .addModifiers(Modifier.PUBLIC)
      .addAnnotation(classOf[java.lang.Override])
      .returns(ClassName.get(classOf[java.lang.String]))

  private def template(
      className: ClassName,
      fieldNames: IndexedSeq[String],
      enclosingClassName: Option[ClassName],
  ): String =
    s"${enclosingClassName.fold("")(n => s"$n.")}$className(${List.fill(fieldNames.size)("%s").mkString(", ")})"

  @SuppressWarnings(
    Array("org.wartremover.warts.JavaSerializable", "org.wartremover.warts.Serializable")
  )
  def generateToString(
      className: ClassName,
      fieldNames: IndexedSeq[String],
      enclosingClassName: Option[ClassName],
  ): MethodSpec = {
    if (fieldNames.isEmpty) {
      initToStringBuilder().addStatement("return $S", className).build()
    } else {
      initToStringBuilder()
        .addStatement(
          s"return $$T.format($$S, ${List.fill(fieldNames.size)("this.$L").mkString(", ")})",
          IndexedSeq(
            classOf[java.lang.String],
            template(className, fieldNames, enclosingClassName),
          ) ++ fieldNames: _*
        )
        .build()
    }
  }

}
