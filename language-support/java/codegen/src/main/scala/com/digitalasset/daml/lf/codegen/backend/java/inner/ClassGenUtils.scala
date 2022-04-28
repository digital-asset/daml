// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.lf.data.Ref.{ChoiceName, PackageId, QualifiedName}
import com.daml.lf.iface.{DefDataType, InterfaceType, Record, TypeCon}

import java.util.Optional
import com.daml.lf.iface._
import com.squareup.javapoet._
import com.daml.ledger.javaapi

import javax.lang.model.element.Modifier
import scala.reflect.ClassTag

private[inner] object ClassGenUtils {

  val optionalString = ParameterizedTypeName.get(classOf[Optional[_]], classOf[String])

  def optional(name: TypeName) =
    ParameterizedTypeName.get(ClassName.get(classOf[Optional[_]]), name)

  def setOfStrings = ParameterizedTypeName.get(classOf[java.util.Set[_]], classOf[String])

  val emptyOptional = CodeBlock.of("$T.empty()", classOf[Optional[_]])
  val emptySet = CodeBlock.of("$T.emptySet()", classOf[java.util.Collections])
  val getContractId = CodeBlock.of("event.getContractId()")
  val getArguments = CodeBlock.of("event.getArguments()")
  val getAgreementText = CodeBlock.of("event.getAgreementText()")

  def getContractKey(t: Type, packagePrefixes: Map[PackageId, String]) =
    CodeBlock.of(
      "event.getContractKey().map(e -> $L)",
      FromValueGenerator.extractor(t, "e", CodeBlock.of("e"), newNameGenerator, packagePrefixes),
    )

  val getSignatories = CodeBlock.of("event.getSignatories()")
  val getObservers = CodeBlock.of("event.getObservers()")

  def getRecord(
      typeCon: TypeCon,
      identifierToType: Map[QualifiedName, InterfaceType],
      packageId: PackageId,
  ): Option[Record.FWT] = {
    // TODO: at the moment we don't support other packages Records because the codegen works on single packages
    if (typeCon.name.identifier.packageId == packageId) {
      identifierToType.get(typeCon.name.identifier.qualifiedName) collect {
        case InterfaceType.Normal(DefDataType(_, record: Record.FWT)) =>
          record
      }
    } else None
  }

  val templateIdFieldName = "TEMPLATE_ID"
  val companionFieldName = "COMPANION"

  def generateTemplateIdField(packageId: PackageId, moduleName: String, name: String) =
    FieldSpec
      .builder(
        ClassName.get(classOf[javaapi.data.Identifier]),
        templateIdFieldName,
        Modifier.STATIC,
        Modifier.FINAL,
        Modifier.PUBLIC,
      )
      .initializer(
        "new $T($S, $S, $S)",
        classOf[javaapi.data.Identifier],
        packageId,
        moduleName,
        name,
      )
      .build()

  def generateFlattenedCreateOrExerciseMethod[T](
      name: String,
      choiceName: ChoiceName,
      choice: TemplateChoice[Type],
      fields: Fields,
      packagePrefixes: Map[PackageId, String],
  )(implicit ct: ClassTag[T]): MethodSpec = {
    val methodName = s"$name${choiceName.capitalize}"
    val choiceBuilder = MethodSpec
      .methodBuilder(methodName)
      .addModifiers(Modifier.PUBLIC)
      .returns(ct.runtimeClass)
    val javaType = toJavaTypeName(choice.param, packagePrefixes)
    for (FieldInfo(_, _, javaName, javaType) <- fields) {
      choiceBuilder.addParameter(javaType, javaName)
    }
    choiceBuilder.addStatement(
      "return $L(new $T($L))",
      methodName,
      javaType,
      generateArgumentList(fields.map(_.javaName)),
    )
    choiceBuilder.build()
  }

}
