// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java.inner
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{DottedName, Identifier, QualifiedName}
import com.digitalasset.daml.lf.iface._
import com.squareup.javapoet.{ClassName, TypeName}
import javax.lang.model.element.Modifier
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
final class RecordFieldsSpec extends FlatSpec with Matchers {

  behavior of "RecordFields"

  it should "not generate any parameter from an empty record" in {
    RecordFields(getFieldsWithTypes(ImmArraySeq(), Map())) shouldBe empty
  }

  it should "throw exception when the parameter name is empty" in {
    an[IllegalArgumentException] shouldBe thrownBy(
      RecordFields(
        getFieldsWithTypes(ImmArraySeq("" -> TypePrim(PrimTypeBool, ImmArraySeq.empty)), Map())))
  }

  it should "return the proper builder for the passed record" in {
    val bool =
      RecordFields(
        getFieldsWithTypes(ImmArraySeq("bool" -> TypePrim(PrimTypeBool, ImmArraySeq.empty)), Map()))

    bool should have length 1

    bool.head.name shouldBe "bool"
    bool.head.`type` shouldBe TypeName.get(classOf[java.lang.Boolean])

    bool.head.annotations shouldBe empty
    bool.head.initializer shouldBe empty
    bool.head.javadoc shouldBe empty
    bool.head.modifiers shouldBe Set(Modifier.PUBLIC, Modifier.FINAL).asJava
  }

  it should "use the supplied package prefix" in {
    val packageId = Ref.PackageId.assertFromString("some other package")
    val ident = Identifier(
      packageId,
      QualifiedName(DottedName.assertFromString("Foo.Bar"), DottedName.assertFromString("Baz")))

    val fields =
      RecordFields(
        getFieldsWithTypes(
          ImmArraySeq("field" -> TypeCon(TypeConName(ident), ImmArraySeq.empty)),
          Map()))

    fields should have length 1

    val field = fields.head
    field.name shouldBe "field"
    field.`type` shouldBe ClassName.get("foo.bar", "Baz")
    field.initializer shouldBe empty
    field.javadoc shouldBe empty
    field.modifiers shouldBe Set(Modifier.PUBLIC, Modifier.FINAL).asJava
  }

}
