// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.v1.value.Identifier

case class FooTemplateDescriptor(
    name: String,
    templateId: Identifier,
    consumingChoiceName: String,
    nonconsumingChoiceName: String,
)

/** NOTE: Keep me in sync with `Foo.daml`
  */
object FooTemplateDescriptor {

  val Foo1: FooTemplateDescriptor = FooTemplateDescriptor(
    name = "Foo1",
    templateId = com.daml.ledger.test.model.Foo.Foo1.id.asInstanceOf[Identifier],
    consumingChoiceName = "Foo1_ConsumingChoice",
    nonconsumingChoiceName = "Foo1_NonconsumingChoice",
  )
  val Foo2: FooTemplateDescriptor = FooTemplateDescriptor(
    name = "Foo2",
    templateId = com.daml.ledger.test.model.Foo.Foo2.id.asInstanceOf[Identifier],
    consumingChoiceName = "Foo2_ConsumingChoice",
    nonconsumingChoiceName = "Foo2_NonconsumingChoice",
  )
  val Foo3: FooTemplateDescriptor = FooTemplateDescriptor(
    name = "Foo3",
    templateId = com.daml.ledger.test.model.Foo.Foo3.id.asInstanceOf[Identifier],
    consumingChoiceName = "Foo3_ConsumingChoice",
    nonconsumingChoiceName = "Foo3_NonconsumingChoice",
  )

  private val all: Map[String, FooTemplateDescriptor] =
    List(Foo1, Foo2, Foo3).map(foo => foo.name -> foo).toMap

  def forName(templateName: String): FooTemplateDescriptor =
    all.getOrElse(templateName, sys.error(s"Invalid template: $templateName"))

  val Divulger_templateId: Identifier =
    com.daml.ledger.test.model.Foo.Divulger.id.asInstanceOf[Identifier]
  val Divulger_DivulgeContractImmediate = "DivulgeContractImmediate"
  val Divulger_DivulgeConsumingExercise = "DivulgeConsumingExercise"
}
