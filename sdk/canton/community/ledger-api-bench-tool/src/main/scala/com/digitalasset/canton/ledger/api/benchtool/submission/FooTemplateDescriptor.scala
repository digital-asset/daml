// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.daml.ledger.api.v2.value.Identifier

final case class FooTemplateDescriptor(
    name: String,
    templateId: Identifier,
    consumingChoiceName: String,
    nonconsumingChoiceName: String,
)

/** NOTE: Keep me in sync with `Foo.daml`
  */
object FooTemplateDescriptor {

  def inefficientFibonacciTemplateId(packageId: String): Identifier = Identifier(
    packageId = packageId,
    moduleName = "Bench",
    entityName = "InefficientFibonacci",
  )

  def fooI1TemplateId(packageId: String): Identifier = Identifier(
    packageId = packageId,
    moduleName = "Foo",
    entityName = "FooI1",
  )

  def fooI2TemplateId(packageId: String): Identifier = Identifier(
    packageId = packageId,
    moduleName = "Foo",
    entityName = "FooI2",
  )

  def fooI3TemplateId(packageId: String): Identifier = Identifier(
    packageId = packageId,
    moduleName = "Foo",
    entityName = "FooI3",
  )

  def divulgerTemplateId(packageId: String): Identifier = Identifier(
    packageId = packageId,
    moduleName = "Foo",
    entityName = "Divulger",
  )

  def dummyTemplateId(packageId: String): Identifier = Identifier(
    packageId = packageId,
    moduleName = "Foo",
    entityName = "Dummy",
  )

  def Foo1(packageId: String): FooTemplateDescriptor = FooTemplateDescriptor(
    name = "Foo1",
    templateId = Identifier(
      packageId = packageId,
      moduleName = "Foo",
      entityName = "Foo1",
    ),
    consumingChoiceName = "Foo1_ConsumingChoice",
    nonconsumingChoiceName = "Foo1_NonconsumingChoice",
  )
  def Foo2(packageId: String): FooTemplateDescriptor = FooTemplateDescriptor(
    name = "Foo2",
    templateId = Identifier(
      packageId = packageId,
      moduleName = "Foo",
      entityName = "Foo2",
    ),
    consumingChoiceName = "Foo2_ConsumingChoice",
    nonconsumingChoiceName = "Foo2_NonconsumingChoice",
  )
  def Foo3(packageId: String): FooTemplateDescriptor = FooTemplateDescriptor(
    name = "Foo3",
    templateId = Identifier(
      packageId = packageId,
      moduleName = "Foo",
      entityName = "Foo3",
    ),
    consumingChoiceName = "Foo3_ConsumingChoice",
    nonconsumingChoiceName = "Foo3_NonconsumingChoice",
  )

  def forName(templateName: String, packageId: String): FooTemplateDescriptor =
    templateName match {
      case "Foo1" => Foo1(packageId)
      case "Foo2" => Foo2(packageId)
      case "Foo3" => Foo3(packageId)
      case other => sys.error(s"Invalid template: $other")
    }

  val Divulger_DivulgeContractImmediate = "DivulgeContractImmediate"
  val Divulger_DivulgeConsumingExercise = "DivulgeConsumingExercise"
}
