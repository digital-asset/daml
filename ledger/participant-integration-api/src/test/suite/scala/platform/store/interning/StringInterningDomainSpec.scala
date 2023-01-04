// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.interning

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StringInterningDomainSpec extends AnyFlatSpec with Matchers {

  behavior of "StringInterningDomain.prefixing"

  case class StringBox(value: String)
  object StringBox {
    def from(raw: String): StringBox = StringBox(raw)
    def to(boxed: StringBox): String = boxed.value
  }

  class StaticStringInterningAccessor(
      idToString: Map[Int, String],
      stringToId: Map[String, Int],
  ) extends StringInterningAccessor[String] {
    override def internalize(t: String): Int = tryInternalize(t).get
    override def tryInternalize(t: String): Option[Int] = stringToId.get(t)
    override def externalize(id: Int): String = tryExternalize(id).get
    override def tryExternalize(id: Int): Option[String] = idToString.get(id)
  }

  object StaticStringInterningAccessor {
    def apply(entries: Seq[(Int, String)]): StaticStringInterningAccessor = {
      new StaticStringInterningAccessor(
        idToString = entries.toMap,
        stringToId = entries.map(_.swap).toMap,
      )
    }
  }

  it should "handle a known string " in {
    val accessor = StaticStringInterningAccessor(List(1 -> ".one", 2 -> ".two"))
    val domain = StringInterningDomain.prefixing(".", accessor, StringBox.from, StringBox.to)

    domain.tryExternalize(2) shouldBe Some(StringBox("two"))
    domain.externalize(2) shouldBe StringBox("two")
    domain.tryInternalize(StringBox("two")) shouldBe Some(2)
    domain.internalize(StringBox("two")) shouldBe 2

    domain.unsafe.tryExternalize(2) shouldBe Some("two")
    domain.unsafe.externalize(2) shouldBe "two"
    domain.unsafe.tryInternalize("two") shouldBe Some(2)
    domain.unsafe.internalize("two") shouldBe 2
  }

  it should "handle an unknown string" in {
    val accessor = StaticStringInterningAccessor(List(1 -> ".one", 2 -> ".two"))
    val domain = StringInterningDomain.prefixing(".", accessor, StringBox.from, StringBox.to)

    domain.tryExternalize(3) shouldBe empty
    domain.tryInternalize(StringBox("three")) shouldBe empty

    domain.unsafe.tryExternalize(3) shouldBe empty
    domain.unsafe.tryInternalize("three") shouldBe empty
  }

  it should "work when two different domains share an accessor" in {
    val accessor = StaticStringInterningAccessor(List(1 -> "aX", 2 -> "bX"))
    val domainA = StringInterningDomain.prefixing("a", accessor, StringBox.from, StringBox.to)
    val domainB = StringInterningDomain.prefixing("b", accessor, StringBox.from, StringBox.to)

    domainA.internalize(StringBox("X")) shouldBe 1
    domainB.internalize(StringBox("X")) shouldBe 2
    domainA.externalize(1) shouldBe StringBox("X")
    domainB.externalize(2) shouldBe StringBox("X")

    domainA.unsafe.internalize("X") shouldBe 1
    domainB.unsafe.internalize("X") shouldBe 2
    domainA.unsafe.externalize(1) shouldBe "X"
    domainB.unsafe.externalize(2) shouldBe "X"
  }

  it should "work when two identical domains share an accessor" in {
    val accessor = StaticStringInterningAccessor(List(1 -> ".one", 2 -> ".two"))
    val domainA = StringInterningDomain.prefixing(".", accessor, StringBox.from, StringBox.to)
    val domainB = StringInterningDomain.prefixing(".", accessor, StringBox.from, StringBox.to)

    domainA.internalize(StringBox("one")) shouldBe 1
    domainB.internalize(StringBox("one")) shouldBe 1
    domainA.externalize(2) shouldBe StringBox("two")
    domainB.externalize(2) shouldBe StringBox("two")

    domainA.unsafe.internalize("one") shouldBe 1
    domainB.unsafe.internalize("one") shouldBe 1
    domainA.unsafe.externalize(2) shouldBe "two"
    domainB.unsafe.externalize(2) shouldBe "two"
  }
}
