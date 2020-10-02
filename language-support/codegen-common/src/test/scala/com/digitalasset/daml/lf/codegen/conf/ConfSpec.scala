// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.conf

import java.nio.file.Paths

import org.scalatest.{FlatSpec, Matchers, OptionValues}

class ConfSpec extends FlatSpec with Matchers with OptionValues {

  behavior of "Conf.parse"

  it should "return error when no arguments are passed" in {
    Conf.parse(Array.empty) shouldBe empty
  }

  it should "return error when only inputs are passed" in {
    Conf.parse(Array("foo")) shouldBe empty
  }

  it should "return error when only output is passed" in {
    Conf.parse(Array("-o", "bar")) shouldBe empty
  }

  it should "return error when only inputs and decoder class are passed" in {
    Conf.parse(Array("-d", "package.ClassName", "input")) shouldBe empty
  }

  it should "return a Conf when input, output and a known backend are passed" in {
    Conf.parse(Array("-o", "output", "input")) shouldNot be(empty)
  }

  it should "return a Conf when input, output, a known backend and deocder FQCN are passed" in {
    Conf.parse(Array("-o", "output", "-d", "package.ClassName", "input")) shouldNot be(empty)
  }

  // XXX SC remove in Scala 2.13. aggregatingNatureOfGenTraversable is
  // mis-signed because it forces Map[K, V] to destructure as
  // TRAV[e] = Map[K, e], which is of course not <: GenTraversable[e]. And it's
  // needless, as proven below, just like the similar problem with
  // Future.traverse's sig
  import scala.collection.GenTraversable, org.scalatest.enablers.Aggregating
  private[this] implicit def `fixed sig aggregatingNatureOfGenTraversable`[
      E: org.scalactic.Equality,
      TRAV]: Aggregating[TRAV with GenTraversable[E]] =
    Aggregating.aggregatingNatureOfGenTraversable[E, GenTraversable]

  it should "return a Conf with expected single unmapped input and output" in {
    val conf = Conf.parse(Array("-o", "output", "input")).value
    conf.darFiles should contain theSameElementsAs Map(Paths.get("input") -> None)
  }

  it should "return error when illegal Decoder class is passed" in {
    Conf.parse(Array("-o", "output", "-d", "$illegal")) shouldBe empty
  }

  it should "return a Conf with expected single mapped input, output and backend" in {
    val conf = Conf.parse(Array("-o", "output", "input=input.prefix")).value
    conf.darFiles should contain theSameElementsAs Map(Paths.get("input") -> Some("input.prefix"))
  }

  it should "return a Conf with expected multiple mapped inputs, output and backend" in {
    val conf = Conf
      .parse(Array("-o", "output", "input1=input1.prefix", "input2=input2.prefix"))
      .value
    conf.darFiles should contain theSameElementsAs Map(
      Paths.get("input1") -> Some("input1.prefix"),
      Paths.get("input2") -> Some("input2.prefix")
    )
  }
  it should "return a Conf with expected multiple mixed inputs, output and backend" in {
    val conf =
      Conf.parse(Array("-o", "output", "input1=input1.prefix", "input2")).value
    conf.darFiles should contain theSameElementsAs Map(
      Paths.get("input1") -> Some("input1.prefix"),
      Paths.get("input2") -> None
    )
  }
}
