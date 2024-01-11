// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging.pretty

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ApiLoggingConfig
import com.digitalasset.canton.util.ShowUtil.*
import org.mockito.exceptions.verification.SmartNullPointerException
import org.scalatest.wordspec.AnyWordSpec

class PrettyPrintingTest extends AnyWordSpec with BaseTest {

  case object ExampleSingleton extends PrettyPrinting {
    override def pretty: Pretty[ExampleSingleton.type] = prettyOfObject[ExampleSingleton.type]
  }

  val singletonInst: ExampleSingleton.type = ExampleSingleton
  val singletonStr: String = "ExampleSingleton"

  /** Example of a class where pretty printing needs to be implemented separately.
    */
  case class ExampleAlienClass(p1: String, p2: String)

  /** Enable pretty printing for [[ExampleAlienClass]].
    */
  implicit val prettyAlien: Pretty[ExampleAlienClass] = {
    import Pretty.*
    prettyOfClass(
      param("p1", _.p1.doubleQuoted),
      unnamedParam(_.p2.doubleQuoted),
      customParam(inst =>
        show"allParams: {${Seq(inst.p1.singleQuoted, inst.p2.singleQuoted).mkShow()}}"
      ),
      paramWithoutValue("confidential"),
    )
  }

  val alienInst: ExampleAlienClass = ExampleAlienClass("p1Val", "p2Val")
  val alienStr: String =
    """ExampleAlienClass(p1 = "p1Val", "p2Val", allParams: {'p1Val', 'p2Val'}, confidential = ...)"""

  /** Example of a class that extends [[PrettyPrinting]].
    */
  case class ExampleCaseClass(alien: ExampleAlienClass, singleton: ExampleSingleton.type)
      extends PrettyPrinting {
    override def pretty: Pretty[ExampleCaseClass] =
      prettyOfClass(param("alien", _.alien), param("singleton", _.singleton))
  }

  val caseClassInst: ExampleCaseClass = ExampleCaseClass(alienInst, ExampleSingleton)
  val caseClassStr: String = s"ExampleCaseClass(alien = $alienStr, singleton = $singletonStr)"

  /** Example of a class that uses ad hoc pretty printing.
    */
  case class ExampleAdHocCaseClass(alien: ExampleAlienClass, caseClass: ExampleCaseClass)
      extends PrettyPrinting {
    override def pretty: Pretty[ExampleAdHocCaseClass] = adHocPrettyInstance
  }

  val adHocCaseClassInst: ExampleAdHocCaseClass = ExampleAdHocCaseClass(alienInst, caseClassInst)
  val adHocCaseClassStr: String =
    s"""ExampleAdHocCaseClass(
       |  ExampleAlienClass("p1Val", "p2Val"),
       |  $caseClassStr
       |)""".stripMargin

  case object ExampleAdHocObject extends PrettyPrinting {
    override def pretty: Pretty[this.type] = adHocPrettyInstance
  }

  val adHocObjectInst: ExampleAdHocObject.type = ExampleAdHocObject
  val adHocObjectStr: String = "ExampleAdHocObject"

  case class ExampleAbstractCaseClass(content: Int) extends PrettyPrinting {
    override def pretty: Pretty[ExampleAbstractCaseClass] = prettyOfClass(
      param("content", _.content)
    )
  }

  val abstractCaseClass: ExampleAbstractCaseClass = ExampleAbstractCaseClass(42)
  val abstractCaseClassStr: String = "ExampleAbstractCaseClass(content = 42)"

  "show is pretty" in {
    singletonInst.show shouldBe singletonStr
    alienInst.show shouldBe alienStr
    caseClassInst.show shouldBe caseClassStr
    adHocCaseClassInst.show shouldBe adHocCaseClassStr
    adHocObjectInst.show shouldBe adHocObjectStr
    abstractCaseClass.show shouldBe abstractCaseClassStr
  }

  "show interpolator is pretty" in {
    show"Showing $singletonInst" shouldBe s"Showing $singletonStr"
    show"Showing $alienInst" shouldBe s"Showing $alienStr"
    show"Showing $caseClassInst" shouldBe s"Showing $caseClassStr"
    show"Showing $adHocCaseClassInst" shouldBe s"Showing $adHocCaseClassStr"
    show"Showing $adHocObjectInst" shouldBe s"Showing $adHocObjectStr"
    show"Showing $abstractCaseClass" shouldBe s"Showing $abstractCaseClassStr"
  }

  "toString is pretty" in {
    singletonInst.toString shouldBe singletonStr
    caseClassInst.toString shouldBe caseClassStr
    adHocCaseClassInst.toString shouldBe adHocCaseClassStr
    adHocObjectInst.toString shouldBe adHocObjectStr
    abstractCaseClass.toString shouldBe abstractCaseClassStr
  }

  "toString is not pretty" in {
    alienInst.toString shouldBe "ExampleAlienClass(p1Val,p2Val)"
  }

  "fail gracefully on a mock" in {
    val mockedInst = mock[ExampleCaseClass]

    (the[SmartNullPointerException] thrownBy mockedInst.toString).getMessage should
      endWith("exampleCaseClass.pretty();\n")
    (the[SmartNullPointerException] thrownBy mockedInst.show).getMessage should
      endWith("exampleCaseClass.pretty();\n")
    import Pretty.PrettyOps
    (the[SmartNullPointerException] thrownBy mockedInst.toPrettyString()).getMessage should
      endWith("exampleCaseClass.pretty();\n")
  }

  "catch exception when pretty printing invalid control-chars" in {
    final case class Invalid(str: String) extends PrettyPrinting {
      override protected[pretty] def pretty: Pretty[Invalid] = prettyOfString(_.str)
    }

    final case class Invalid2(str: String)

    val invalidAnsi = "\u001b[0;31m"
    val errorStr =
      "Unknown ansi-escape [0;31m at index 0 inside string cannot be parsed into an fansi.Str"

    val invalid = Invalid(invalidAnsi)
    show"$invalid" should include(errorStr)

    val invalid2 = Invalid2(invalidAnsi)
    val config = ApiLoggingConfig()
    val pprinter = new CantonPrettyPrinter(config.maxStringLength, config.maxMessageLines)
    pprinter.printAdHoc(invalid2) should include(errorStr)
  }

  "prettyOfClass" should {
    "work for primitive classes" in {
      Pretty
        .prettyOfClass[Long](Pretty.unnamedParam(Predef.identity))
        .treeOf(13L)
        .show shouldBe "Long(13)"
    }

    "work for Null" in {
      @SuppressWarnings(Array("org.wartremover.warts.Null"))
      val nulll = Pretty.prettyOfClass[Null]().treeOf(null)
      nulll.show shouldBe "Null()"
    }

    "work for AnyRef" in {
      Pretty.prettyOfClass[AnyRef]().treeOf(new Object).show shouldBe "Object()"
    }

    "work for Java interfaces" in {
      Pretty
        .prettyOfClass[Runnable]()
        .treeOf(new Runnable() {
          override def run(): Unit = ???
        })
        .show shouldBe "Object()"
    }
  }
}
