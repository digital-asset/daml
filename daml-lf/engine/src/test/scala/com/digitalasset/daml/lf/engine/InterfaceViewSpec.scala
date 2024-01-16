// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.archive.UniversalArchiveDecoder
import com.daml.lf.data.Ref.{Identifier, PackageId, Party, QualifiedName}
import com.daml.lf.data.ImmArray
import com.daml.lf.language.Ast.Package
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.transaction.Versioned
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ValueBool, ValueInt64, ValueParty, ValueRecord}
import com.daml.logging.LoggingContext

import java.io.File
import org.scalatest.EitherValues
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.language.implicitConversions

class InterfaceViewSpecV1 extends InterfaceViewSpec(LanguageMajorVersion.V1)
class InterfaceViewSpecV2 extends InterfaceViewSpec(LanguageMajorVersion.V2)

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Serializable",
    "org.wartremover.warts.Product",
  )
)
class InterfaceViewSpec(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with Matchers
    with EitherValues
    with BazelRunfiles {

  import InterfaceViewSpec._

  private[this] implicit def logContext: LoggingContext = LoggingContext.ForTesting

  private def loadPackage(resource: String): (PackageId, Package, Map[PackageId, Package]) = {
    val packages = UniversalArchiveDecoder.assertReadFile(new File(rlocation(resource)))
    val (mainPkgId, mainPkg) = packages.main
    (mainPkgId, mainPkg, packages.all.toMap)
  }

  private val (interfaceviewsPkgId, interfaceviewsPkg, allPackages) = loadPackage(
    s"daml-lf/tests/InterfaceViews-v${majorLanguageVersion.pretty}.dar"
  )

  val interfaceviewsSignatures =
    language.PackageInterface(Map(interfaceviewsPkgId -> interfaceviewsPkg))
  val engine = Engine.DevEngine(majorLanguageVersion)

  private def id(s: String) = Identifier(interfaceviewsPkgId, s"InterfaceViews:$s")

  def computeView(templateId: Identifier, argument: Value, interfaceId: Identifier) =
    engine
      .computeInterfaceView(templateId, argument, interfaceId)
      .consume(PartialFunction.empty, allPackages)

  private val t1 = id("T1")
  private val t2 = id("T2")
  private val t3 = id("T3")
  private val t4 = id("T4")
  private val i = id("I")

  "interface view" should {

    "return result of view method when it succeds" in {
      inside(
        computeView(
          t1,
          ValueRecord(None, ImmArray((None, ValueParty(party)), (None, ValueInt64(42)))),
          i,
        )
      ) { case Right(Versioned(_, v)) =>
        v shouldBe ValueRecord(None, ImmArray((None, ValueInt64(42)), (None, ValueBool(true))))
      }
      inside(
        computeView(
          t2,
          ValueRecord(None, ImmArray((None, ValueParty(party)), (None, ValueInt64(23)))),
          i,
        )
      ) { case Right(Versioned(_, v)) =>
        v shouldBe ValueRecord(None, ImmArray((None, ValueInt64(23)), (None, ValueBool(false))))
      }
    }
    "fail with Error.Interpretation if view crashes" in {
      inside(
        computeView(
          t3,
          ValueRecord(None, ImmArray((None, ValueParty(party)), (None, ValueInt64(42)))),
          i,
        )
      ) { case Left(err) =>
        err shouldBe a[Error.Interpretation]
      }
    }
    "fail with Error.Preprocessing if template does not implement interface" in {
      inside(
        computeView(
          t4,
          ValueRecord(None, ImmArray((None, ValueParty(party)), (None, ValueInt64(42)))),
          i,
        )
      ) { case Left(err) =>
        err shouldBe a[Error.Preprocessing]
      }
    }
    "fail with Error.Preprocessing if argument has invalid type" in {
      inside(computeView(t1, ValueRecord(None, ImmArray((None, ValueParty(party)))), i)) {
        case Left(err) => err shouldBe a[Error.Preprocessing]
      }
    }
    "fail with Error.Preprocessing if template does not exist" in {
      inside(
        computeView(
          id("NonExistent"),
          ValueRecord(None, ImmArray((None, ValueParty(party)), (None, ValueInt64(42)))),
          i,
        )
      ) { case Left(err) =>
        err shouldBe a[Error.Preprocessing]
      }
    }
    "fail with Error.Preprocessing if   nterface does not exist" in {
      inside(
        computeView(
          t1,
          ValueRecord(None, ImmArray((None, ValueParty(party)), (None, ValueInt64(42)))),
          id("NonExistent"),
        )
      ) { case Left(err) =>
        err shouldBe a[Error.Preprocessing]
      }
    }
  }
}

object InterfaceViewSpec {

  private val party = Party.assertFromString("Party")

  implicit def qualifiedNameStr(s: String): QualifiedName =
    QualifiedName.assertFromString(s)
}
