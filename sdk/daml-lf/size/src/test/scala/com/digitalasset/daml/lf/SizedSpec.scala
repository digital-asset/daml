// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml
package lf

import com.digitalasset.daml.lf.data.{BackStack, FrontStack, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.value.Value
//import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.test.ValueGenerators
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.immutable.TreeMap
import org.scalacheck.Gen
import org.scalacheck.util.Buildable
import org.scalactic.anyvals.PosInt
import com.google.protobuf

import scala.reflect.ClassTag

class SizedSpec extends AnyWordSpec with Matchers with ScalaCheckPropertyChecks {

  import SizedSpec._

  //  import com.digitalasset.daml.lf.value.test.ValueGenerators._

  //  import Sized._
  //  import data.Sized._
  //  import speedy.Sized._

  def test[T](
      name: String,
      strict: Boolean = false,
      verbose: Boolean = false,
      minSucc: PosInt = 10,
  )(implicit sized: Sized[T], gen: Gen[T]): Unit =
    s"approximate real footprint of $name" in {
      forAll(gen, minSuccessful(minSucc)) { x: T =>
        remy.catcha {
          sized.bloat(x)

          val shallowFootprint = sized.shallowFootprint(x)
          val approximativeShallowSize = sized.approximateShallowFootprint(x)

          val s = compare(shallowFootprint, approximativeShallowSize)
          println(
            s"Sized: ${name}, actualShallowSize: $shallowFootprint, approximativeShallowSize: $approximativeShallowSize ($s), ${compare(shallowFootprint, approximativeShallowSize)}, x=`${x.toString
                .take(60)}`"
          )
          if (verbose) {
            println(classLayout(x))
            println(graphLayout(x))
          }
          assert(shallowFootprint == sized.shallowFootprint(x), "5")

          if (shallowFootprint != approximativeShallowSize)
            if (strict)
              shallowFootprint shouldBe approximativeShallowSize
            else
              shallowFootprint should be <= approximativeShallowSize

          Sized.footprint(x) should be <= sized.approximateFootprint(x)
        }
      }
    }

  "Sized#approximateFootprint" should {

    /* Generic atom */
    test[MyLong]("MyInt")
    test[Array[Byte]]("Array[Byte]", strict = Sized.precise, minSucc = 100)
    test[String]("String", minSucc = 100, verbose = false, strict = Sized.precise)
    test[protobuf.ByteString]("BytesString", strict = false) // Sized.precise)

    /* Generic data structures */
    test[Option[MyLong]]("Option[MyInt]")
    test[List[MyLong]]("List[MyInt]", minSucc = 100)
    test[Array[MyLong]]("Array[MyInt]", minSucc = 100)
    test[TreeMap[MyLong, MyLong]]("TreeMap[MyInt, MyInt]", minSucc = 100)

    test[Option[String]]("Option[String]", strict = Sized.precise, minSucc = 100)
    test[List[String]](
      "List[String]",
      strict = Sized.precise,
      minSucc = 100,
    ) // List[String] is not a fixed size data structure, so we need to generate bigger lists
    test[Array[String]]("Array[String]", strict = Sized.precise)
    test[TreeMap[String, String]]("TreeMap[String, String]", minSucc = 100)

    /* LF data structures */

    import data.Sized._

    test[data.Bytes]("Bytes", strict = Sized.precise)
    test[crypto.Hash]("Hash", strict = false, verbose = true)
    test[data.Ref.DottedName]("DottedName", strict = true)
    test[data.Ref.QualifiedName]("QualifiedName", strict = true)
    test[data.Ref.Identifier]("Identifier", strict = true)
    test[data.Numeric]("Numeric", strict = false, minSucc = 100)
    test[data.Time.Date]("Date", strict = true)
    test[data.Time.Timestamp]("Timestamp", strict = true)
    test[Value.ContractId.V1](name = "ContractId.V1")
    // test[Value.ContractId](name = "ContractId", strict = Sized.precise)

    test[data.ImmArray[MyLong]]("ImmArray[MyInt]", strict = true)
    test[data.FrontStack[MyLong]]("FrontStack[MyInt]", strict = true)
    test[data.BackStack[MyLong]]("BackStack[MyInt]", strict = true)

    test[data.ImmArray[String]]("ImmArray[String]", strict = Sized.precise)
    test[data.FrontStack[String]]("FrontStack[String]", strict = Sized.precise)
    test[data.BackStack[String]]("BackStack[String]", strict = Sized.precise)

    /* Speedy data structures */

    import speedy.Sized._

    // test[SValue.SRecord]("SRecord")
    test[SValue.SUnit.type]("SUnit", strict = true, minSucc = 1)
    test[SValue.SBool]("SBool", minSucc = 2)
    test[SValue.SInt64]("SInt64", strict = true)
    test[SValue.SDate](name = "SDate", strict = true)
    test[SValue.STimestamp](name = "STimestamp", strict = true)
    test[SValue.SText](name = "SText", strict = true)
    test[SValue.SNumeric](name = "SNumeric", strict = true)
    test[SValue.SContractId]("SContractId", strict = true)
    test[SValue.SParty]("SParty", strict = true)

    test[SValue.SRecord]("SRecord", strict = true)
    test[SValue.SVariant]("SVariant", strict = true)
    test[SValue.SEnum]("SEnum", strict = true)
    test[SValue.SOptional]("SOptional", strict = true)
    test[SValue.SList]("SList", strict = true)
    test[SValue.SMap]("SMap", strict = true)
    //    test[SValue.SStruct]("SStruct", strict = true)
    //    test[SValue.SPAP]("SPAP", strict = true)
    //    test[SValue.SAny]("SAny", strict = true)
    //    test[SValue.STypeRep]("STypeRep", strict = true)
    //    test[SValue.SToken]

  }
}

object SizedSpec {

  def graphLayout(x: Any) =
    org.openjdk.jol.info.GraphLayout.parseInstance(x).toFootprint

  def classLayout(x: Any) =
    org.openjdk.jol.info.ClassLayout.parseInstance(x).toPrintable

  case class MyLong(x: Long)

  def compare(x: Long, y: Long) =
    "%.2f%%".format(((x.toDouble - y.toDouble) / x.toDouble * 100))

  object MyLong {
    implicit val ordering: Ordering[MyLong] = Ordering.by[MyLong, Long](_.x)
    implicit val gen: Gen[MyLong] = Gen.long.map(MyLong(_))
    implicit val sized: Sized[MyLong] = new SizedFixSizeAtom[MyLong](24)
  }

  implicit def buildableArray[T: ClassTag]: Buildable[T, Array[T]] =
    new Buildable[T, Array[T]] {
      def builder = Array.newBuilder[T]
    }

  implicit def genByte: Gen[Byte] = Gen.chooseNum(Byte.MinValue, Byte.MaxValue)

  def genByteArrayN(n: Int) = Gen.containerOfN(n, genByte)

  implicit def genByteArray: Gen[Array[Byte]] =
    Gen.sized(s => Gen.choose(0, s max 0)).flatMap(genByteArrayN)

  implicit def genString: Gen[String] = Gen.alphaStr.map {
    case "" => String.valueOf(Array.empty) // avoid sharing of empty string
    case s => s
  }

  implicit def genOption[X](implicit x: Gen[X]): Gen[Option[X]] = Gen.option(x)

  implicit def genList[X](implicit x: Gen[X]): Gen[List[X]] = Gen.listOf(x)

  implicit def genArray[X: ClassTag](implicit x: Gen[X]): Gen[Array[X]] = Gen.containerOf(x)

  implicit def genTreeMap[K, V](implicit
      k: Gen[K],
      v: Gen[V],
      ordering: Ordering[K],
  ): Gen[TreeMap[K, V]] =
    Gen.listOf(for { x <- k; y <- v } yield (x, y)).map(_.to(TreeMap))

  implicit def genImmArray[X](implicit x: Gen[X]): Gen[ImmArray[X]] =
    Gen.listOf(x).map(ImmArray.from(_))

  implicit def genFrontStack[X](implicit x: Gen[X]): Gen[FrontStack[X]] =
    Gen.listOf(x).map(_.foldLeft(FrontStack.empty[X])(_.+:(_)))

  implicit def genBackStack[X](implicit x: Gen[X]): Gen[BackStack[X]] =
    Gen.listOf(x).map(_.foldLeft(BackStack.empty[X])(_.:+(_)))

  implicit def genNumeric: Gen[data.Numeric] = ValueGenerators.unscaledNumGen

  implicit def genDate: Gen[Time.Date] = ValueGenerators.dateGen

  implicit def genTimeStamp: Gen[Time.Timestamp] = ValueGenerators.timestampGen

  implicit def genByteString: Gen[protobuf.ByteString] =
    genByteArray.map(protobuf.ByteString.copyFrom(_))

  implicit def genBytes: Gen[data.Bytes] =
    genByteString.map(data.Bytes.fromByteString)

  implicit def genHash: Gen[crypto.Hash] =
    genByteArrayN(32).map(crypto.Hash.assertFromByteArray)

  def genParty: Gen[Ref.Party] = ValueGenerators.party

  def genName: Gen[Ref.Name] = ValueGenerators.nameGen

  implicit def genDottedName: Gen[Ref.DottedName] = ValueGenerators.dottedNameGen

  implicit def genQualifiedName: Gen[Ref.QualifiedName] = genIdentifier.map(_.qualifiedName)

  implicit def genIdentifier: Gen[Ref.Identifier] = ValueGenerators.idGen

  implicit def genContractIdV1: Gen[Value.ContractId.V1] = for {
    discriminator <- genHash
    suffixLength <- Gen.choose(0, 0)
    suffix <- genByteArrayN(suffixLength)
  } yield Value.ContractId.V1(discriminator, data.Bytes.fromByteArray(suffix))

  implicit lazy val genSValue: Gen[SValue] =
    Gen.oneOf(genSBool, genSBool)

  implicit val genSUnit: Gen[SValue.SUnit.type] = Gen.const(SValue.SUnit)
  implicit val genSBool: Gen[SValue.SBool] = Gen.oneOf(true, false).map(SValue.SBool(_))
  implicit val genSInt64: Gen[SValue.SInt64] = Gen.long.map(SValue.SInt64(_))
  implicit val genSDate: Gen[SValue.SDate] = genDate.map(SValue.SDate)
  implicit val genSTimestamp: Gen[SValue.STimestamp] = genTimeStamp.map(SValue.STimestamp)
  implicit val genSNumeric: Gen[SValue.SNumeric] = genNumeric.map(SValue.SNumeric(_))
  implicit val genSText: Gen[SValue.SText] = Gen.alphaStr.map(SValue.SText)
  implicit val genSContractId: Gen[SValue.SContractId] = genContractIdV1.map(SValue.SContractId)
  implicit val genSParty: Gen[SValue.SParty] = genParty.map(SValue.SParty)

  implicit val genSRecord: Gen[SValue.SRecord] =
    for {
      id <- genIdentifier
      n <- Gen.choose(0, 10)
      fields <- Gen.listOfN(n, genName)
      values <- Gen.listOf(genSValue)
    } yield SValue.SRecord(id, ImmArray.from(fields), values.toArray)

  implicit val genSVariant: Gen[SValue.SVariant] =
    for {
      id <- genIdentifier
      n <- Gen.choose(0, 10)
      constructor <- genName
      values <- genSValue
    } yield SValue.SVariant(id, constructor, n, values)

  implicit val genSEnum: Gen[SValue.SEnum] =
    for {
      id <- genIdentifier
      n <- Gen.choose(0, 10)
      constructor <- genName
    } yield SValue.SEnum(id, constructor, n)

  implicit val genSList: Gen[SValue.SList] = genFrontStack(genSValue).map(SValue.SList(_))

  implicit val genSOptional: Gen[SValue.SOptional] =
    Gen.option(genSValue).map(SValue.SOptional(_))

  implicit val genSMap: Gen[SValue.SMap] = for {
    isTextMap <- Gen.oneOf(true, false)
    keyGen = if (isTextMap) genSText else genSValue
    tree <- genTreeMap(keyGen, genSValue, SValue.SMap.`SMap Ordering`)
  } yield SValue.SMap(isTextMap, tree)

}
