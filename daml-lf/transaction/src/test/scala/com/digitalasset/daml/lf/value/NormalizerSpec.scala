package com.daml.lf.value

import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.test.ValueGenerators._
import com.daml.lf.value.Value.ContractId
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.Ordering.Implicits.infixOrderingOps

class NormalizerSpec extends AnyWordSpec with Matchers with ScalaCheckPropertyChecks {

  private[this] def reference(version: TransactionVersion, value: Value[ContractId]) =
    for {
      encoded <- ValueCoder
        .encodeValue(ValueCoder.CidEncoder, version, value)
        .left
        .map(_.errorMessage)
      decoded <- ValueCoder
        .decodeValue(ValueCoder.CidDecoder, version, encoded)
        .left
        .map(_.errorMessage)
    } yield decoded

  forAll(valueGen, Gen.oneOf(TransactionVersion.All)) { (value, version) =>
    whenever(TransactionBuilder.assertAssignVersion(value) <= version) {
      Right(Normalizer.normalize(value, version)) shouldBe reference(version, value)
    }
  }

}
