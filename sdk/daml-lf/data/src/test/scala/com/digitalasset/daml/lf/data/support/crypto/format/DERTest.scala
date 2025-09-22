// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data
package support.crypto

import org.bouncycastle.asn1.{
  ASN1Encodable,
  ASN1Integer,
  ASN1ObjectIdentifier,
  DERBitString,
  DEROctetString,
  DLSequence,
  DLTaggedObject,
}

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._

class DERTest extends AnyFreeSpec with Matchers {

  "correctly decode valid DER hex strings" in {
    val testCases = Table(
      ("Hex String", "DER Object"),
      (
        Ref.HexString.assertFromString(
          "30818d020100301006072a8648ce3d020106052b8104000a0476307402010104207308c95bf6e240ed8de37b5a7c5f453d88ece2b5e93c02ef985e8553f856474aa00706052b8104000aa144034200043f4ae6efb79de2cf60636219110f11b695d5c1776c0b0dad1468672fba1c6f6acf79396b8403e110cbf60ccd7aefab4c541d49844a51049fcbd22dae1a51d681"
        ),
        new DLSequence(
          Array[ASN1Encodable](
            new ASN1Integer(0),
            new DLSequence(
              Array[ASN1Encodable](
                new ASN1ObjectIdentifier("1.2.840.10045.2.1"),
                new ASN1ObjectIdentifier("1.3.132.0.10"),
              )
            ),
            new DEROctetString(
              new DLSequence(
                Array[ASN1Encodable](
                  new ASN1Integer(1),
                  new DEROctetString(
                    Bytes
                      .assertFromString(
                        "7308c95bf6e240ed8de37b5a7c5f453d88ece2b5e93c02ef985e8553f856474a"
                      )
                      .toByteArray
                  ),
                  new DLTaggedObject(0, new ASN1ObjectIdentifier("1.3.132.0.10")),
                  new DLTaggedObject(
                    1,
                    new DERBitString(
                      new DEROctetString(
                        Bytes
                          .assertFromString(
                            "4ae6efb79de2cf60636219110f11b695d5c1776c0b0dad1468672fba1c6f6acf79396b8403e110cbf60ccd7aefab4c541d49844a51049fcbd22dae1a51d681"
                          )
                          .toByteArray
                      )
                    ),
                  ),
                )
              )
            ),
          )
        ),
      ),
    )

    forAll(testCases) { case (hexStr, derObj) =>
      DER.decode(Bytes.fromHexString(hexStr)) shouldBe derObj
    }
  }

  "correctly encode DER objects" in {
    val testCases = Table(
      ("DER Object", "Hex String"),
      (
        new DLSequence(
          Array[ASN1Encodable](
            new ASN1Integer(0),
            new DLSequence(
              Array[ASN1Encodable](
                new ASN1ObjectIdentifier("1.2.840.10045.2.1"),
                new ASN1ObjectIdentifier("1.3.132.0.10"),
              )
            ),
            new DEROctetString(
              new DLSequence(
                Array[ASN1Encodable](
                  new ASN1Integer(1),
                  new DEROctetString(
                    Bytes
                      .assertFromString(
                        "7308c95bf6e240ed8de37b5a7c5f453d88ece2b5e93c02ef985e8553f856474a"
                      )
                      .toByteArray
                  ),
                  new DLTaggedObject(0, new ASN1ObjectIdentifier("1.3.132.0.10")),
                  new DLTaggedObject(
                    1,
                    new DERBitString(
                      new DEROctetString(
                        Bytes
                          .assertFromString(
                            "4ae6efb79de2cf60636219110f11b695d5c1776c0b0dad1468672fba1c6f6acf79396b8403e110cbf60ccd7aefab4c541d49844a51049fcbd22dae1a51d681"
                          )
                          .toByteArray
                      )
                    ),
                  ),
                )
              )
            ),
          )
        ),
        Ref.HexString.assertFromString(
          "30818d020100301006072a8648ce3d020106052b8104000a0476307402010104207308c95bf6e240ed8de37b5a7c5f453d88ece2b5e93c02ef985e8553f856474aa00706052b8104000aa144034200043f4ae6efb79de2cf60636219110f11b695d5c1776c0b0dad1468672fba1c6f6acf79396b8403e110cbf60ccd7aefab4c541d49844a51049fcbd22dae1a51d681"
        ),
      ),
    )

    forAll(testCases) { case (derObj, hexStr) =>
      DER.encode(derObj) shouldBe Bytes.fromHexString(hexStr)
    }
  }
}
