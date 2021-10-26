// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import com.daml.lf.data.Ref.{PackageId, PackageName}

object StablePackages {
  // Based on compiler/damlc/tests/src/stable-packages.sh
  // TODO: Use this map to generate the mapping used in the test,
  // or generate both from a single source.
  private[this] def nameToIdMap: Map[PackageName, PackageId] = Map(
    PackageName.assertFromString(
      "daml-prim-DA-Exception-ArithmeticError-cb0552debf219cc909f51cbb5c3b41e9981d39f8f645b1f35e2ef5be2e0b858a"
    ) -> PackageId.assertFromString(
      "cb0552debf219cc909f51cbb5c3b41e9981d39f8f645b1f35e2ef5be2e0b858a"
    ),
    PackageName.assertFromString(
      "daml-prim-DA-Exception-AssertionFailed-3f4deaf145a15cdcfa762c058005e2edb9baa75bb7f95a4f8f6f937378e86415"
    ) -> PackageId.assertFromString(
      "3f4deaf145a15cdcfa762c058005e2edb9baa75bb7f95a4f8f6f937378e86415"
    ),
    PackageName.assertFromString(
      "daml-prim-DA-Exception-GeneralError-86828b9843465f419db1ef8a8ee741d1eef645df02375ebf509cdc8c3ddd16cb"
    ) -> PackageId.assertFromString(
      "86828b9843465f419db1ef8a8ee741d1eef645df02375ebf509cdc8c3ddd16cb"
    ),
    PackageName.assertFromString(
      "daml-prim-DA-Exception-PreconditionFailed-f20de1e4e37b92280264c08bf15eca0be0bc5babd7a7b5e574997f154c00cb78"
    ) -> PackageId.assertFromString(
      "f20de1e4e37b92280264c08bf15eca0be0bc5babd7a7b5e574997f154c00cb78"
    ),
    PackageName.assertFromString(
      "daml-prim-DA-Internal-Erased-76bf0fd12bd945762a01f8fc5bbcdfa4d0ff20f8762af490f8f41d6237c6524f"
    ) -> PackageId.assertFromString(
      "76bf0fd12bd945762a01f8fc5bbcdfa4d0ff20f8762af490f8f41d6237c6524f"
    ),
    PackageName.assertFromString(
      "daml-prim-DA-Internal-NatSyn-38e6274601b21d7202bb995bc5ec147decda5a01b68d57dda422425038772af7"
    ) -> PackageId.assertFromString(
      "38e6274601b21d7202bb995bc5ec147decda5a01b68d57dda422425038772af7"
    ),
    PackageName.assertFromString(
      "daml-prim-DA-Internal-PromotedText-d58cf9939847921b2aab78eaa7b427dc4c649d25e6bee3c749ace4c3f52f5c97"
    ) -> PackageId.assertFromString(
      "d58cf9939847921b2aab78eaa7b427dc4c649d25e6bee3c749ace4c3f52f5c97"
    ),
    PackageName.assertFromString(
      "daml-prim-DA-Types-40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7"
    ) -> PackageId.assertFromString(
      "40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7"
    ),
    PackageName.assertFromString(
      "daml-prim-GHC-Prim-e491352788e56ca4603acc411ffe1a49fefd76ed8b163af86cf5ee5f4c38645b"
    ) -> PackageId.assertFromString(
      "e491352788e56ca4603acc411ffe1a49fefd76ed8b163af86cf5ee5f4c38645b"
    ),
    PackageName.assertFromString(
      "daml-prim-GHC-Tuple-6839a6d3d430c569b2425e9391717b44ca324b88ba621d597778811b2d05031d"
    ) -> PackageId.assertFromString(
      "6839a6d3d430c569b2425e9391717b44ca324b88ba621d597778811b2d05031d"
    ),
    PackageName.assertFromString(
      "daml-prim-GHC-Types-518032f41fd0175461b35ae0c9691e08b4aea55e62915f8360af2cc7a1f2ba6c"
    ) -> PackageId.assertFromString(
      "518032f41fd0175461b35ae0c9691e08b4aea55e62915f8360af2cc7a1f2ba6c"
    ),
    PackageName.assertFromString(
      "daml-stdlib-DA-Date-Types-bfcd37bd6b84768e86e432f5f6c33e25d9e7724a9d42e33875ff74f6348e733f"
    ) -> PackageId.assertFromString(
      "bfcd37bd6b84768e86e432f5f6c33e25d9e7724a9d42e33875ff74f6348e733f"
    ),
    PackageName.assertFromString(
      "daml-stdlib-DA-Internal-Any-cc348d369011362a5190fe96dd1f0dfbc697fdfd10e382b9e9666f0da05961b7"
    ) -> PackageId.assertFromString(
      "cc348d369011362a5190fe96dd1f0dfbc697fdfd10e382b9e9666f0da05961b7"
    ),
    PackageName.assertFromString(
      "daml-stdlib-DA-Internal-Down-057eed1fd48c238491b8ea06b9b5bf85a5d4c9275dd3f6183e0e6b01730cc2ba"
    ) -> PackageId.assertFromString(
      "057eed1fd48c238491b8ea06b9b5bf85a5d4c9275dd3f6183e0e6b01730cc2ba"
    ),
    PackageName.assertFromString(
      "daml-stdlib-DA-Internal-Template-d14e08374fc7197d6a0de468c968ae8ba3aadbf9315476fd39071831f5923662"
    ) -> PackageId.assertFromString(
      "d14e08374fc7197d6a0de468c968ae8ba3aadbf9315476fd39071831f5923662"
    ),
    PackageName.assertFromString(
      "daml-stdlib-DA-Logic-Types-c1f1f00558799eec139fb4f4c76f95fb52fa1837a5dd29600baa1c8ed1bdccfd"
    ) -> PackageId.assertFromString(
      "c1f1f00558799eec139fb4f4c76f95fb52fa1837a5dd29600baa1c8ed1bdccfd"
    ),
    PackageName.assertFromString(
      "daml-stdlib-DA-Monoid-Types-6c2c0667393c5f92f1885163068cd31800d2264eb088eb6fc740e11241b2bf06"
    ) -> PackageId.assertFromString(
      "6c2c0667393c5f92f1885163068cd31800d2264eb088eb6fc740e11241b2bf06"
    ),
    PackageName.assertFromString(
      "daml-stdlib-DA-NonEmpty-Types-e22bce619ae24ca3b8e6519281cb5a33b64b3190cc763248b4c3f9ad5087a92c"
    ) -> PackageId.assertFromString(
      "e22bce619ae24ca3b8e6519281cb5a33b64b3190cc763248b4c3f9ad5087a92c"
    ),
    PackageName.assertFromString(
      "daml-stdlib-DA-Semigroup-Types-8a7806365bbd98d88b4c13832ebfa305f6abaeaf32cfa2b7dd25c4fa489b79fb"
    ) -> PackageId.assertFromString(
      "8a7806365bbd98d88b4c13832ebfa305f6abaeaf32cfa2b7dd25c4fa489b79fb"
    ),
    PackageName.assertFromString(
      "daml-stdlib-DA-Set-Types-97b883cd8a2b7f49f90d5d39c981cf6e110cf1f1c64427a28a6d58ec88c43657"
    ) -> PackageId.assertFromString(
      "97b883cd8a2b7f49f90d5d39c981cf6e110cf1f1c64427a28a6d58ec88c43657"
    ),
    PackageName.assertFromString(
      "daml-stdlib-DA-Time-Types-733e38d36a2759688a4b2c4cec69d48e7b55ecc8dedc8067b815926c917a182a"
    ) -> PackageId.assertFromString(
      "733e38d36a2759688a4b2c4cec69d48e7b55ecc8dedc8067b815926c917a182a"
    ),
    PackageName.assertFromString(
      "daml-stdlib-DA-Validation-Types-99a2705ed38c1c26cbb8fe7acf36bbf626668e167a33335de932599219e0a235"
    ) -> PackageId.assertFromString(
      "99a2705ed38c1c26cbb8fe7acf36bbf626668e167a33335de932599219e0a235"
    ),
  )
  val Ids: Set[PackageId] = nameToIdMap.values.toSet
}
