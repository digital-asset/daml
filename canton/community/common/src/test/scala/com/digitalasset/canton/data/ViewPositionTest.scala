// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.Order
import cats.instances.order.*
import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndex.Direction
import com.digitalasset.canton.data.ViewPosition.{
  ListIndex,
  MerklePathElement,
  MerkleSeqIndex,
  MerkleSeqIndexFromRoot,
}

class ViewPositionTest extends BaseTestWordSpec {

  def mkIndex(i: Int): MerkleSeqIndex = MerkleSeqIndex(List.fill(i)(MerkleSeqIndex.Direction.Left))

  "Correctly determine descendants" in {
    val p1 = ViewPosition(List(mkIndex(1)))
    val p2 = ViewPosition(List(mkIndex(2), mkIndex(1)))
    val p3 = ViewPosition(List(mkIndex(3), mkIndex(1)))
    val p4 = ViewPosition(List(mkIndex(4), mkIndex(3), mkIndex(1)))

    ViewPosition.isDescendant(p1, p1) shouldBe true
    ViewPosition.isDescendant(p2, p1) shouldBe true
    ViewPosition.isDescendant(p3, p1) shouldBe true
    ViewPosition.isDescendant(p4, p1) shouldBe true

    ViewPosition.isDescendant(p1, p2) shouldBe false
    ViewPosition.isDescendant(p3, p2) shouldBe false
    ViewPosition.isDescendant(p4, p2) shouldBe false

    ViewPosition.isDescendant(p1, p3) shouldBe false
    ViewPosition.isDescendant(p2, p3) shouldBe false
    ViewPosition.isDescendant(p4, p3) shouldBe true

    ViewPosition.isDescendant(p1, p4) shouldBe false
    ViewPosition.isDescendant(p2, p4) shouldBe false
    ViewPosition.isDescendant(p3, p4) shouldBe false
  }

  "Correctly order positions" in {
    import Direction.*

    implicit val orderViewPosition: Order[ViewPosition] = ViewPosition.orderViewPosition
    implicit val orderMerklePathElement: Order[MerklePathElement] =
      ViewPosition.MerklePathElement.orderMerklePathElement

    Left should be < (Right: Direction)

    MerkleSeqIndex(List.empty) should be < (MerkleSeqIndex(List(Left)): MerklePathElement)
    MerkleSeqIndex(List(Left)) should be < (MerkleSeqIndex(List(Left, Left)): MerklePathElement)
    MerkleSeqIndex(List(Left, Left)) should be
      < (MerkleSeqIndex(List(Right, Left)): MerklePathElement)
    MerkleSeqIndex(List(Right, Left)) should be
      < (MerkleSeqIndex(List(Left, Right)): MerklePathElement)
    MerkleSeqIndex(List(Left, Right)) should be
      < (MerkleSeqIndex(List(Right, Right)): MerklePathElement)

    MerklePathElement.orderMerklePathElement.compare(
      MerkleSeqIndexFromRoot(List(Left, Right)),
      MerkleSeqIndex(List(Right, Left)),
    ) shouldBe 0

    an[UnsupportedOperationException] shouldBe thrownBy {
      MerklePathElement.orderMerklePathElement.compare(
        ListIndex(2),
        MerkleSeqIndex(List(Left, Right)),
      )
    }

    ListIndex(1) should be < (ListIndex(2): MerklePathElement)

    ViewPosition(List(MerkleSeqIndex(List(Right)), MerkleSeqIndex(List(Left)))) should be
      < ViewPosition(List(MerkleSeqIndex(List(Left)), MerkleSeqIndex(List(Left, Left))))
  }
}
