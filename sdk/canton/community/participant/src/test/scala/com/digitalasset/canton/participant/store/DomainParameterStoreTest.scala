// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.topology.{DomainId, UniqueIdentifier}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AsyncWordSpec

trait DomainParameterStoreTest { this: AsyncWordSpec & BaseTest =>

  private val domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domainId::domainId"))

  private def anotherProtocolVersion(testedProtocolVersion: ProtocolVersion): ProtocolVersion =
    if (testedProtocolVersion.isDev)
      ProtocolVersion.minimum
    else
      ProtocolVersion.dev

  def domainParameterStore(mk: DomainId => DomainParameterStore): Unit = {

    "setParameters" should {
      "store new parameters" in {
        val store = mk(domainId)
        val params = defaultStaticDomainParameters
        for {
          _ <- store.setParameters(params)
          last <- store.lastParameters
        } yield {
          last shouldBe Some(params)
        }
      }

      "be idempotent" in {
        val store = mk(domainId)
        val params =
          BaseTest.defaultStaticDomainParametersWith(protocolVersion =
            anotherProtocolVersion(testedProtocolVersion)
          )
        for {
          _ <- store.setParameters(params)
          _ <- store.setParameters(params)
          last <- store.lastParameters
        } yield {
          last shouldBe Some(params)
        }
      }

      "not overwrite changed domain parameters" in {
        val store = mk(domainId)
        val params = defaultStaticDomainParameters
        val modified =
          BaseTest.defaultStaticDomainParametersWith(protocolVersion =
            anotherProtocolVersion(testedProtocolVersion)
          )
        for {
          _ <- store.setParameters(params)
          ex <- store.setParameters(modified).failed
          last <- store.lastParameters
        } yield {
          ex shouldBe an[IllegalArgumentException]
          last shouldBe Some(params)
        }
      }
    }

    "lastParameters" should {
      "return None for the empty store" in {
        val store = mk(domainId)
        for {
          last <- store.lastParameters
        } yield {
          last shouldBe None
        }
      }
    }
  }
}
