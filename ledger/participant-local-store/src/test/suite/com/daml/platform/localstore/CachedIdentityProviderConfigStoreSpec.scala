// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.metrics.Metrics
import com.daml.platform.localstore.api.{IdentityProviderConfigStore, IdentityProviderConfigUpdate}
import com.daml.platform.localstore.api.IdentityProviderConfigStore.{
  IdentityProviderConfigByIssuerNotFound,
  IdentityProviderConfigNotFound,
}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.freespec.AsyncFreeSpec
import scala.concurrent.duration._

class CachedIdentityProviderConfigStoreSpec
    extends AsyncFreeSpec
    with IdentityProviderConfigStoreTests
    with MockitoSugar
    with ArgumentMatchersSugar {

  override def newStore(): IdentityProviderConfigStore = createTested(
    new InMemoryIdentityProviderConfigStore()
  )

  private def createTested(
      delegate: IdentityProviderConfigStore
  ): CachedIdentityProviderConfigStore =
    new CachedIdentityProviderConfigStore(
      delegate,
      cacheExpiryAfterWrite = 1.second,
      maximumCacheSize = 10,
      Metrics.ForTesting,
    )

  "test identity-provider-config cache result gets invalidated after new config creation" in {
    val delegate = spy(new InMemoryIdentityProviderConfigStore())
    val tested = createTested(delegate)
    val cfg = config()
    for {
      getYetNonExistent <- tested.getIdentityProviderConfig(cfg.identityProviderId)
      _ <- tested.createIdentityProviderConfig(cfg)
      get <- tested.getIdentityProviderConfig(cfg.identityProviderId)
    } yield {
      getYetNonExistent shouldBe Left(IdentityProviderConfigNotFound(cfg.identityProviderId))
      get.value shouldBe cfg
    }
  }

  "test cache population" in {
    val delegate = spy(new InMemoryIdentityProviderConfigStore())
    val tested = createTested(delegate)
    val cfg = config()
    for {
      _ <- tested.createIdentityProviderConfig(cfg)
      res1 <- tested.getIdentityProviderConfig(cfg.issuer)
      res2 <- tested.getIdentityProviderConfig(cfg.issuer)
      res3 <- tested.getIdentityProviderConfig(cfg.issuer)
      res4 <- tested.listIdentityProviderConfigs()
      res5 <- tested.listIdentityProviderConfigs()
    } yield {
      verify(delegate, times(1)).getIdentityProviderConfig(cfg.issuer)
      verify(delegate, times(2)).listIdentityProviderConfigs()
      res1.value shouldBe cfg
      res2.value shouldBe cfg
      res3.value shouldBe cfg
      res4.value shouldBe Vector(cfg)
      res5.value shouldBe Vector(cfg)
    }
  }

  "test cache invalidation after every write method" in {
    val delegate = spy(new InMemoryIdentityProviderConfigStore())
    val tested = createTested(delegate)
    val cfg = config()
    for {
      _ <- tested.createIdentityProviderConfig(cfg)
      res1 <- tested.getIdentityProviderConfig(cfg.identityProviderId)
      res2 <- tested.getIdentityProviderConfig(cfg.issuer)
      res3 <- tested.updateIdentityProviderConfig(
        IdentityProviderConfigUpdate(
          cfg.identityProviderId,
          isDeactivatedUpdate = Some(true),
        )
      )
      res4 <- tested.getIdentityProviderConfig(cfg.identityProviderId)
      res5 <- tested.getIdentityProviderConfig(cfg.issuer)
      res6 <- tested.deleteIdentityProviderConfig(cfg.identityProviderId)
      res7 <- tested.getIdentityProviderConfig(cfg.identityProviderId)
      res8 <- tested.getIdentityProviderConfig(cfg.issuer)
    } yield {
      val order = inOrder(delegate)
      order.verify(delegate, times(1)).createIdentityProviderConfig(cfg)
      order.verify(delegate, times(1)).getIdentityProviderConfig(cfg.identityProviderId)
      order.verify(delegate, times(1)).getIdentityProviderConfig(cfg.issuer)
      order
        .verify(delegate, times(1))
        .updateIdentityProviderConfig(
          IdentityProviderConfigUpdate(
            cfg.identityProviderId,
            isDeactivatedUpdate = Some(true),
          )
        )
      order.verify(delegate, times(1)).getIdentityProviderConfig(cfg.identityProviderId)
      order.verify(delegate, times(1)).getIdentityProviderConfig(cfg.issuer)
      order.verify(delegate, times(1)).deleteIdentityProviderConfig(cfg.identityProviderId)
      order.verify(delegate, times(1)).getIdentityProviderConfig(cfg.identityProviderId)
      order.verify(delegate, times(1)).getIdentityProviderConfig(cfg.issuer)
      order.verifyNoMoreInteractions()
      res1.value shouldBe cfg
      res2.value shouldBe cfg
      res3.value shouldBe cfg.copy(isDeactivated = true)
      res4.value shouldBe cfg.copy(isDeactivated = true)
      res5.value shouldBe cfg.copy(isDeactivated = true)
      res6.value shouldBe ()
      res7 shouldBe Left(IdentityProviderConfigNotFound(cfg.identityProviderId))
      res8 shouldBe Left(IdentityProviderConfigByIssuerNotFound(cfg.issuer))
    }
  }

  "listing all users should not be cached" in {
    val delegate = spy(new InMemoryIdentityProviderConfigStore())
    val tested = createTested(delegate)
    val cfg1 = config()
    val cfg2 = config()
    for {
      _ <- tested.createIdentityProviderConfig(cfg1)
      _ <- tested.createIdentityProviderConfig(cfg2)
      res1 <- tested.listIdentityProviderConfigs()
      res2 <- tested.listIdentityProviderConfigs()
      res3 <- tested.listIdentityProviderConfigs()
    } yield {
      verify(delegate, times(3)).listIdentityProviderConfigs()
      res1.value should contain theSameElementsAs Vector(cfg1, cfg2)
      res2.value should contain theSameElementsAs Vector(cfg1, cfg2)
      res3.value should contain theSameElementsAs Vector(cfg1, cfg2)
    }
  }

  "cache entries expire after a set time" in {
    val delegate = spy(new InMemoryIdentityProviderConfigStore())
    val tested = createTested(delegate)
    val cfg = config()
    for {
      _ <- tested.createIdentityProviderConfig(cfg)
      res1 <- tested.getIdentityProviderConfig(cfg.identityProviderId)
      res1iss <- tested.getIdentityProviderConfig(cfg.issuer)
      res2 <- tested.listIdentityProviderConfigs()

      res3 <- {
        Thread.sleep(2000)
        tested.getIdentityProviderConfig(cfg.identityProviderId)
      }
      res3iss <- tested.getIdentityProviderConfig(cfg.issuer)
      res4 <- tested.listIdentityProviderConfigs()
    } yield {
      verify(delegate, times(2)).getIdentityProviderConfig(cfg.identityProviderId)
      verify(delegate, times(2)).getIdentityProviderConfig(cfg.issuer)
      verify(delegate, times(2)).listIdentityProviderConfigs()
      res1.value shouldBe cfg
      res1iss.value shouldBe cfg
      res2.value shouldBe Vector(cfg)
      res3.value shouldBe cfg
      res3iss.value shouldBe cfg
      res4.value shouldBe Vector(cfg)
    }
  }
}
