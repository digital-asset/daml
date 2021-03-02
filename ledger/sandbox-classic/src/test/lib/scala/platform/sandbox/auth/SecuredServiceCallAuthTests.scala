package com.daml.platform.sandbox.auth

trait SecuredServiceCallAuthTests extends ServiceCallAuthTests {
  behavior of serviceCallName

  it should "deny unauthenticated calls" in {
    expectUnauthenticated(serviceCallWithToken(None))
  }
}
