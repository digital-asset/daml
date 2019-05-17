package com.digitalasset.ledger.server.authentication.api;

public interface LedgerApiAuthenticator {
    boolean authenticate(String authenticationData);
}
