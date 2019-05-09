// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.EventOuterClass;
import com.digitalasset.ledger.api.v1.TransactionOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

/**
 * @see FlatEvent
 * @see TreeEvent
 * @see <a href="https://github.com/digital-asset/daml/issues/960">#960</a>
 * @deprecated This class is deprecated in favour of the interfaces {@link FlatEvent} used by {@link Transaction}
 * and {@link TreeEvent} used by {@link TransactionTree}.
 */
@Deprecated
public abstract class Event {

    @NonNull
    public abstract List<@NonNull String> getWitnessParties();

    @NonNull
    public abstract String getEventId();

    @NonNull
    public abstract Identifier getTemplateId();

    @NonNull
    public abstract String getContractId();


    public static TreeEvent fromProtoTreeEvent(TransactionOuterClass.TreeEvent event) {
        return TreeEvent.fromProtoTreeEvent(event);
    }

    public static FlatEvent fromProtoEvent(EventOuterClass.Event event) {
        return FlatEvent.fromProtoEvent(event);
    }
}

class UnsupportedEventTypeException extends RuntimeException {
    public UnsupportedEventTypeException(String eventStr) {
        super("Unsupported event " + eventStr);
    }
}
