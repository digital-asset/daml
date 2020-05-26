// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.util;

import com.daml.ledger.rxjava.components.Bot;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowableLogger {

    private final static Logger logger = LoggerFactory.getLogger(Bot.class);


    public static <A> Flowable<A> log(Flowable<A> flowable, String name) {
        if (logger.isDebugEnabled()) {
            return flowable
                    .doOnError(t -> logger.error(name + ".onError: " + t))
                    .doOnComplete(() -> logger.debug(name + ".complete"))
                    .doOnNext(a -> logger.debug(name + ".next: " + a))
                    .doOnCancel(() -> logger.debug(name + ".cancel"))
                    .doOnRequest(request -> logger.debug(name + ".request: " + request))
                    .doOnSubscribe(s -> logger.debug(name + ".subscribe: " + s))
                    .doFinally(() -> logger.debug(name + ".finally"))
                    .doOnTerminate(() -> logger.debug(name + ".terminate"))
                    ;
        } else {
            return flowable;
        }
    }

    public static <A> Maybe<A> log(Maybe<A> flowable, String name) {
        if (logger.isDebugEnabled()) {
            return flowable
                    .doOnError(t -> logger.error(name + ".onError: " + t))
                    .doOnComplete(() -> logger.debug(name + ".complete"))
                    .doOnSuccess(a -> logger.debug(name + ".success: " + a))
                    .doOnDispose(() -> logger.debug(name + ".dispose"))
                    .doOnSubscribe(s -> logger.debug(name + ".subscribe: " + s))
                    .doFinally(() -> logger.debug(name + ".finally"))
                    ;
        } else {
            return flowable;
        }
    }
}
