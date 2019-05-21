// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as signature from "cookie-signature"
import * as cookie from "cookie"
import NodeCache from "node-cache"
import * as uid from "uid-safe"
import { Request, Response } from "express";

const conf = require('./config').read(),
      debug = require('debug')('webide:session'),
      store = new NodeCache({ stdTTL: conf.session.inactiveTimeout, checkperiod: 120, useClones: false })

type Callback<T> = (error?: any, result?: T) => void;
export type SaveSession = (state :any) => boolean;
type ReadSessionCallback = (err :any, result: any, sessionId :string) => void;
type SaveSessionCallback = (err :any, result: any, sessionId :string, save :SaveSession) => void;


/**
 * callback: function( value ) where value is the state of the session
 * is called when a session inactivetimeout occurs. This could be due to inactivity or cookie expiration
 */
export function onTimeout(callback: Callback<any>) {
  store.on( "del", ( key :string, value :any) => {
    console.log("session removed: %s", key)
    callback(value || {})
  });
}

/**
 * explicit synchronous getter in case we need state current state
 * @param sessionId 
 */
export function getStateSync(sessionId :string) :any {
  return store.get(sessionId)
}

/**
 * returns promise resolved to array of session entries :: [sessionId, state]
 */
export function allSessionEntries() {
  return new Promise((resolve, reject) => {
    store.keys((err, keys) => {
      if (err) reject(err)
      if (!keys) resolve([])
      store.mget(<string[]> keys, (err2, storeObj :any) => {
        if (err2) reject(err2)
        
        resolve(Object.entries(storeObj).map(e => {
          return {
            sessionId : e[0],
            container : e[1],
            ttl : store.getTtl(e[0])
          }
        }))
      })
    })
  })
}

/**
 * returns promise resolved to array of sessionIds
 */
export function allSessionIds() {
  return new Promise((resolve, reject) => {
    store.keys((err, keys) => {
      if (err) reject(err)
      else resolve(keys)
    })
  })
  
}

export function keepActive(sessionId :string) {
  store.ttl(sessionId, conf.session.inactiveTimeout)
}

export function close() {
  store.close()
}

/**
 * 
 * @param {*} req http req
 * @param {*} res http res (used to set session cookie)
 * @param {*} callback of the form function(err, state, sessionId, saveSession(state) => {}) 
 */
export function session(req :Request, res :Response, callback :SaveSessionCallback) {
    const sessionIdFromCookie = getSessionIdFromCookie(req)
    const sessionId = sessionIdFromCookie === undefined ? generateSessionId() : sessionIdFromCookie
    if (sessionIdFromCookie !== sessionId) setCookie(res, sessionId)
    handleSessionCallback(sessionId, callback);
}

/**
 * 
 * @param {*} req http request
 * @param {*} callback of the form function(err, state, sessionId) 
 */
export function readSession(req :Request, callback: ReadSessionCallback) {
  const sessionId = getSessionIdFromCookie(req)
  keepActive(sessionId)
  store.get(sessionId, (err, value) => {
    const state = (value || {})
    callback(err, state, sessionId)
  })
}

function handleSessionCallback(sessionId :string, callback :SaveSessionCallback) {
  store.get(sessionId, (err, value) => {
    keepActive(sessionId);
    callback(err, (value || {}), sessionId, (state) => {
      return save(sessionId, state);
    });
  });
}

function save(sessionId :string, state :any) :boolean {
    if (conf.session.timeout && !state._started) {
      state._started = Date.now()
      const delay = conf.session.timeout * 1000
      const timeout = setTimeout(() => {
        debug("session timeout occured, started %s", state._started)
        clearTimeout(timeout)
        store.del(sessionId)
      }, delay)
    }
    return store.set(sessionId, state, 0)
}

function generateSessionId() {
    return uid.sync(24);
}

//TODO use cookie-parser?
function getSessionIdFromCookie(req :Request) :string {
    const name = conf.session.name
    const header = req.headers.cookie;
    var val;
  
    // read from cookie header
    if (header) {
      const cookies = cookie.parse(header)
      const raw = cookies[name];
      if (raw) {
        if (raw.substr(0, 2) === 's:') {
          val = unsigncookie(raw.slice(2));
          if (val === false) {
            console.error('cookie signature invalid');
            val = undefined;
          }
        } else {
          console.error('cookie unsigned')
        }
      }
    }
  
    return <string>val;
  }
  
function setCookie(res :Response, val :string) {
    const name = conf.session.name
    const secret = conf.session.secret

    const signed = 's:' + signature.sign(val, secret);
    const data = cookie.serialize(name, signed, conf.session.cookie);
    const prev = res.getHeader('Set-Cookie') || []
    const header = Array.isArray(prev) ? (<string[]>prev).concat(data) : [prev, data];

    res.setHeader('Set-Cookie', header)
}

function unsigncookie(val :string) {
    return signature.unsign(val, conf.session.secret);
}
