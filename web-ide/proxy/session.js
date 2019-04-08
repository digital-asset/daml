// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

const signature = require('cookie-signature'),
      cookie = require('cookie'),
      uid = require('uid-safe').sync,
      NodeCache = require( "node-cache" ),
      config = require('./config.json'),

      store = new NodeCache({ stdTTL: config.session.inactiveTimeout, checkperiod: 120, useClones: false })


module.exports = {
  allSessionIds: allSessionIds,
  allSessionEntries: allSessionEntries,
  close: close,
  remove: remove,
  keepActive: keepActive,
  session: session,
  readSession: readSession,
  onTimeout: onTimeout
} 

/**
 * callback: function( value ) where value is the state of the session
 * is called when a session inactivetimeout occurs. This could be due to inactivity or cookie expiration
 */
function onTimeout(callback) {
  store.on( "del", function( key, value ){
    console.log("session removed: %s", key)
    callback(value || {})
  });
}

function remove(sessionId) {
  store.del(sessionId)
}

/**
 * returns promise resolved to array of session entries :: [sessionId, state]
 */
function allSessionEntries() {
  return new Promise((resolve, reject) => {
    store.keys((err, keys) => {
      if (err) reject(err)
      if (!keys) resolve([])
      store.mget(keys, (err2, storeObj) => {
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
function allSessionIds() {
  return new Promise((resolve, reject) => {
    store.keys((err, keys) => {
      if (err) reject(err)
      else resolve(keys)
    })
  })
  
}

function keepActive(sessionId) {
  store.ttl(sessionId, config.session.inactiveTimeout)
}

function close() {
  store.close()
}

/**
 * 
 * @param {*} req http req
 * @param {*} res http res (used to set session cookie)
 * @param {*} callback of the form function(err, state, sessionId, saveSession(state) => {}) 
 */
function session(req, res, callback) {
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
function readSession(req, callback) {
  const sessionId = getSessionIdFromCookie(req)
  keepActive(sessionId)
  store.get(sessionId, (err, value) => {
    const state = (value || {})
    callback(err, state, sessionId)
  })
}

function handleSessionCallback(sessionId, callback) {
  store.get(sessionId, (err, value) => {
    keepActive(sessionId);
    callback(err, (value || {}), sessionId, (state) => {
      //console.log("saving state %s : %O", sessionId, state)
      save(sessionId, state);
    });
  });
}

function save(sessionId, state) {
    if (config.session.timeout && !state._started) {
      state._started = Date.now()
      const delay = config.session.timeout * 1000
      //console.log("session started %s, ending in %s seconds", state._started, config.session.timeout)
      const timeout = setTimeout(() => {
        console.log("session timeout occured, started %s", state._started)
        clearTimeout(timeout)
        store.del(sessionId)
      }, delay)
    }
    store.set(sessionId, state)
}

function generateSessionId() {
    return uid(24);
}

function getSessionIdFromCookie(req) {
    const name = config.session.name
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
            console.log('cookie signature invalid');
            val = undefined;
          }
        } else {
          console.log('cookie unsigned')
        }
      }
    }
  
    return val;
  }
  
function setCookie(res, val) {
    const name = config.session.name
    const secret = config.session.secret

    const signed = 's:' + signature.sign(val, secret);
    const data = cookie.serialize(name, signed, config.session.cookie);
    const prev = res.getHeader('Set-Cookie') || []
    const header = Array.isArray(prev) ? prev.concat(data) : [prev, data];

    res.setHeader('Set-Cookie', header)
}

function unsigncookie(val) {
    return signature.unsign(val, config.session.secret);
}
