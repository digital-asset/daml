-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Network.Socket.Extended
    ( module Network.Socket
    , getFreePort
    , socketHints
    ) where

import Control.Exception (bracket)
import Network.Socket

-- This is slightly hacky: we need to find a free port but pass it to an
-- external process. Technically this port could be reused between us
-- getting it from the kernel and the external process listening
-- on that port but ports are usually not reused aggressively so this should
-- be fine and is certainly better than hardcoding the port.
getFreePort :: IO PortNumber
getFreePort = do
    addr : _ <- getAddrInfo
        (Just socketHints)
        (Just "127.0.0.1")
        (Just "0")
    bracket
        (socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr))
        close
        (\s -> do bind s (addrAddress addr)
                  name <- getSocketName s
                  case name of
                      SockAddrInet p _ -> pure p
                      _ -> fail $ "Expected a SockAddrInet but got " <> show name)

socketHints :: AddrInfo
socketHints = defaultHints { addrFlags = [AI_NUMERICHOST, AI_NUMERICSERV], addrSocketType = Stream }
