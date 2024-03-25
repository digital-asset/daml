-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.FreePort (getFreePort) where

import Control.Exception (bracket)
import Network.Socket

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

