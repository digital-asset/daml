-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger ( -- High level interface to the Ledger API
    Port(..), Host(..), ClientConfig(..), ClientSSLConfig(..), ClientSSLKeyCertPair(..),

    module DA.Ledger.LedgerService,
    module DA.Ledger.Services,
    module DA.Ledger.Types,

    configOfPort,
    configOfHostAndPort,
    ) where

import Network.GRPC.HighLevel.Generated(Port(..),Host(..),ClientConfig(..))
import Network.GRPC.HighLevel.Client (ClientSSLConfig(..), ClientSSLKeyCertPair(..))
import Network.GRPC.LowLevel.Call (endpoint)
import Network.GRPC.Unsafe.ChannelArgs (Arg(..))
import DA.Ledger.LedgerService
import DA.Ledger.Services
import DA.Ledger.Types

-- | Note: This does not enable TLS
configOfPort :: Port -> ClientConfig
configOfPort port = configOfHostAndPort "localhost" port [] Nothing

configOfHostAndPort :: Host -> Port -> [Arg] -> Maybe ClientSSLConfig -> ClientConfig
configOfHostAndPort host port args sslConfig =
    ClientConfig { clientServerEndpoint = endpoint host port
                 , clientArgs = args
                 , clientSSLConfig = sslConfig
                 , clientAuthority = Nothing
                 }
