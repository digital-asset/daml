-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}

module DA.Ledger.GrpcWrapUtils (
    noTrace, emptyMdm, unwrap, sendToStream
    ) where

import Prelude hiding (fail)

import Com.Digitalasset.Ledger.Api.V1.TraceContext (TraceContext)
import Control.Exception (throwIO)
import Control.Monad.Fail (fail)
import Control.Monad.Fix (fix)
import DA.Ledger.Stream
import Network.GRPC.HighLevel (clientCallCancel)
import Network.GRPC.HighLevel.Generated
import qualified Data.Map as Map

noTrace :: Maybe TraceContext
noTrace = Nothing

emptyMdm :: MetadataMap
emptyMdm = MetadataMap Map.empty

unwrap :: ClientResult 'Normal a -> IO a
unwrap = \case
    ClientNormalResponse x _m1 _m2 _status _details -> return x
    ClientErrorResponse (ClientIOError e) -> throwIO e
    ClientErrorResponse ce -> fail (show ce)

sendToStream :: Int -> a -> (b -> [Either Closed c]) -> Stream c -> (ClientRequest 'ServerStreaming a b -> IO (ClientResult 'ServerStreaming b)) -> IO ()
sendToStream timeout request f stream rpc = do
    ClientReaderResponse _meta _code _details <- rpc $
        ClientReaderRequestCC request timeout emptyMdm
        (\cc -> onClose stream $ \_cancel -> clientCallCancel cc)
        $ \_mdm recv -> do
          fix $ \again -> do
            recv >>= \case
                Left e -> do
                    writeStream stream (Left (Abnormal (show e)))
                    return ()
                Right Nothing -> do
                    writeStream stream (Left EOS)
                    return ()
                Right (Just x) ->
                    do
                        mapM_ (writeStream stream) (f x)
                        again
    return ()
