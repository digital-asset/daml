-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.LanguageServer.Conduits
  ( contentLengthParserConduit
  , contentLengthEmitterConduit
  , instrumentConduit
  ) where

import           Control.Monad.Catch              (MonadThrow)
import           Control.Monad.Trans              (MonadIO)
import Control.Monad.IO.Class

import qualified Data.Attoparsec.ByteString.Char8 as AP
import qualified Data.ByteString.Char8            as B
import           Data.Conduit                     (ConduitT, (.|),
                                                   await, yield)
import qualified Data.Conduit.List                as CL
import           Data.Conduit.Attoparsec          (conduitParser)

import qualified DA.Service.Logger                as Logger


------------------------------------------------------------------------
-- Conduits for content-length framed messages
------------------------------------------------------------------------

-- | Conduit for parsing content-length delimited messages.
contentLengthParserConduit :: MonadThrow m => ConduitT B.ByteString B.ByteString m ()
contentLengthParserConduit = conduitParser parser .| CL.map snd
  where
    parser = "Content-Length: " *> AP.decimal <* "\r\n\r\n" >>= AP.take

-- | Conduit for emitting content-length delimited messages.
contentLengthEmitterConduit :: Monad m => ConduitT B.ByteString B.ByteString m ()
contentLengthEmitterConduit = do
    message <- await
    case message of
      Just msg -> do
          yield $ B.pack $ "Content-Length: " <> show (B.length msg) <> "\r\n\r\n"
          yield msg
          contentLengthEmitterConduit
      Nothing -> return ()


-- | Conduit for instrumenting message sizes using a given logger distribution.
instrumentConduit
  :: (Monad m, MonadIO m)
  => Logger.Distribution IO
  -> ConduitT B.ByteString B.ByteString m ()
instrumentConduit dist = do
    mbMsg <- await
    case mbMsg of
      Just msg -> do
          liftIO $ Logger.distributionAdd dist $ fromIntegral $ B.length msg
          yield msg
          instrumentConduit dist
      Nothing ->
          return ()
