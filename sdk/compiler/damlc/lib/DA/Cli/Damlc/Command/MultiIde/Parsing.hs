-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE ApplicativeDo       #-}
{-# LANGUAGE RankNTypes       #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE GADTs #-}

module DA.Cli.Damlc.Command.MultiIde.Parsing (
  onChunks,
  parseClientMessageWithTracker,
  parseServerMessageWithTracker,
  putChunk,
  putReqMethodAll,
  putReqMethodSingleFromClient,
  putReqMethodSingleFromServer,
  putServerReq,
) where

import Control.Concurrent.STM.TVar
import Control.Lens
import Control.Monad.STM
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import qualified Data.Attoparsec.ByteString.Lazy as Attoparsec
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSLC
import Data.Foldable (forM_)
import DA.Cli.Damlc.Command.MultiIde.Types
import Data.Functor.Product
import qualified Data.IxMap as IM
import Data.List (delete)
import Data.Maybe (fromMaybe)
import qualified Language.LSP.Types as LSP
import System.IO.Extra

-- Missing from Data.Attoparsec.ByteString.Lazy, copied from Data.Attoparsec.ByteString.Char8
decimal :: Attoparsec.Parser Int
decimal = B.foldl' step 0 `fmap` Attoparsec.takeWhile1 (\w -> w - 48 <= 9)
  where step a w = a * 10 + fromIntegral (w - 48)

contentChunkParser :: Attoparsec.Parser B.ByteString
contentChunkParser = do
  _ <- Attoparsec.string "Content-Length: "
  len <- decimal
  _ <- Attoparsec.string "\r\n\r\n"
  Attoparsec.take len

-- Runs a handler on chunks as they come through the handle
onChunks :: Handle -> (B.ByteString -> IO ()) -> IO ()
onChunks handle act =
  let handleResult bytes =
        case Attoparsec.parse contentChunkParser bytes of
          Attoparsec.Done leftovers result -> act result >> handleResult leftovers
          Attoparsec.Fail _ _ err -> error $ "Chunk parse failed: " <> err
   in BSL.hGetContents handle >>= handleResult

putChunk :: Handle -> BSL.ByteString -> IO ()
putChunk handle payload = do
  let fullMessage = "Content-Length: " <> BSLC.pack (show (BSL.length payload)) <> "\r\n\r\n" <> payload
  BSL.hPut handle fullMessage
  hFlush handle

putReqMethodSingleFromServer
  :: forall (m :: LSP.Method 'LSP.FromServer 'LSP.Request)
  .  MethodTrackerVar 'LSP.FromServer -> FilePath -> LSP.LspId m -> LSP.SMethod m -> IO ()
putReqMethodSingleFromServer tracker home id method = putReqMethod tracker id $ TrackedSingleMethodFromServer method home

putReqMethodSingleFromClient
  :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
  .  MethodTrackerVar 'LSP.FromClient -> LSP.LspId m -> LSP.SMethod m -> IO ()
putReqMethodSingleFromClient tracker id method = putReqMethod tracker id $ TrackedSingleMethodFromClient method

putReqMethodAll
  :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
  .  MethodTrackerVar 'LSP.FromClient
  -> LSP.LspId m
  -> LSP.SMethod m
  -> [FilePath]
  -> ResponseCombiner m
  -> IO ()
putReqMethodAll tracker id method ides combine =
  putReqMethod tracker id $ TrackedAllMethod method id combine ides []

putReqMethod
  :: forall (f :: LSP.From) (m :: LSP.Method f 'LSP.Request)
  .  MethodTrackerVar f -> LSP.LspId m -> TrackedMethod m -> IO ()
putReqMethod tracker id method = atomically $ modifyTVar' tracker $ \im ->
  fromMaybe im $ IM.insertIxMap id method im

pickReqMethodTo
  :: forall (f :: LSP.From) r
  .  MethodTrackerVar f
  -> ((forall (m :: LSP.Method f 'LSP.Request)
        . LSP.LspId m
        -> (Maybe (TrackedMethod m), MethodTracker f)
      ) -> (r, Maybe (MethodTracker f)))
  -> IO r
pickReqMethodTo tracker handler = atomically $ do
  im <- readTVar tracker
  let (r, mayNewIM) = handler (flip IM.pickFromIxMap im)
  forM_ mayNewIM $ writeTVar tracker
  pure r

-- We're forced to give a result of type `(SMethod m, a m)` by parseServerMessage and parseClientMessage, but we want to include the updated MethodTracker
-- so we use Product to ensure our result has the SMethod and our MethodTracker
wrapParseMessageLookup
  :: forall (f :: LSP.From) (m :: LSP.Method f 'LSP.Request)
  .  (Maybe (TrackedMethod m), MethodTracker f)
  -> Maybe
      ( LSP.SMethod m
      , Product TrackedMethod (Const (MethodTracker f)) m
      )
wrapParseMessageLookup (mayTM, newIM) =
  fmap (\tm -> (tmMethod tm, Pair tm (Const newIM))) mayTM

-- Parses a message from the server providing context about previous requests from client
-- allowing the server parser to reconstruct typed responses to said requests
-- Handles TrackedAllMethod by returning Nothing for messages that do not have enough replies yet.
parseServerMessageWithTracker :: MethodTrackerVar 'LSP.FromClient -> FilePath -> Aeson.Value -> IO (Either String (Maybe LSP.FromServerMessage))
parseServerMessageWithTracker tracker selfIde val = pickReqMethodTo tracker $ \extract ->
  case Aeson.parseEither (LSP.parseServerMessage (wrapParseMessageLookup . extract)) val of
    Right (LSP.FromServerMess meth mess) -> (Right (Just $ LSP.FromServerMess meth mess), Nothing)
    Right (LSP.FromServerRsp (Pair (TrackedSingleMethodFromClient method) (Const newIxMap)) rsp) -> (Right (Just (LSP.FromServerRsp method rsp)), Just newIxMap)
    -- Multi reply logic, for requests that are sent to all IDEs with responses unified. Required for some queries
    Right (LSP.FromServerRsp (Pair tm@TrackedAllMethod {} (Const newIxMap)) rsp) -> do
      -- Haskell gets a little confused when updating existential records, so we need to build a new one
      let tm' = TrackedAllMethod
                  { tamMethod = tamMethod tm
                  , tamLspId = tamLspId tm
                  , tamCombiner = tamCombiner tm
                  , tamResponses = (selfIde, LSP._result rsp) : tamResponses tm
                  , tamRemainingResponseIDERoots = delete selfIde $ tamRemainingResponseIDERoots tm
                  }
      if null $ tamRemainingResponseIDERoots tm'
        then let msg = LSP.FromServerRsp (tamMethod tm) $ rsp {LSP._result = tamCombiner tm' (tamResponses tm')}
              in (Right $ Just msg, Just newIxMap)
        else let insertedIxMap = fromMaybe newIxMap $ IM.insertIxMap (tamLspId tm) tm' newIxMap
              in (Right Nothing, Just insertedIxMap)
    Left msg -> (Left msg, Nothing)

-- Similar to parseServerMessageWithTracker but using Client message types, and checking previous requests from server
-- Also does not include the multi-reply logic
-- For responses, gives the ide that sent the initial request
parseClientMessageWithTracker
  :: MethodTrackerVar 'LSP.FromServer
  -> Aeson.Value
  -> IO (Either String (LSP.FromClientMessage' (Product LSP.SMethod (Const FilePath))))
parseClientMessageWithTracker tracker val = pickReqMethodTo tracker $ \extract ->
  case Aeson.parseEither (LSP.parseClientMessage (wrapParseMessageLookup . extract)) val of
    Right (LSP.FromClientMess meth mess) -> (Right (LSP.FromClientMess meth mess), Nothing)
    Right (LSP.FromClientRsp (Pair (TrackedSingleMethodFromServer method home) (Const newIxMap)) rsp) ->
      (Right (LSP.FromClientRsp (Pair method (Const home)) rsp), Just newIxMap)
    Left msg -> (Left msg, Nothing)

-- Takes a message from server and stores it if its a request, so that later messages from the client can deduce response context
putServerReq :: MethodTrackerVar 'LSP.FromServer -> FilePath -> LSP.FromServerMessage -> IO ()
putServerReq tracker home msg =
  case msg of
    LSP.FromServerMess meth mess ->
      case LSP.splitServerMethod meth of
        LSP.IsServerReq ->
          let LSP.RequestMessage {_id, _method} = mess
            in putReqMethodSingleFromServer tracker home _id _method
        LSP.IsServerEither ->
          case mess of
            LSP.ReqMess LSP.RequestMessage {_id, _method} -> putReqMethodSingleFromServer tracker home _id _method
            _ -> pure ()
        _ -> pure ()
    _ -> pure ()
