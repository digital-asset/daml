-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GADTs #-}

module DA.Cli.Damlc.Command.MultiIde.Parsing (
  getUnrespondedRequestsToResend,
  getUnrespondedRequestsFallbackResponses,
  onChunks,
  parseClientMessageWithTracker,
  parseServerMessageWithTracker,
  putChunk,
  putReqMethodAll,
  putReqMethodSingleFromClient,
  putReqMethodSingleFromServer,
  putReqMethodSingleFromServerCoordinator,
  putFromServerMessage,
  putSingleFromClientMessage,
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
import DA.Cli.Damlc.Command.MultiIde.Util
import Data.Bifunctor (second)
import Data.Functor.Product
import qualified Data.IxMap as IM
import Data.List (delete)
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import Data.Some.Newtype (Some, mkSome, withSome)
import qualified Language.LSP.Types as LSP
import qualified Language.LSP.Types.Lens as LSP
import System.IO.Extra
import Unsafe.Coerce (unsafeCoerce)

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
-- Returns an error string on failure
onChunks :: Handle -> (B.ByteString -> IO ()) -> IO ()
onChunks handle act =
  let handleResult bytes =
        case Attoparsec.parse contentChunkParser bytes of
          Attoparsec.Done leftovers result -> act result >> handleResult leftovers
          Attoparsec.Fail _ _ "not enough input" -> pure ()
          Attoparsec.Fail _ _ err -> error $ "Chunk parse failed: " <> err
   in BSL.hGetContents handle >>= handleResult

putChunk :: Handle -> BSL.ByteString -> IO ()
putChunk handle payload = do
  let fullMessage = "Content-Length: " <> BSLC.pack (show (BSL.length payload)) <> "\r\n\r\n" <> payload
  BSL.hPut handle fullMessage
  hTryFlush handle

putReqMethodSingleFromServer
  :: forall (m :: LSP.Method 'LSP.FromServer 'LSP.Request)
  .  MethodTrackerVar 'LSP.FromServer -> PackageHome -> LSP.LspId m -> LSP.SMethod m -> IO ()
putReqMethodSingleFromServer tracker home id method = putReqMethod tracker id $ TrackedSingleMethodFromServer method $ Just home

putReqMethodSingleFromServerCoordinator
  :: forall (m :: LSP.Method 'LSP.FromServer 'LSP.Request)
  .  MethodTrackerVar 'LSP.FromServer -> LSP.LspId m -> LSP.SMethod m -> IO ()
putReqMethodSingleFromServerCoordinator tracker id method = putReqMethod tracker id $ TrackedSingleMethodFromServer method Nothing

-- Takes a message from server and stores it if its a request, so that later messages from the client can deduce response context
putFromServerMessage :: MultiIdeState -> PackageHome -> LSP.FromServerMessage -> IO ()
putFromServerMessage miState home (LSP.FromServerMess method mess) =
  case (LSP.splitServerMethod method, mess) of
    (LSP.IsServerReq, _) -> putReqMethodSingleFromServer (misFromServerMethodTrackerVar miState) home (mess ^. LSP.id) method
    (LSP.IsServerEither, LSP.ReqMess mess) -> putReqMethodSingleFromServer (misFromServerMethodTrackerVar miState) home (mess ^. LSP.id) method
    _ -> pure ()
putFromServerMessage _ _ _ = pure ()

putReqMethodSingleFromClient
  :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
  .  MethodTrackerVar 'LSP.FromClient -> LSP.LspId m -> LSP.SMethod m -> LSP.FromClientMessage -> PackageHome -> IO ()
putReqMethodSingleFromClient tracker id method message home = putReqMethod tracker id $ TrackedSingleMethodFromClient method message home

-- Convenience wrapper around putReqMethodSingleFromClient
putSingleFromClientMessage :: MultiIdeState -> PackageHome -> LSP.FromClientMessage -> IO ()
putSingleFromClientMessage miState home msg@(LSP.FromClientMess method mess) =
  case (LSP.splitClientMethod method, mess) of
    (LSP.IsClientReq, _) -> putReqMethodSingleFromClient (misFromClientMethodTrackerVar miState) (mess ^. LSP.id) method msg home
    (LSP.IsClientEither, LSP.ReqMess mess) -> putReqMethodSingleFromClient (misFromClientMethodTrackerVar miState) (mess ^. LSP.id) method msg home
    _ -> pure ()
putSingleFromClientMessage _ _ _ = pure ()

putReqMethodAll
  :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
  .  MethodTrackerVar 'LSP.FromClient
  -> LSP.LspId m
  -> LSP.SMethod m
  -> LSP.FromClientMessage
  -> [PackageHome]
  -> ResponseCombiner m
  -> IO ()
putReqMethodAll tracker id method msg ides combine =
  putReqMethod tracker id $ TrackedAllMethod method id msg combine ides []

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
parseServerMessageWithTracker :: MethodTrackerVar 'LSP.FromClient -> PackageHome -> Aeson.Value -> IO (Either String (Maybe LSP.FromServerMessage))
parseServerMessageWithTracker tracker home val = pickReqMethodTo tracker $ \extract ->
  case Aeson.parseEither (LSP.parseServerMessage (wrapParseMessageLookup . extract)) val of
    Right (LSP.FromServerMess meth mess) -> (Right (Just $ LSP.FromServerMess meth mess), Nothing)
    Right (LSP.FromServerRsp (Pair (TrackedSingleMethodFromClient method _ _) (Const newIxMap)) rsp) -> (Right (Just (LSP.FromServerRsp method rsp)), Just newIxMap)
    -- Multi reply logic, for requests that are sent to all IDEs with responses unified. Required for some queries
    Right (LSP.FromServerRsp (Pair tm@TrackedAllMethod {} (Const newIxMap)) rsp) -> do
      -- Haskell gets a little confused when updating existential records, so we need to build a new one
      let tm' = TrackedAllMethod
                  { tamMethod = tamMethod tm
                  , tamLspId = tamLspId tm
                  , tamClientMessage = tamClientMessage tm
                  , tamCombiner = tamCombiner tm
                  , tamResponses = (home, LSP._result rsp) : tamResponses tm
                  , tamRemainingResponsePackageHomes = delete home $ tamRemainingResponsePackageHomes tm
                  }
      if null $ tamRemainingResponsePackageHomes tm'
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
  -> IO (Either String (LSP.FromClientMessage' SMethodWithSender))
parseClientMessageWithTracker tracker val = pickReqMethodTo tracker $ \extract ->
  case Aeson.parseEither (LSP.parseClientMessage (wrapParseMessageLookup . extract)) val of
    Right (LSP.FromClientMess meth mess) -> (Right (LSP.FromClientMess meth mess), Nothing)
    Right (LSP.FromClientRsp (Pair (TrackedSingleMethodFromServer method mHome) (Const newIxMap)) rsp) ->
      (Right (LSP.FromClientRsp (SMethodWithSender method mHome) rsp), Just newIxMap)
    Left msg -> (Left msg, Nothing)

-- Map.mapAccumWithKey where the replacement value is a Maybe. Accumulator is still updated for `Nothing` values
mapMaybeAccumWithKey :: Ord k => (a -> k -> b -> (a, Maybe c)) -> a -> Map.Map k b -> (a, Map.Map k c)
mapMaybeAccumWithKey f z = flip Map.foldrWithKey (z, Map.empty) $ \k v (accum, m) ->
  second (maybe m (\v' -> Map.insert k v' m)) $ f accum k v

-- Convenience for the longwinded FromClient Some TrackedMethod type
type SomeFromClientTrackedMethod = Some @(LSP.Method 'LSP.FromClient 'LSP.Request) TrackedMethod

-- Sadly some coercions needed here, as IxMap doesn't expose methods to traverse the map safely
-- Each usage is explained in comments nearby
-- We disable the restricted `unsafeCoerce` warning below
{-# ANN adjustClientTrackers ("HLint: ignore Avoid restricted function" :: String) #-}
adjustClientTrackers 
  :: forall a
  .  MultiIdeState
  -> PackageHome
  -> (  forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
     .  LSP.LspId m
     -> TrackedMethod m 
     -> (Maybe (TrackedMethod m), Maybe a)
     )
  -> IO [a]
adjustClientTrackers miState home adjuster = atomically $ stateTVar (misFromClientMethodTrackerVar miState) $ \tracker ->
  let doAdjust 
        :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
        .  [a]
        -> LSP.LspId m
        -> TrackedMethod m
        -> ([a], Maybe SomeFromClientTrackedMethod)
      doAdjust accum lspId tracker = let (mTracker, mV) = adjuster lspId tracker in (maybe accum (:accum) mV, mkSome <$> mTracker)
      -- In this function, we unpack the SomeLspId to LspId m', then coerce the `m'` to match the `m` of TrackedMethod.
      -- This invariant is enforced by the interface to IxMaps, and thus is safe.
      adjust :: [a] -> LSP.SomeLspId -> SomeFromClientTrackedMethod -> ([a], Maybe SomeFromClientTrackedMethod)
      adjust accum someLspId someTracker = withSome someTracker $ \tracker -> case (tracker, someLspId) of
        (TrackedSingleMethodFromClient _ _ home', LSP.SomeLspId lspId) | home == home' -> doAdjust accum (unsafeCoerce lspId) tracker
        (TrackedAllMethod {tamRemainingResponsePackageHomes}, LSP.SomeLspId lspId) | home `elem` tamRemainingResponsePackageHomes -> doAdjust accum (unsafeCoerce lspId) tracker
        _ -> (accum, Just someTracker)
      -- We know that the misFromClientMethodTrackerVar only contains Trackers for FromClient, but this information is lost in the `Some` inside the IxMap
      -- We define our `adjust` method safely, by having it know this `FromClient` constraint, then coerce it to bring said constraint into scope.
      -- (trackerMap :: forall (from :: LSP.From). Map.Map SomeLspId (Some @(Lsp.Method from @LSP.Request) TrackedMethod))
      -- where `from` is constrained outside the IxMap and as such, enforced weakly (using unsafeCoerce)
      (accum, trackerMap) = mapMaybeAccumWithKey (unsafeCoerce adjust) [] $ IM.getMap tracker
   in (accum, IM.IxMap trackerMap)

-- Checks if a given Shutdown or Initialize lspId is for an IDE that is still closing, and as such, should not be removed
isClosingIdeInFlight :: SubIdeData -> LSP.SMethod m -> LSP.LspId m -> Bool
isClosingIdeInFlight ideData LSP.SShutdown (LSP.IdString str) = any (\ide -> str == ideMessageIdPrefix ide <> "-shutdown") $ ideDataClosing ideData
isClosingIdeInFlight ideData LSP.SInitialize (LSP.IdString str) = any (\ide -> str == ideMessageIdPrefix ide <> "-init") $ ideDataClosing ideData
isClosingIdeInFlight _ _ _ = False

-- Reads all unresponded messages for a given home, gives back the original messages. Ignores and deletes Initialize and Shutdown requests
-- but only if no ideClosing ides are using them
getUnrespondedRequestsToResend :: MultiIdeState -> SubIdeData -> PackageHome -> IO [LSP.FromClientMessage]
getUnrespondedRequestsToResend miState ideData home = adjustClientTrackers miState home $ \lspId tracker -> case tmMethod tracker of
  -- Keep shutdown/initialize messages that are in use, but don't return them
  method | isClosingIdeInFlight ideData method lspId -> (Just tracker, Nothing)
  LSP.SInitialize -> (Nothing, Nothing)
  LSP.SShutdown -> (Nothing, Nothing)
  _ -> (Just tracker, Just $ tmClientMessage tracker)

-- Gets fallback responses for all unresponded requests for a given home.
-- For Single IDE requests, we return noIDEReply, and delete the request from the tracker
-- For All IDE requests, we delete this home from the aggregate response, and if it is now complete, run the combiner and return the result
getUnrespondedRequestsFallbackResponses :: MultiIdeState -> SubIdeData -> PackageHome -> IO [LSP.FromServerMessage]
getUnrespondedRequestsFallbackResponses miState ideData home = adjustClientTrackers miState home $ \lspId tracker -> case tracker of
-- Keep shutdown/initialize messages that are in use, but don't return them
  TrackedSingleMethodFromClient method _ _ | isClosingIdeInFlight ideData method lspId -> (Just tracker, Nothing)
  TrackedSingleMethodFromClient _ msg _ -> (Nothing, noIDEReply msg)
  tm@TrackedAllMethod {tamRemainingResponsePackageHomes = [home']} | home' == home ->
    let reply = LSP.FromServerRsp (tamMethod tm) $ LSP.ResponseMessage "2.0" (Just $ tamLspId tm) (tamCombiner tm $ tamResponses tm)
     in (Nothing, Just reply)
  TrackedAllMethod {..} ->
    let tm = TrackedAllMethod
              { tamMethod
              , tamLspId
              , tamClientMessage
              , tamCombiner
              , tamResponses
              , tamRemainingResponsePackageHomes = delete home tamRemainingResponsePackageHomes
              }
     in (Just tm, Nothing)
