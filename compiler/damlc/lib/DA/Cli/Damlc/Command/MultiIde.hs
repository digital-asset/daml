-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE ApplicativeDo       #-}
{-# LANGUAGE RankNTypes       #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DisambiguateRecordFields #-}

module DA.Cli.Damlc.Command.MultiIde (runMultiIde) where

import Control.Concurrent.Async (async, waitAny)
import Control.Concurrent.STM.TChan
import Control.Concurrent.STM.TVar
import Control.Monad
import Control.Monad.STM
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import qualified Data.Attoparsec.ByteString.Lazy as Attoparsec
import qualified Data.ByteString as B
import Data.ByteString.Builder.Extra (defaultChunkSize)
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSLC
import Data.Functor.Const
import Data.Functor.Product
import qualified Data.IxMap as IM
import Data.Maybe (fromJust, fromMaybe)
import qualified Language.LSP.Types as LSP
import qualified Language.LSP.Types.Message as LSP
import qualified Language.LSP.Types.Method as LSP
import qualified Language.LSP.Types.Parsing as LSP
import System.IO.Extra
import System.IO.Unsafe (unsafeInterleaveIO)
import System.Process (StdStream(..), CreateProcess(..), ProcessHandle, proc, waitForProcess, createProcess)

{-# ANN allBytes ("HLint: ignore Avoid restricted function" :: String) #-}
-- unsafeInterleaveIO used to create a chunked lazy bytestring over IO for a given Handle
allBytes :: Handle -> IO BSL.ByteString
allBytes hin = fmap BSL.fromChunks go
  where
    go :: IO [B.ByteString]
    go = do
      first <- unsafeInterleaveIO $ B.hGetSome hin defaultChunkSize
      rest <- unsafeInterleaveIO go
      pure (first : rest)

-- Feels like this could just be a `takeWhile isDigit >>= read . concat`
-- I'd also be quite surprised if Attoparsec doesn't already have an integer parser...
decimal :: Attoparsec.Parser Int
decimal = B.foldl' step 0 `fmap` Attoparsec.takeWhile1 (\w -> w - 48 <= 9)
  where step a w = a * 10 + fromIntegral (w - 48)

contentChunkParser :: Attoparsec.Parser B.ByteString
contentChunkParser = do
  _ <- Attoparsec.string "Content-Length: "
  len <- decimal
  _ <- Attoparsec.string "\r\n\r\n"
  Attoparsec.take len

getChunks :: Handle -> IO [B.ByteString]
getChunks handle =
  let loop bytes =
        case Attoparsec.parse contentChunkParser bytes of
          Attoparsec.Done leftovers result -> result : loop leftovers
          _ -> []
  in
  loop <$> allBytes handle

putChunk :: Handle -> BSL.ByteString -> IO ()
putChunk handle payload = do
  let fullMessage = "Content-Length: " <> BSLC.pack (show (BSL.length payload)) <> "\r\n\r\n" <> payload
  BSL.hPut handle fullMessage
  hFlush handle

er :: Show x => String -> Either x a -> a
er _msg (Right a) = a
er msg (Left e) = error $ msg <> ": " <> show e

type MethodTracker (from :: LSP.From) = IM.IxMap @(LSP.Method from 'LSP.Request) LSP.LspId LSP.SMethod
type MethodTrackerVar (from :: LSP.From) = TVar (MethodTracker from)
data MethodTrackerVars = MethodTrackerVars 
  { fromClientTracker :: MethodTrackerVar 'LSP.FromClient
  , fromServerTracker :: MethodTrackerVar 'LSP.FromServer
  }

putReqMethod
  :: forall (f :: LSP.From) (m :: LSP.Method f 'LSP.Request)
    . MethodTrackerVar f -> LSP.LspId m -> LSP.SMethod m -> IO ()
putReqMethod tracker id method = atomically $ modifyTVar' tracker $ \im ->
  fromMaybe im $ IM.insertIxMap id method im

pickReqMethodTo
  :: forall (f :: LSP.From) r
    . MethodTrackerVar f
  -> ((forall (m :: LSP.Method f 'LSP.Request)
        . LSP.LspId m
        -> (Maybe (LSP.SMethod m), MethodTracker f)
      ) -> (r, Maybe (MethodTracker f)))
  -> IO r
pickReqMethodTo tracker handler = atomically $ do
  im <- readTVar tracker
  let (r, mayNewIM) = handler (flip IM.pickFromIxMap im)
  case mayNewIM of
    Just newIM -> writeTVar tracker newIM
    Nothing -> pure ()
  pure r

-- We're forced to give a result of type `(SMethod m, a m)` by parseServerMessage, but we want to include the updated MethodTracker
-- so we use Product to ensure our result has the SMethod and our MethodTracker
wrapExtract
  :: forall (f :: LSP.From) (m :: LSP.Method f 'LSP.Request)
    . (Maybe (LSP.SMethod m), MethodTracker f)
  -> Maybe
      ( LSP.SMethod m
      , Product LSP.SMethod (Const (MethodTracker f)) m
      )
wrapExtract (mayMethod, newIM) =
  fmap (\meth -> (meth, Pair meth (Const newIM))) mayMethod

-- Parses a message from the server providing context about previous requests from client
-- allowing the server parser to reconstruct typed responses to said requests
parseServerMessageWithTracker :: MethodTrackerVar 'LSP.FromClient -> Aeson.Value -> IO (Either String LSP.FromServerMessage)
parseServerMessageWithTracker tracker val = pickReqMethodTo tracker $ \extract ->
  case Aeson.parseEither (LSP.parseServerMessage (wrapExtract . extract)) val of
    Right (LSP.FromServerMess meth mess) -> (Right (LSP.FromServerMess meth mess), Nothing)
    Right (LSP.FromServerRsp (Pair meth (Const newIxMap)) rsp) -> (Right (LSP.FromServerRsp meth rsp), Just newIxMap)
    Left msg -> (Left msg, Nothing)

-- Identical to parseServerMessageWithTracker but using Client message types, and checking previous requests from server
parseClientMessageWithTracker :: MethodTrackerVar 'LSP.FromServer -> Aeson.Value -> IO (Either String LSP.FromClientMessage)
parseClientMessageWithTracker tracker val = pickReqMethodTo tracker $ \extract ->
  case Aeson.parseEither (LSP.parseClientMessage (wrapExtract . extract)) val of
    Right (LSP.FromClientMess meth mess) -> (Right (LSP.FromClientMess meth mess), Nothing)
    Right (LSP.FromClientRsp (Pair meth (Const newIxMap)) rsp) -> (Right (LSP.FromClientRsp meth rsp), Just newIxMap)
    Left msg -> (Left msg, Nothing)

-- Takes a message from server and stores it if its a request, so that later messages from the client can deduce response context
putServerReq :: MethodTrackerVar 'LSP.FromServer -> LSP.FromServerMessage -> IO ()
putServerReq tracker msg =
  case msg of
    LSP.FromServerMess meth mess ->
      case LSP.splitServerMethod meth of
        LSP.IsServerReq ->
          let LSP.RequestMessage {_id, _method} = mess
            in putReqMethod tracker _id _method
        LSP.IsServerEither ->
          case mess of
            LSP.ReqMess LSP.RequestMessage {_id, _method} -> putReqMethod tracker _id _method
            _ -> pure ()
        _ -> pure ()
    _ -> pure ()

-- Identical to putServerReq but for messages from client
putClientReq :: MethodTrackerVar 'LSP.FromClient -> LSP.FromClientMessage -> IO ()
putClientReq tracker msg =
  case msg of
    LSP.FromClientMess meth mess ->
      case LSP.splitClientMethod meth of
        LSP.IsClientReq ->
          let LSP.RequestMessage {_id, _method} = mess
            in putReqMethod tracker _id _method
        LSP.IsClientEither ->
          case mess of
            LSP.ReqMess LSP.RequestMessage {_id, _method} -> putReqMethod tracker _id _method
            _ -> pure ()
        _ -> pure ()
    _ -> pure ()

subprocMessageHandler :: MethodTrackerVars -> TChan BSL.ByteString -> TChan BSL.ByteString -> B.ByteString -> IO ()
subprocMessageHandler trackers toClientChan toSubprocChan bs = do
  hPutStrLn stderr "Called subprocMessageHandler"
  BSC.hPutStrLn stderr bs

  -- Decode a value, parse
  let val :: Aeson.Value
      val = er "eitherDecode" $ Aeson.eitherDecodeStrict bs
  msg <- either error id <$> parseServerMessageWithTracker (fromClientTracker trackers) val

  let sendClient :: LSP.FromServerMessage -> IO ()
      sendClient = atomically . writeTChan toClientChan . Aeson.encode
      _sendSubproc :: LSP.FromClientMessage -> IO ()
      _sendSubproc = atomically . writeTChan toSubprocChan . Aeson.encode

  -- If its a request (builtin or custom), save it for response handling.
  putServerReq (fromServerTracker trackers) msg

  hPutStrLn stderr "About to thunk message"
  case msg of
    LSP.FromServerRsp LSP.SInitialize LSP.ResponseMessage {_result} -> do
      hPutStrLn stderr "Backwarding initialization"
      hFlush stderr
      sendClient msg
    _ -> do
      hPutStrLn stderr "Backwarding unknown message"
      hFlush stderr
      sendClient msg

clientMessageHandler :: MethodTrackerVars -> TChan BSL.ByteString -> TChan BSL.ByteString -> B.ByteString -> IO ()
clientMessageHandler trackers toClientChan toSubprocChan bs = do
  hPutStrLn stderr "Called clientMessageHandler"
  hFlush stderr

  -- Decode a value, parse
  let val :: Aeson.Value
      val = er "eitherDecode" $ Aeson.eitherDecodeStrict bs
  msg <- either error id <$> parseClientMessageWithTracker (fromServerTracker trackers) val

  let sendClient :: LSP.FromServerMessage -> IO ()
      sendClient = atomically . writeTChan toClientChan . Aeson.encode
      sendSubproc :: LSP.FromClientMessage -> IO ()
      sendSubproc = atomically . writeTChan toSubprocChan . Aeson.encode

  -- If its a request (builtin or custom), save it for response handling.
  putClientReq (fromClientTracker trackers) msg

  -- Get a client message
  case msg of
    -- LSP.FromClientMess LSP.STextDocumentDidOpen LSP.NotificationMessage {_method, _params} -> do
    --   let (LSP.DidOpenTextDocumentParams LSP.TextDocumentItem{_uri}) = _params
    --   hPutStrLn stderr ("Opened virtual resource: " <> T.unpack (LSP.getUri _uri))
    --   hFlush stderr
    --   pure ()
    LSP.FromClientMess (LSP.SCustomMethod "daml/keepAlive") (LSP.ReqMess LSP.RequestMessage {_id, _method, _params}) -> do
      hPutStrLn stderr "Custom message daml/keepAlive"
      hFlush stderr
      sendClient $ LSP.FromServerRsp _method $ LSP.ResponseMessage "2.0" (Just _id) (Right Aeson.Null)
    LSP.FromClientMess LSP.SInitialize LSP.RequestMessage {_id, _method, _params} -> do
      hPutStrLn stderr "Initialize"
      hFlush stderr
      sendSubproc msg 
      -- TODO: initialization needs to be smart
      -- We need to first store the initialization message from vscode, and send some valid response back
      -- Then, whenever we add a new subIDE to our set, we need to replay this message to them (modifying the root dir, and anything else needed)
      -- and lastly, we void the responses from the subIDE, assuming that they should vaguely match the response we already gave
      --sendCLient $ LSP.FromClientMess LSP.SInitialize mess
      --let true = Just $ LSP.InL True
      --    capabilities =
      --      LSP.ServerCapabilities
      --        { _textDocumentSync = Just $ LSP.InL $ LSP.TextDocumentSyncOptions
      --            { LSP._openClose = Just True
      --            , LSP._change = Just LSP.TdSyncIncremental
      --            , LSP._willSave = Nothing
      --            , LSP._willSaveWaitUntil = Nothing
      --            , LSP._save = Just $ LSP.InR $ LSP.SaveOptions Nothing
      --            }
      --        , _hoverProvider                    = true
      --        , _completionProvider               = Nothing
      --        , _signatureHelpProvider            = Nothing
      --        , _declarationProvider              = true
      --        , _definitionProvider               = true
      --        , _typeDefinitionProvider           = true
      --        , _implementationProvider           = true
      --        , _referencesProvider               = true
      --        , _documentHighlightProvider        = true
      --        , _documentSymbolProvider           = true
      --        , _codeActionProvider               = true
      --        , _codeLensProvider                 = Nothing
      --        , _documentFormattingProvider       = true
      --        , _documentRangeFormattingProvider  = true
      --        , _documentOnTypeFormattingProvider = Nothing
      --        , _renameProvider                   = true
      --        , _documentLinkProvider             = Nothing
      --        , _colorProvider                    = true
      --        , _foldingRangeProvider             = true
      --        , _executeCommandProvider           = Nothing
      --        , _selectionRangeProvider           = true
      --        , _callHierarchyProvider            = true
      --        , _semanticTokensProvider           = Nothing
      --        , _workspaceSymbolProvider          = Just True
      --        , _workspace                        = Nothing
      --        -- TODO: Add something for experimental
      --        , _experimental                     = Nothing :: Maybe Aeson.Value
      --        }
      --pure $ Just $ LSP.FromServerRsp LSP.SInitialize $ LSP.ResponseMessage "2.0" (Just _id) (Right (LSP.InitializeResult capabilities Nothing))
    _ -> do
      hPutStrLn stderr "Forwarding unknown message"
      hFlush stderr
      sendSubproc msg

runSubProc :: IO (Handle, Handle, ProcessHandle)
runSubProc = do
  -- TODO: Better to make a tmp file and print its location
  subproc_stderr_out <- openFile "/home/samuelwilliams/subproc_stderr" WriteMode
  (fromJust -> subprocStdin, fromJust -> subprocStdout, _, subprocHandle) <-
    createProcess
      -- TODO: This needs to be version aware, use the env var to set SDK version then envoke daml assistant via its own env var
      -- it likely doesn't matter which version of daml assistant is used, as it'll grab the correct IDE under the hood.
      (proc "/home/samuelwilliams/.daml/sdk/0.0.0/daml/daml" ["ide", "--debug"])
      { std_in = CreatePipe, std_out = CreatePipe, std_err = UseHandle subproc_stderr_out }
  pure (subprocStdin, subprocStdout, subprocHandle)

runMultiIde :: [String] -> IO ()
runMultiIde sourceDeps = do
  hPrint stderr sourceDeps

  -- Launch sub-server
  (subprocStdin, subprocStdout, subprocHandle) <- runSubProc

  -- Request trackers for response messages
  (requestFromClientMethodTracker :: MethodTrackerVar 'LSP.FromClient) <- newTVarIO IM.emptyIxMap
  (requestFromServerMethodTracker :: MethodTrackerVar 'LSP.FromServer) <- newTVarIO IM.emptyIxMap
  let methodTrackerVars = MethodTrackerVars requestFromClientMethodTracker requestFromServerMethodTracker

  hPutStrLn stderr "Listening for bytes"

  -- Client <- *****
  toClientChan <- atomically newTChan
  toClientThread <- async $ forever $ do
    msg <- atomically $ readTChan toClientChan
    hPutStrLn stderr "Pushing message to client"
    BSLC.hPutStrLn stderr msg
    putChunk stdout msg

  --           ***** -> Subproc
  toSubprocChan <- atomically newTChan
  toSubprocThread <- async $ forever $ do
    msg <- atomically $ readTChan toSubprocChan
    hPutStrLn stderr "Pushing message to subproc"
    BSLC.hPutStrLn stderr msg
    putChunk subprocStdin msg

  -- Client -> Coord
  clientToCoordThread <- async $ do
    chunks <- getChunks stdin
    mapM_ (clientMessageHandler methodTrackerVars toClientChan toSubprocChan) chunks

  --           Coord <- Subproc
  subprocToCoord <- async $ do
    chunks <- getChunks subprocStdout
    mapM_ (subprocMessageHandler methodTrackerVars toClientChan toSubprocChan) chunks

  waitAny
    [ toClientThread
    , toSubprocThread
    , clientToCoordThread
    , subprocToCoord
    ]
  waitForProcess subprocHandle
  -- hPutStrLn stderr (show exitCode)
  pure ()
