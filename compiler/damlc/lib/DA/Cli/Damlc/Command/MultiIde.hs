-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE ApplicativeDo       #-}
{-# LANGUAGE RankNTypes       #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE GADTs #-}

module DA.Cli.Damlc.Command.MultiIde (runMultiIde) where

import Control.Concurrent.Async (Async, async, cancel, pollSTM)
import Control.Concurrent.STM.TChan
import Control.Concurrent.STM.TVar
import Control.Concurrent.STM.TMVar
import Control.Concurrent.MVar
import Control.Lens
import Control.Monad
import Control.Monad.STM
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import qualified Data.Attoparsec.ByteString.Lazy as Attoparsec
import qualified Data.ByteString as B
import Data.ByteString.Builder.Extra (defaultChunkSize)
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSLC
import DA.Cli.Damlc.Command.MultiIdeMessageDir
import Data.Either (lefts)
import Data.Functor.Product
import qualified Data.IxMap as IM
import Data.List (delete, find, isPrefixOf)
import qualified Data.Map as Map
import Data.Maybe (catMaybes, fromMaybe)
import qualified Data.Text as T
import GHC.Conc (unsafeIOToSTM)
import qualified Language.LSP.Types as LSP
import qualified Language.LSP.Types.Capabilities as LSP
import qualified Language.LSP.Types.Lens as LSP
import System.Directory (doesDirectoryExist, listDirectory)
import System.Environment (getEnv)
import System.FilePath (takeDirectory)
import System.IO.Extra
import System.IO.Unsafe (unsafeInterleaveIO, unsafePerformIO)
import System.Posix.Signals (installHandler, sigTERM, Handler (Catch))
import System.Process (getPid)
import System.Process.Typed (
  Process,
  createPipe,
  getExitCodeSTM,
  getStdin,
  getStdout,
  nullStream,
  proc,
  setStderr,
  setStdin,
  setStdout,
  setWorkingDir,
  startProcess,
  stopProcess,
  unsafeProcessHandle,
 )

-- TODO: Currently errors if you boot the IDE with files from multiple packages, seems spinning up multiple subides at the same time breaks
-- [Error - 17:46:40] Registering progress handler for token 2 failed.
-- Error: Progress handler for token 2 already registered
-- Does recover though!

-- TODO: Logic to properly shutdown an IDE if its process fails
-- TODO: the W request for goto definition - really do consider the alternative of copying and modifying the `hie` files to simply point to the right directory
--   since then all the magic logic is already done, once you end up in that new file, it'll open up its own SubIDE
--   Note that this approach won't work if the code of the dep is out of date with the dependents copy of it in their dar
--   but the IDE is supposed to deal with this...?

-- Stop mangling my prints! >:(
{-# ANN printLock ("HLint: ignore Avoid restricted function" :: String) #-}
{-# NOINLINE printLock #-}
printLock :: MVar ()
printLock = unsafePerformIO $ newMVar ()

debugPrint :: String -> IO ()
debugPrint msg = withMVar printLock $ \_ -> do
  hPutStrLn stderr msg
  hFlush stderr

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

data TrackedMethod (m :: LSP.Method from 'LSP.Request) where
  TrackedSingleMethodFromClient
    :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
    .  LSP.SMethod m
    -> TrackedMethod m
  TrackedSingleMethodFromServer
    :: forall (m :: LSP.Method 'LSP.FromServer 'LSP.Request)
    .  LSP.SMethod m
    -> FilePath -- Also store the IDE that sent the request
    -> TrackedMethod m
  TrackedAllMethod :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request).
    { tamMethod :: LSP.SMethod m
        -- ^ The method of the initial request
    , tamLspId :: LSP.LspId m
    , tamCombiner :: ResponseCombiner m
        -- ^ How to combine the results from each IDE
    , tamRemainingResponseIDERoots :: [FilePath]
        -- ^ The IDES that have not yet replied to this message
    , tamResponses :: [(FilePath, Either LSP.ResponseError (LSP.ResponseResult m))]
    } -> TrackedMethod m

tmMethod
  :: forall (from :: LSP.From) (m :: LSP.Method from 'LSP.Request)
  .  TrackedMethod m
  -> LSP.SMethod m
tmMethod (TrackedSingleMethodFromClient m) = m
tmMethod (TrackedSingleMethodFromServer m _) = m
tmMethod (TrackedAllMethod {tamMethod}) = tamMethod

type MethodTracker (from :: LSP.From) = IM.IxMap @(LSP.Method from 'LSP.Request) LSP.LspId TrackedMethod
type MethodTrackerVar (from :: LSP.From) = TVar (MethodTracker from)
data MethodTrackerVars = MethodTrackerVars 
  { fromClientTracker :: MethodTrackerVar 'LSP.FromClient
    -- ^ The client will track its own IDs to ensure they're unique, so no worries about collisions
  , fromServerTracker :: MethodTrackerVar 'LSP.FromServer
    -- ^ We will prefix LspIds before they get here based on their SubIDE messageIdPrefix, to avoid collisions
  }

data SubIDE = SubIDE
  { ideInhandleAsync :: Async ()
  , ideInHandleChannel :: TChan BSL.ByteString
  , ideOutHandleAsync :: Async ()
    -- ^ For sending messages to that SubIDE
  , ideProcess :: Process Handle Handle ()
  , ideHomeDirectory :: FilePath
  , ideMessageIdPrefix :: T.Text
    -- ^ Some unique string used to prefix message ids created by the SubIDE, to avoid collisions with other SubIDEs
    -- We use the stringified process ID
  }

type SubIDEs = Map.Map FilePath SubIDE
type SubIDEsVar = TMVar SubIDEs

type InitParams = LSP.InitializeParams
type InitParamsVar = MVar InitParams

-- add IDE, send initialize, do not send further messages until we get the initialize response and have sent initialized
-- we can do this by locking the sending thread, but still allowing the channel to be pushed
addNewSubIDE
  :: SubIDEsVar
  -> InitParamsVar
  -> MethodTrackerVars
  -> TChan BSL.ByteString
  -> FilePath
  -> IO SubIDE
addNewSubIDE idesVar initParamsVar methodTrackerVars toClientChan home = do
  debugPrint "Trying to make a SubIDE"
  ides <- atomically $ takeTMVar idesVar

  let mExistingIde = Map.lookup home ides
  case mExistingIde of
    Just ide -> ide <$ debugPrint "SubIDE already exists"
    Nothing -> do
      debugPrint "Making a SubIDE"
      subIdeProcess <- runSubProc home
      let inHandle = getStdin subIdeProcess
          outHandle = getStdout subIdeProcess

      -- Handles blocking the sender thread until the IDE is initialized.
      sendBlocker <- newEmptyMVar @()
      let unblock = putMVar sendBlocker ()
          onceUnblocked = (readMVar sendBlocker >>)

      --           ***** -> Subproc
      toSubprocChan <- atomically newTChan
      toSubproc <- async $ onceUnblocked $ forever $ do
        msg <- atomically $ readTChan toSubprocChan
        debugPrint "Pushing message to subproc"
        putChunk inHandle msg

      --           Coord <- Subproc
      subprocToCoord <- async $ do
        -- Wait until our own IDE exists then pass it forward
        ide <- atomically $ fromMaybe (error "Failed to get own IDE") . Map.lookup home <$> readTMVar idesVar
        chunks <- getChunks outHandle
        mapM_ (subprocMessageHandler methodTrackerVars unblock ide toClientChan) chunks

      pid <- fromMaybe (error "SubIDE has no PID") <$> getPid (unsafeProcessHandle subIdeProcess)

      mInitParams <- tryReadMVar initParamsVar
      let !initParams = fromMaybe (error "Attempted to create a SubIDE before initialization!") mInitParams
          initId = LSP.IdString $ T.pack $ show pid
          (initMsg :: LSP.FromClientMessage) = LSP.FromClientMess LSP.SInitialize LSP.RequestMessage 
                  { _id = initId
                  , _method = LSP.SInitialize
                  , _params = initParams
                      { LSP._rootPath = Just $ T.pack home
                      , LSP._rootUri = Just $ LSP.filePathToUri home
                      }
                  , _jsonrpc = "2.0"
                  }
          ide = 
            SubIDE
              { ideInhandleAsync = toSubproc
              , ideInHandleChannel = toSubprocChan
              , ideOutHandleAsync = subprocToCoord
              , ideProcess = subIdeProcess
              , ideHomeDirectory = home
              , ideMessageIdPrefix = T.pack $ show pid
              }

      atomically $ putTMVar idesVar $ Map.insert home ide ides

      putReqMethodSingleFromClient (fromClientTracker methodTrackerVars) initId LSP.SInitialize
      putChunk inHandle $ Aeson.encode initMsg

      pure ide

-- We need to ensure all IDs from different subIDEs are unique to the client, so we prefix them with
-- `ideMessageIdPrefix ide`. Given IDs can be int or text, we encode them as text as well as a tag to say if the original was an int
-- Such that IdInt 10       -> IdString "iPREFIX-10"
--       and IdString "hello" -> IdString "tPREFIX-hello"
addLspPrefix
  :: forall (f :: LSP.From) (m :: LSP.Method f 'LSP.Request)
  .  SubIDE
  -> LSP.LspId m
  -> LSP.LspId m
addLspPrefix ide (LSP.IdInt t) = LSP.IdString $ "i" <> ideMessageIdPrefix ide <> "-" <> T.pack (show t)
addLspPrefix ide (LSP.IdString t) = LSP.IdString $ "t" <> ideMessageIdPrefix ide <> "-" <> t

removeLspPrefix
  :: forall (f :: LSP.From) (m :: LSP.Method f 'LSP.Request)
  .  LSP.LspId m
  -> LSP.LspId m
removeLspPrefix (LSP.IdString (T.unpack -> ('i':rest))) = LSP.IdInt $ read $ tail $ dropWhile (/='-') rest
removeLspPrefix (LSP.IdString (T.uncons -> Just ('t', rest))) = LSP.IdString $ T.tail $ T.dropWhile (/='-') rest
-- Maybe this should error? This method should only be called on LspIds that we know have been prefixed
removeLspPrefix t = t


-- TODO: This should send the shutdown and exit messages, remove self from SUBIdesVar (immediately)
shutdownIde :: SubIDE -> IO ()
shutdownIde subIde = do
  cancel $ ideInhandleAsync subIde
  cancel $ ideOutHandleAsync subIde
  stopProcess $ ideProcess subIde

runSubProc :: FilePath -> IO (Process Handle Handle ())
runSubProc home = do
  assistantPath <- getEnv "DAML_ASSISTANT"

  startProcess $
    proc assistantPath ["ide", "--debug"] &
      setStdin createPipe &
      setStdout createPipe &
      setStderr nullStream &
      setWorkingDir home

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
  case mayNewIM of
    Just newIM -> writeTVar tracker newIM
    Nothing -> pure ()
  pure r

-- We're forced to give a result of type `(SMethod m, a m)` by parseServerMessage, but we want to include the updated MethodTracker
-- so we use Product to ensure our result has the SMethod and our MethodTracker
wrapExtract
  :: forall (f :: LSP.From) (m :: LSP.Method f 'LSP.Request)
  .  (Maybe (TrackedMethod m), MethodTracker f)
  -> Maybe
      ( LSP.SMethod m
      , Product TrackedMethod (Const (MethodTracker f)) m
      )
wrapExtract (mayTM, newIM) =
  fmap (\tm -> (tmMethod tm, Pair tm (Const newIM))) mayTM

-- Parses a message from the server providing context about previous requests from client
-- allowing the server parser to reconstruct typed responses to said requests
-- Handles TrackedAllMethod by returning Nothing for messages that do not have enough replies yet.
parseServerMessageWithTracker :: MethodTrackerVar 'LSP.FromClient -> FilePath -> Aeson.Value -> IO (Either String (Maybe LSP.FromServerMessage))
parseServerMessageWithTracker tracker selfIde val = pickReqMethodTo tracker $ \extract ->
  case Aeson.parseEither (LSP.parseServerMessage (wrapExtract . extract)) val of
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
  case Aeson.parseEither (LSP.parseClientMessage (wrapExtract . extract)) val of
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

addLspPrefixToServerMessage :: SubIDE -> LSP.FromServerMessage -> LSP.FromServerMessage
addLspPrefixToServerMessage _ res@(LSP.FromServerRsp _ _) = res
addLspPrefixToServerMessage ide res@(LSP.FromServerMess method params) =
  case LSP.splitServerMethod method of
    LSP.IsServerReq -> LSP.FromServerMess method $ params & LSP.id %~ addLspPrefix ide
    LSP.IsServerNot -> res
    LSP.IsServerEither ->
      case params of
        LSP.ReqMess params' -> LSP.FromServerMess method $ LSP.ReqMess $ params' & LSP.id %~ addLspPrefix ide
        LSP.NotMess _ -> res

subprocMessageHandler :: MethodTrackerVars -> IO () -> SubIDE -> TChan BSL.ByteString -> B.ByteString -> IO ()
subprocMessageHandler trackers unblock selfIde toClientChan bs = do
  debugPrint "Called subprocMessageHandler"
  -- BSC.hPutStrLn stderr bs

  -- Decode a value, parse
  let val :: Aeson.Value
      val = er "eitherDecode" $ Aeson.eitherDecodeStrict bs
  mMsg <- either error id <$> parseServerMessageWithTracker (fromClientTracker trackers) (ideHomeDirectory selfIde) val

  forM_ mMsg $ \unPrefixedMsg -> do
    let sendClient :: LSP.FromServerMessage -> IO ()
        sendClient = atomically . writeTChan toClientChan . Aeson.encode
        sendSubproc :: LSP.FromClientMessage -> IO ()
        sendSubproc = atomically . writeTChan (ideInHandleChannel selfIde) . Aeson.encode

    let msg = addLspPrefixToServerMessage selfIde unPrefixedMsg

    -- If its a request (builtin or custom), save it for response handling.
    putServerReq (fromServerTracker trackers) (ideHomeDirectory selfIde) msg

    debugPrint "About to thunk message"
    case msg of
      LSP.FromServerRsp LSP.SInitialize LSP.ResponseMessage {_result} -> do
        debugPrint "Got initialization reply, sending initialized and unblocking"
        sendSubproc $ LSP.FromClientMess LSP.SInitialized $ LSP.NotificationMessage "2.0" LSP.SInitialized (Just LSP.InitializedParams)
        unblock
      _ -> do
        debugPrint "Backwarding unknown message"
        sendClient msg

-- Taken directly from the Initialize response
initializeResult :: LSP.InitializeResult
initializeResult = LSP.InitializeResult 
  { _capabilities = LSP.ServerCapabilities 
      { _textDocumentSync = Just $ LSP.InL $ LSP.TextDocumentSyncOptions 
          { _openClose = Just True
          , _change = Just LSP.TdSyncIncremental
          , _willSave = Nothing
          , _willSaveWaitUntil = Nothing
          , _save = Just (LSP.InR (LSP.SaveOptions {_includeText = Nothing}))
          }
      , _hoverProvider = true
      , _completionProvider = Just $ LSP.CompletionOptions
          { _workDoneProgress = Nothing
          , _triggerCharacters = Nothing
          , _allCommitCharacters = Nothing
          , _resolveProvider = Just False
          }
      , _signatureHelpProvider = Nothing
      , _declarationProvider = false
      , _definitionProvider = true
      , _typeDefinitionProvider = false
      , _implementationProvider = false
      , _referencesProvider = false
      , _documentHighlightProvider = false
      , _documentSymbolProvider = true
      , _codeActionProvider = true
      , _codeLensProvider = Just (LSP.CodeLensOptions {_workDoneProgress = Just False, _resolveProvider = Just False})
      , _documentLinkProvider = Nothing
      , _colorProvider = false
      , _documentFormattingProvider = false
      , _documentRangeFormattingProvider = false
      , _documentOnTypeFormattingProvider = Nothing
      , _renameProvider = false
      , _foldingRangeProvider = false
      , _executeCommandProvider = Just (LSP.ExecuteCommandOptions {_workDoneProgress = Nothing, _commands = LSP.List ["typesignature.add"]})
      , _selectionRangeProvider = false
      , _callHierarchyProvider = false
      , _semanticTokensProvider = Just $ LSP.InR $ LSP.SemanticTokensRegistrationOptions
          { _documentSelector = Nothing
          , _workDoneProgress = Nothing
          , _legend = LSP.SemanticTokensLegend
              { _tokenTypes = LSP.List
                  [ LSP.SttType
                  , LSP.SttClass
                  , LSP.SttEnum
                  , LSP.SttInterface
                  , LSP.SttStruct
                  , LSP.SttTypeParameter
                  , LSP.SttParameter
                  , LSP.SttVariable
                  , LSP.SttProperty
                  , LSP.SttEnumMember
                  , LSP.SttEvent
                  , LSP.SttFunction
                  , LSP.SttMethod
                  , LSP.SttMacro
                  , LSP.SttKeyword
                  , LSP.SttModifier
                  , LSP.SttComment
                  , LSP.SttString
                  , LSP.SttNumber
                  , LSP.SttRegexp
                  , LSP.SttOperator
                  ]
              , _tokenModifiers = LSP.List
                  [ LSP.StmDeclaration
                  , LSP.StmDefinition
                  , LSP.StmReadonly
                  , LSP.StmStatic
                  , LSP.StmDeprecated
                  , LSP.StmAbstract
                  , LSP.StmAsync
                  , LSP.StmModification
                  , LSP.StmDocumentation
                  , LSP.StmDefaultLibrary
                  ]
              }

          , _range = Nothing
          , _full = Nothing
          , _id = Nothing
          }
      , _workspaceSymbolProvider = Just False
      , _workspace = Just $ LSP.WorkspaceServerCapabilities
          { _workspaceFolders =
              Just (LSP.WorkspaceFoldersServerCapabilities {_supported = Just True, _changeNotifications = Just (LSP.InR True)})
          }
      , _experimental = Nothing
      }
  , _serverInfo = Nothing
  }
  where
    true = Just (LSP.InL True)
    false = Just (LSP.InL False)

-- Given a file path, move up directory until we find a daml.yaml and give its path (if it exists)
findHome :: FilePath -> IO (Maybe FilePath)
findHome path = do
  exists <- doesDirectoryExist path
  if exists then aux path else aux (takeDirectory path)
  where
    aux :: FilePath -> IO (Maybe FilePath)
    aux path = do
      hasDamlYaml <- elem "daml.yaml" <$> listDirectory path
      if hasDamlYaml
        then pure $ Just path
        else do
          let newPath = takeDirectory path
          if path == newPath
            then pure Nothing
            else aux newPath

clientMessageHandler :: MethodTrackerVars -> InitParamsVar -> TChan BSL.ByteString -> SubIDEsVar -> B.ByteString -> IO ()
clientMessageHandler trackers initParamsVar toClientChan subIDEsVar bs = do
  debugPrint "Called clientMessageHandler"

  -- Decode a value, parse
  let val :: Aeson.Value
      val = er "eitherDecode" $ Aeson.eitherDecodeStrict bs
  msg <- either error id <$> parseClientMessageWithTracker (fromServerTracker trackers) val

  let sendClient :: LSP.FromServerMessage -> IO ()
      sendClient = atomically . writeTChan toClientChan . Aeson.encode

      castFromClientMessage :: LSP.FromClientMessage' (Product LSP.SMethod (Const FilePath)) -> LSP.FromClientMessage
      castFromClientMessage = \case
        LSP.FromClientMess method params -> LSP.FromClientMess method params
        LSP.FromClientRsp (Pair method _) params -> LSP.FromClientRsp method params

      sendSubproc :: FilePath -> LSP.FromClientMessage' (Product LSP.SMethod (Const FilePath)) -> IO ()
      sendSubproc path = sendSubprocUncast path . castFromClientMessage

      sendSubprocUncast :: FilePath -> LSP.FromClientMessage -> IO ()
      sendSubprocUncast path msg = do
        mHome <- sendSubproc_ path msg
        forM_ mHome $ \home -> do
          ide <- addNewSubIDE subIDEsVar initParamsVar trackers toClientChan home
          atomically $ writeTChan (ideInHandleChannel ide) $ Aeson.encode msg

      -- If a SubIDE is needed, returns the path out of the STM transaction
      sendSubproc_ :: FilePath -> LSP.FromClientMessage -> IO (Maybe FilePath)
      sendSubproc_ path msg = atomically $ do
        ides <- readTMVar subIDEsVar

        let mHome = find (`isPrefixOf` path) $ Map.keys ides
            mIde = mHome >>= flip Map.lookup ides

        case mIde of
          Just ide -> do
            writeTChan (ideInHandleChannel ide) (Aeson.encode msg)
            unsafeIOToSTM $ debugPrint $ "Found relevant SubIDE: " <> ideHomeDirectory ide
            pure Nothing
          Nothing -> do
            -- Safe as findHome only does reads
            mHome <- unsafeIOToSTM $ findHome path
            case mHome of
              -- Returned out of the transaction to be handled in IO
              Just home -> pure $ Just home
              Nothing -> do
                -- We get here if we cannot find a daml.yaml file for a file mentioned in a request
                -- if we're sending a response, ignore it, as this means the server that sent the request has been killed already.
                -- if we're sending a request, respond to the client with an error.
                -- if we're sending a notification, ignore it - theres nothing the protocol allows us to do to signify notification failures.
                let replyError :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request). LSP.SMethod m -> LSP.LspId m -> STM ()
                    replyError method id =
                      writeTChan toClientChan $ Aeson.encode $
                        LSP.FromServerRsp method $ LSP.ResponseMessage "2.0" (Just id) $ Left $
                          LSP.ResponseError LSP.InvalidParams "Could not find daml.yaml for this file." Nothing
                case msg of
                  LSP.FromClientMess method params ->
                    case (LSP.splitClientMethod method, params) of
                      (LSP.IsClientReq, LSP.RequestMessage {_id}) -> Nothing <$ replyError method _id
                      (LSP.IsClientEither, LSP.ReqMess (LSP.RequestMessage {_id})) -> Nothing <$ replyError method _id
                      _ -> pure Nothing
                  _ -> pure Nothing

      sendAllSubproc :: LSP.FromClientMessage' (Product LSP.SMethod (Const FilePath)) -> IO [FilePath]
      sendAllSubproc msg = atomically $ do
        ides <- readTMVar subIDEsVar
        if null ides then error "Got a broadcast to nothing :(" else pure ()
        forM (Map.elems ides) $ \ide -> ideHomeDirectory ide <$ writeTChan (ideInHandleChannel ide) (Aeson.encode $ castFromClientMessage msg)

  case msg of
    -- Store the initialize params for starting subIDEs, respond statically with what ghc-ide usually sends.
    LSP.FromClientMess LSP.SInitialize LSP.RequestMessage {_id, _method, _params} -> do
      putMVar initParamsVar _params
      sendClient $ LSP.FromServerRsp _method $ LSP.ResponseMessage "2.0" (Just _id) (Right initializeResult)
    LSP.FromClientMess meth params ->
      case getMessageForwardingBehaviour meth params of
        ForwardRequest mess (Single path) -> do
          debugPrint $ "single req on method " <> show meth <> " over path " <> path
          let LSP.RequestMessage {_id, _method} = mess
          putReqMethodSingleFromClient (fromClientTracker trackers) _id _method
          sendSubproc path msg

        ForwardRequest mess (AllRequest combine) -> do
          debugPrint $ "all req on method " <> show meth
          let LSP.RequestMessage {_id, _method} = mess
          ides <- sendAllSubproc msg
          putReqMethodAll (fromClientTracker trackers) _id _method ides combine

        ForwardNotification _ (Single path) -> do
          debugPrint $ "single not on method " <> show meth <> " over path " <> path
          sendSubproc path msg

        ForwardNotification _ AllNotification -> do
          debugPrint $ "all not on method " <> show meth
          void $ sendAllSubproc msg

        ExplicitHandler handler -> do
          handler sendClient sendSubprocUncast
    LSP.FromClientRsp (Pair method (Const home)) rMsg -> 
      sendSubprocUncast home $ LSP.FromClientRsp method $ 
        rMsg & LSP.id %~ fmap removeLspPrefix

runMultiIde :: [String] -> IO ()
runMultiIde sourceDeps = do
  hPrint stderr sourceDeps

  -- Request trackers for response messages
  (requestFromClientMethodTracker :: MethodTrackerVar 'LSP.FromClient) <- newTVarIO IM.emptyIxMap
  (requestFromServerMethodTracker :: MethodTrackerVar 'LSP.FromServer) <- newTVarIO IM.emptyIxMap
  let methodTrackerVars = MethodTrackerVars requestFromClientMethodTracker requestFromServerMethodTracker
  subIDEsVar <- newTMVarIO @SubIDEs mempty
  toClientChan <- atomically newTChan
  initParamsVar <- newEmptyMVar @InitParams

  debugPrint "Listening for bytes"
  -- Client <- *****
  toClientThread <- async $ forever $ do
    msg <- atomically $ readTChan toClientChan
    debugPrint "Pushing message to client"
    -- BSLC.hPutStrLn stderr msg
    putChunk stdout msg

  -- Client -> Coord
  clientToCoordThread <- async $ do
    chunks <- getChunks stdin
    mapM_ (clientMessageHandler methodTrackerVars initParamsVar toClientChan subIDEsVar) chunks

  let killAll :: IO ()
      killAll = do
        subIDEs <- atomically $ readTMVar subIDEsVar
        forM_ subIDEs shutdownIde

  installHandler sigTERM (Catch killAll) Nothing

  atomically $ do
    unsafeIOToSTM $ debugPrint "Running main loop"
    subIDEs <- readTMVar subIDEsVar
    let asyncs = concatMap (\subIDE -> [ideInhandleAsync subIDE, ideOutHandleAsync subIDE]) subIDEs
    errs <- lefts . catMaybes <$> traverse pollSTM (asyncs ++ [toClientThread, clientToCoordThread])
    when (not $ null errs) $
      unsafeIOToSTM $ debugPrint $ "A thread handler errored with: " <> show (head errs)

    let procs = ideProcess <$> subIDEs
    exits <- catMaybes <$> traverse getExitCodeSTM (Map.elems procs)
    when (not $ null exits) $
      unsafeIOToSTM $ debugPrint $ "A subIDE finished with code: " <> show (head exits)

    when (null exits && null errs) retry

  -- If we get here, something failed/stopped, so stop everything
  killAll

  pure ()
