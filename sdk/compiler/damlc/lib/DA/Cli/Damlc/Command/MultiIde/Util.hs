-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}

module DA.Cli.Damlc.Command.MultiIde.Util (
  module DA.Cli.Damlc.Command.MultiIde.Util
) where

import Control.Concurrent.Async (AsyncCancelled (..))
import Control.Concurrent.MVar
import Control.Concurrent.STM.TMVar
import Control.Exception (SomeException, fromException, handle, try, tryJust)
import Control.Lens ((^.))
import Control.Monad (void)
import Control.Monad.STM
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except
import DA.Cli.Damlc.Command.MultiIde.Types
import DA.Daml.Package.Config (isDamlYamlContentForPackage)
import DA.Daml.Project.Config (readProjectConfig, queryProjectConfig, queryProjectConfigRequired)
import DA.Daml.Project.Consts (projectConfigName)
import DA.Daml.Project.Types (ConfigError (..), parseUnresolvedVersion)
import Data.Aeson (Value (Null))
import Data.Bifunctor (first)
import Data.Either (fromRight)
import Data.List.Extra (lower, replace)
import Data.Maybe (fromMaybe)
import qualified Data.Map as Map
import qualified Data.Text as T
import qualified Data.Text.Extended as T
import qualified Language.LSP.Types as LSP
import qualified Language.LSP.Types.Lens as LSP
import qualified Language.LSP.Types.Capabilities as LSP
import System.Directory (doesDirectoryExist, listDirectory, withCurrentDirectory, canonicalizePath)
import qualified System.FilePath as NativeFilePath
import System.FilePath.Posix (joinDrive, takeDirectory, takeExtension, (</>))
import System.IO (Handle, hClose, hFlush)
import Text.Read (readMaybe)

er :: Show x => String -> Either x a -> a
er _msg (Right a) = a
er msg (Left e) = error $ msg <> ": " <> show e

makeIOBlocker :: IO (IO a -> IO a, IO ())
makeIOBlocker = do
  sendBlocker <- newEmptyMVar @()
  let unblock = putMVar sendBlocker ()
      onceUnblocked = (readMVar sendBlocker >>)
  pure (onceUnblocked, unblock)

modifyTMVar :: TMVar a -> (a -> (a, b)) -> STM b
modifyTMVar var f = modifyTMVarM var (pure . f)

modifyTMVar_ :: TMVar a -> (a -> a) -> STM ()
modifyTMVar_ var f = modifyTMVarM_ var (pure . f)

modifyTMVarM :: TMVar a -> (a -> STM (a, b)) -> STM b
modifyTMVarM var f = do
  x <- takeTMVar var
  (x', b) <- f x
  putTMVar var x'
  pure b

modifyTMVarM_ :: TMVar a -> (a -> STM a) -> STM ()
modifyTMVarM_ var f = modifyTMVarM var (fmap (,()) . f)

onSubIde :: MultiIdeState -> PackageHome -> (SubIdeData -> (SubIdeData, a)) -> STM a
onSubIde miState home f =
  modifyTMVar (misSubIdesVar miState) $ \subIdes ->
    let (subIde, res) = f $ lookupSubIde home subIdes
     in (Map.insert home subIde subIdes, res)

onSubIde_ :: MultiIdeState -> PackageHome -> (SubIdeData -> SubIdeData) -> STM ()
onSubIde_ miState home f = onSubIde miState home ((,()) . f)

-- Taken directly from the Initialize response
initializeResult :: Maybe T.Text -> LSP.InitializeResult
initializeResult mCommandPrefix = LSP.InitializeResult 
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
      , _executeCommandProvider = Just (LSP.ExecuteCommandOptions
          { _workDoneProgress = Nothing
          , _commands = LSP.List
              [maybe "" (<> ".") mCommandPrefix <> "typesignature.add"]
          })
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

initializeRequest :: InitParams -> SubIdeInstance -> LSP.FromClientMessage
initializeRequest initParams ide = LSP.FromClientMess LSP.SInitialize LSP.RequestMessage 
  { _id = LSP.IdString $ ideMessageIdPrefix ide <> "-init"
  , _method = LSP.SInitialize
  , _params = initParams
      { LSP._rootPath = Just $ T.pack $ unPackageHome $ ideHome ide
      , LSP._rootUri = Just $ LSP.filePathToUri $ unPackageHome $ ideHome ide
      }
  , _jsonrpc = "2.0"
  }

openFileNotification :: DamlFile -> T.Text -> LSP.FromClientMessage
openFileNotification path content = LSP.FromClientMess LSP.STextDocumentDidOpen LSP.NotificationMessage
  { _method = LSP.STextDocumentDidOpen
  , _params = LSP.DidOpenTextDocumentParams
    { _textDocument = LSP.TextDocumentItem
      { _uri = LSP.filePathToUri $ unDamlFile path
      , _languageId = "daml"
      , _version = 1
      , _text = content
      }
    }
  , _jsonrpc = "2.0"
  }

closeFileNotification :: DamlFile -> LSP.FromClientMessage
closeFileNotification path = LSP.FromClientMess LSP.STextDocumentDidClose LSP.NotificationMessage
  {_method = LSP.STextDocumentDidClose
  , _params = LSP.DidCloseTextDocumentParams $ LSP.TextDocumentIdentifier $
      LSP.filePathToUri $ unDamlFile path
  , _jsonrpc = "2.0"
  }

registerFileWatchersMessage :: FilePath -> LSP.FromServerMessage
registerFileWatchersMessage multiPackageHome =
  LSP.FromServerMess LSP.SClientRegisterCapability $
    LSP.RequestMessage "2.0" (LSP.IdString "MultiIdeWatchedFiles") LSP.SClientRegisterCapability $ LSP.RegistrationParams $ LSP.List
      [ LSP.SomeRegistration $ LSP.Registration "MultiIdeWatchedFiles" LSP.SWorkspaceDidChangeWatchedFiles $ LSP.DidChangeWatchedFilesRegistrationOptions $ LSP.List
        [ LSP.FileSystemWatcher (T.pack $ multiPackageHome </> "**/multi-package.yaml") Nothing
        , LSP.FileSystemWatcher (T.pack $ multiPackageHome </> "**/daml.yaml") Nothing
        , LSP.FileSystemWatcher (T.pack $ multiPackageHome </> "**/*.dar") Nothing
        , LSP.FileSystemWatcher (T.pack $ multiPackageHome </> "**/*.daml") Nothing
        ]
      , LSP.SomeRegistration $ LSP.Registration "MultiIdeOpenFiles" LSP.STextDocumentDidOpen $ LSP.TextDocumentRegistrationOptions $ Just $ LSP.List
        [ LSP.DocumentFilter Nothing Nothing $ Just $ T.pack $ multiPackageHome </> "**/daml.yaml"
        ]
      , LSP.SomeRegistration $ LSP.Registration "MultiIdeOpenFiles" LSP.STextDocumentDidClose $ LSP.TextDocumentRegistrationOptions $ Just $ LSP.List
        [ LSP.DocumentFilter Nothing Nothing $ Just $ T.pack $ multiPackageHome </> "**/daml.yaml"
        ]
      ]

castLspId :: LSP.LspId m -> LSP.LspId m'
castLspId (LSP.IdString s) = LSP.IdString s
castLspId (LSP.IdInt i) = LSP.IdInt i

-- Given a file path, move up directory until we find a daml.yaml and give its path (if it exists)
findHome :: FilePath -> IO (Maybe PackageHome)
findHome path = do
  exists <- doesDirectoryExist path
  if exists then aux path else aux (takeDirectory path)
  where
    hasValidDamlYaml :: FilePath -> IO Bool
    hasValidDamlYaml path = do
      hasDamlYaml <- elem projectConfigName <$> listDirectory path
      if hasDamlYaml
        then shouldHandleDamlYamlChange <$> T.readFileUtf8 (path </> projectConfigName)
        else pure False

    aux :: FilePath -> IO (Maybe PackageHome)
    aux path = do
      isValid <- hasValidDamlYaml path
      if isValid
        then pure $ Just $ PackageHome path
        else do
          let newPath = takeDirectory path
          if path == newPath
            then pure Nothing
            else aux newPath

packageSummaryFromDamlYaml :: PackageHome -> IO (Either ConfigError PackageSummary)
packageSummaryFromDamlYaml path = do
  handle (\(e :: ConfigError) -> return $ Left e) $ runExceptT $ do
    project <- lift $ readProjectConfig $ toProjectPath path
    dataDeps <- except $ fromMaybe [] <$> queryProjectConfig ["data-dependencies"] project
    directDeps <- except $ fromMaybe [] <$> queryProjectConfig ["dependencies"] project
    let directDarDeps = filter (\dep -> takeExtension dep == ".dar") directDeps
    canonDeps <- lift $ withCurrentDirectory (unPackageHome path) $ traverse canonicalizePath $ dataDeps <> directDarDeps
    name <- except $ queryProjectConfigRequired ["name"] project
    version <- except $ queryProjectConfigRequired ["version"] project
    releaseVersion <- except $ queryProjectConfigRequired ["sdk-version"] project
    -- Default error gives too much information, e.g. `Invalid SDK version  "2.8.e": Failed reading: takeWhile1`
    -- Just saying its invalid is enough
    unresolvedReleaseVersion <- except $ first (const $ ConfigFieldInvalid "project" ["sdk-version"] $ "Invalid Daml SDK version: " <> T.unpack releaseVersion) 
      $ parseUnresolvedVersion releaseVersion
    pure PackageSummary
      { psUnitId = UnitId $ name <> "-" <> version
      , psDeps = DarFile . toPosixFilePath <$> canonDeps
      , psReleaseVersion = unresolvedReleaseVersion
      }

-- LSP requires all requests are replied to. When we don't have a working IDE (say the daml.yaml is malformed), we need to reply
-- We don't want to reply with LSP errors, as there will be too many. Instead, we show our error in diagnostics, and send empty replies
noIDEReply :: LSP.FromClientMessage -> Maybe LSP.FromServerMessage
noIDEReply (LSP.FromClientMess method params) =
  case (method, params) of
    (LSP.STextDocumentWillSaveWaitUntil, _) -> makeRes params $ LSP.List []
    (LSP.STextDocumentCompletion, _) -> makeRes params $ LSP.InL $ LSP.List []
    (LSP.STextDocumentHover, _) -> makeRes params Nothing
    (LSP.STextDocumentSignatureHelp, _) -> makeRes params $ LSP.SignatureHelp (LSP.List []) Nothing Nothing
    (LSP.STextDocumentDeclaration, _) -> makeRes params $ LSP.InR $ LSP.InL $ LSP.List []
    (LSP.STextDocumentDefinition, _) -> makeRes params $ LSP.InR $ LSP.InL $ LSP.List []
    (LSP.STextDocumentDocumentSymbol, _) -> makeRes params $ LSP.InL $ LSP.List []
    (LSP.STextDocumentCodeAction, _) -> makeRes params $ LSP.List []
    (LSP.STextDocumentCodeLens, _) -> makeRes params $ LSP.List []
    (LSP.STextDocumentDocumentLink, _) -> makeRes params $ LSP.List []
    (LSP.STextDocumentColorPresentation, _) -> makeRes params $ LSP.List []
    (LSP.STextDocumentOnTypeFormatting, _) -> makeRes params $ LSP.List []
    (LSP.SWorkspaceExecuteCommand, _) -> makeRes params Null
    (LSP.SCustomMethod "daml/tryGetDefinition", LSP.ReqMess params) -> noDefinitionRes params
    (LSP.SCustomMethod "daml/gotoDefinitionByName", LSP.ReqMess params) -> noDefinitionRes params
    _ -> Nothing
  where
    makeRes :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request). LSP.RequestMessage m -> LSP.ResponseResult m -> Maybe LSP.FromServerMessage
    makeRes params result = Just $ LSP.FromServerRsp (params ^. LSP.method) $ LSP.ResponseMessage "2.0" (Just $ params ^. LSP.id) (Right result)
    noDefinitionRes :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request). LSP.RequestMessage m -> Maybe LSP.FromServerMessage
    noDefinitionRes params = Just $ LSP.FromServerRsp LSP.STextDocumentDefinition $ LSP.ResponseMessage "2.0" (Just $ castLspId $ params ^. LSP.id) $
      Right $ LSP.InR $ LSP.InL $ LSP.List []
noIDEReply _ = Nothing

-- | Publishes an error diagnostic for a file containing the given message
fullFileDiagnostic :: LSP.DiagnosticSeverity -> String -> FilePath -> LSP.FromServerMessage
fullFileDiagnostic severity message path = LSP.FromServerMess LSP.STextDocumentPublishDiagnostics $ LSP.NotificationMessage "2.0" LSP.STextDocumentPublishDiagnostics 
  $ LSP.PublishDiagnosticsParams (LSP.filePathToUri path) Nothing $ LSP.List [LSP.Diagnostic 
    { _range = LSP.Range (LSP.Position 0 0) (LSP.Position 0 1000)
    , _severity = Just severity
    , _code = Nothing
    , _source = Just "Daml Multi-IDE"
    , _message = T.pack message
    , _tags = Nothing
    , _relatedInformation = Nothing
    }]

-- | Clears diagnostics for a given file
clearDiagnostics :: FilePath -> LSP.FromServerMessage
clearDiagnostics path = LSP.FromServerMess LSP.STextDocumentPublishDiagnostics $ LSP.NotificationMessage "2.0" LSP.STextDocumentPublishDiagnostics 
  $ LSP.PublishDiagnosticsParams (LSP.filePathToUri path) Nothing $ LSP.List []

showMessageRequest :: LSP.LspId 'LSP.WindowShowMessageRequest -> LSP.MessageType -> T.Text -> [T.Text] -> LSP.FromServerMessage
showMessageRequest lspId messageType message options = LSP.FromServerMess LSP.SWindowShowMessageRequest $
  LSP.RequestMessage "2.0" lspId LSP.SWindowShowMessageRequest $ LSP.ShowMessageRequestParams messageType message $
    Just $ LSP.MessageActionItem <$> options

showMessage :: LSP.MessageType -> T.Text -> LSP.FromServerMessage
showMessage messageType message = LSP.FromServerMess LSP.SWindowShowMessage $
  LSP.NotificationMessage "2.0" LSP.SWindowShowMessage $ LSP.ShowMessageParams messageType message

fromClientRequestLspId :: LSP.FromClientMessage -> Maybe LSP.SomeLspId
fromClientRequestLspId (LSP.FromClientMess method params) =
  case (LSP.splitClientMethod method, params) of
    (LSP.IsClientReq, _) -> Just $ LSP.SomeLspId $ params ^. LSP.id
    (LSP.IsClientEither, LSP.ReqMess params) -> Just $ LSP.SomeLspId $ params ^. LSP.id
    _ -> Nothing
fromClientRequestLspId _ = Nothing

fromClientRequestMethod :: LSP.FromClientMessage -> LSP.SomeMethod
fromClientRequestMethod (LSP.FromClientMess method _) = LSP.SomeMethod method
fromClientRequestMethod (LSP.FromClientRsp method _) = LSP.SomeMethod method

-- Windows can throw errors like `resource vanished` on dead handles, instead of being a no-op
-- In those cases, we're already convinced the handle is closed, so we simply "try" to close handles
-- and accept whatever happens
hTryClose :: Handle -> IO ()
hTryClose handle = void $ try @SomeException $ hClose handle

-- hFlush will error if the handle closes while its blocked on flushing
-- We don't care what happens in this event, so we ignore the error as with tryClose
hTryFlush :: Handle -> IO ()
hTryFlush handle = void $ try @SomeException $ hFlush handle

-- Changes backslashes to forward slashes, lowercases the drive
-- Need native filepath for splitDrive, as Posix version just takes first n `/`s
toPosixFilePath :: FilePath -> FilePath
toPosixFilePath = uncurry joinDrive . first lower . NativeFilePath.splitDrive . replace "\\" "/"

-- Attempts to exact the percent amount from a string containing strings like 30%
extractPercentFromText :: T.Text -> Maybe Integer
extractPercentFromText = readMaybe . T.unpack . T.dropWhile (==' ') . T.takeEnd 3 . T.dropEnd 1 . fst . T.breakOnEnd "%"

-- try @SomeException that doesn't catch AsyncCancelled exceptions
tryForwardAsync :: IO a -> IO (Either SomeException a)
tryForwardAsync = tryJust @SomeException $ \case
  (fromException -> Just AsyncCancelled) -> Nothing
  e -> Just e

shouldHandleDamlYamlChange :: T.Text -> Bool
shouldHandleDamlYamlChange = fromRight True . isDamlYamlContentForPackage
