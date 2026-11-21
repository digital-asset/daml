-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}

module DA.Cli.Damlc.Command.MultiIde.Util (
  module DA.Cli.Damlc.Command.MultiIde.Util,
  readTVarIO,
) where

import Control.Concurrent.Async (AsyncCancelled (..))
import Control.Concurrent.MVar
import Control.Concurrent.STM.TMVar
import Control.Exception (SomeException, fromException, handle, try, tryJust)
import Control.Lens ((^.), (&), (.~), (?~))
import Control.Monad (void)
import Control.Monad.STM
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except
import DA.Cli.Damlc.Command.MultiIde.Types
import DA.Daml.Package.Config (getSimplePathOrName, isDamlYamlContentForPackage)
import DA.Daml.Project.Config (readPackageConfig, queryPackageConfig, queryPackageConfigRequired)
import DA.Daml.Project.Consts (packageConfigName)
import DA.Daml.Project.Types (ConfigError (..))
import Data.Bifunctor (first)
import Data.Either (fromRight)
import Data.List.Extra (lower, replace)
import Data.Maybe (mapMaybe)
import qualified Data.Map as Map
import qualified Data.Text as T
import qualified Data.Text.Extended as T
import GHC.Conc (TVar, readTVar, readTVarIO, writeTVar)
import qualified Language.LSP.Protocol.Types as LSP
import qualified Language.LSP.Protocol.Message as LSP
import qualified Language.LSP.Protocol.Lens as LSP
import qualified Language.LSP.Types as OldLSP
import GHC.TypeLits (symbolVal)
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

modifyTVar :: TVar a -> (a -> (a, b)) -> STM b
modifyTVar var f = modifyTVarM var (pure . f)

modifyTVar_ :: TVar a -> (a -> a) -> STM ()
modifyTVar_ var f = modifyTVarM_ var (pure . f)

modifyTVarM :: TVar a -> (a -> STM (a, b)) -> STM b
modifyTVarM var f = do
  x <- readTVar var
  (x', b) <- f x
  writeTVar var x'
  pure b

modifyTVarM_ :: TVar a -> (a -> STM a) -> STM ()
modifyTVarM_ var f = modifyTVarM var (fmap (,()) . f)

writeTVarIO :: TVar a -> a -> IO ()
writeTVarIO var = atomically . writeTVar var

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
      { _positionEncoding = Nothing
      , _notebookDocumentSync = Nothing
      , _textDocumentSync = Just $ LSP.InL $ LSP.TextDocumentSyncOptions
          { _openClose = Just True
          , _change = Just LSP.TextDocumentSyncKind_Incremental
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
          , _completionItem = Nothing
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
          , _commands =
              [maybe "" (<> ".") mCommandPrefix <> "typesignature.add"]
          })
      , _selectionRangeProvider = false
      , _callHierarchyProvider = false
      , _semanticTokensProvider = Just $ LSP.InR $ LSP.SemanticTokensRegistrationOptions
          { _documentSelector = LSP.InR LSP.Null
          , _workDoneProgress = Nothing
          , _legend = LSP.SemanticTokensLegend
              { _tokenTypes = map LSP.toEnumBaseType
                  [ LSP.SemanticTokenTypes_Type
                  , LSP.SemanticTokenTypes_Class
                  , LSP.SemanticTokenTypes_Enum
                  , LSP.SemanticTokenTypes_Interface
                  , LSP.SemanticTokenTypes_Struct
                  , LSP.SemanticTokenTypes_TypeParameter
                  , LSP.SemanticTokenTypes_Parameter
                  , LSP.SemanticTokenTypes_Variable
                  , LSP.SemanticTokenTypes_Property
                  , LSP.SemanticTokenTypes_EnumMember
                  , LSP.SemanticTokenTypes_Event
                  , LSP.SemanticTokenTypes_Function
                  , LSP.SemanticTokenTypes_Method
                  , LSP.SemanticTokenTypes_Macro
                  , LSP.SemanticTokenTypes_Keyword
                  , LSP.SemanticTokenTypes_Modifier
                  , LSP.SemanticTokenTypes_Comment
                  , LSP.SemanticTokenTypes_String
                  , LSP.SemanticTokenTypes_Number
                  , LSP.SemanticTokenTypes_Regexp
                  , LSP.SemanticTokenTypes_Operator
                  ]
              , _tokenModifiers = map LSP.toEnumBaseType
                  [ LSP.SemanticTokenModifiers_Declaration
                  , LSP.SemanticTokenModifiers_Definition
                  , LSP.SemanticTokenModifiers_Readonly
                  , LSP.SemanticTokenModifiers_Static
                  , LSP.SemanticTokenModifiers_Deprecated
                  , LSP.SemanticTokenModifiers_Abstract
                  , LSP.SemanticTokenModifiers_Async
                  , LSP.SemanticTokenModifiers_Modification
                  , LSP.SemanticTokenModifiers_Documentation
                  , LSP.SemanticTokenModifiers_DefaultLibrary
                  ]
              }

          , _range = Nothing
          , _full = Nothing
          , _id = Nothing
          }
      , _workspaceSymbolProvider = false
      , _linkedEditingRangeProvider = Nothing
      , _monikerProvider = Nothing
      , _typeHierarchyProvider = Nothing
      , _inlineValueProvider = Nothing
      , _inlayHintProvider = Nothing
      , _diagnosticProvider = Nothing
      , _workspace = Just $ LSP.WorkspaceOptions
          { _workspaceFolders =
              Just (LSP.WorkspaceFoldersServerCapabilities {_supported = Just True, _changeNotifications = Just (LSP.InR True)})
          , _fileOperations = Nothing
          }
      , _experimental = Nothing
      }
  , _serverInfo = Nothing
  }
  where
    true = Just (LSP.InL True)
    false = Just (LSP.InL False)

initializeRequest :: InitParams -> SubIdeInstance -> LSP.FromClientMessage
initializeRequest initParams ide = LSP.FromClientMess LSP.SMethod_Initialize LSP.TRequestMessage
  { _id = LSP.IdString $ ideMessageIdPrefix ide <> "-init"
  , _method = LSP.SMethod_Initialize
  , _params = initParams
      & LSP.rootPath ?~ LSP.InL (T.pack $ unPackageHome $ ideHome ide)
      & LSP.rootUri .~ LSP.InL (LSP.filePathToUri $ unPackageHome $ ideHome ide)
  , _jsonrpc = "2.0"
  }

openFileNotification :: DamlFile -> T.Text -> LSP.FromClientMessage
openFileNotification path content = LSP.FromClientMess LSP.SMethod_TextDocumentDidOpen LSP.TNotificationMessage
  { _method = LSP.SMethod_TextDocumentDidOpen
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
closeFileNotification path = LSP.FromClientMess LSP.SMethod_TextDocumentDidClose LSP.TNotificationMessage
  {_method = LSP.SMethod_TextDocumentDidClose
  , _params = LSP.DidCloseTextDocumentParams $ LSP.TextDocumentIdentifier $
      LSP.filePathToUri $ unDamlFile path
  , _jsonrpc = "2.0"
  }

registerFileWatchersMessage :: FilePath -> LSP.FromServerMessage
registerFileWatchersMessage multiPackageHome =
  LSP.FromServerMess LSP.SMethod_ClientRegisterCapability $
    LSP.TRequestMessage "2.0" (LSP.IdString "MultiIdeWatchedFiles") LSP.SMethod_ClientRegisterCapability $ LSP.RegistrationParams
      [ LSP.toUntypedRegistration $ LSP.TRegistration "MultiIdeWatchedFiles" LSP.SMethod_WorkspaceDidChangeWatchedFiles $ Just $ LSP.DidChangeWatchedFilesRegistrationOptions
          [ watcher "**/multi-package.yaml"
          , watcher "**/daml.yaml"
          , watcher "**/*.dar"
          , watcher "**/*.daml"
          ]
      , LSP.toUntypedRegistration $ LSP.TRegistration "MultiIdeOpenFiles" LSP.SMethod_TextDocumentDidOpen $ Just documentOptions
      , LSP.toUntypedRegistration $ LSP.TRegistration "MultiIdeOpenFiles" LSP.SMethod_TextDocumentDidClose $ Just documentOptions
      ]
  where
    patternFor glob = LSP.Pattern $ T.pack $ multiPackageHome </> glob
    watcher glob = LSP.FileSystemWatcher (LSP.GlobPattern $ LSP.InL $ patternFor glob) Nothing
    documentOptions = LSP.TextDocumentRegistrationOptions $ LSP.InL $ LSP.DocumentSelector
      [ LSP.DocumentFilter $ LSP.InL $ LSP.TextDocumentFilter $ LSP.InR $ LSP.InR $
          LSP.TextDocumentFilterPattern Nothing Nothing (T.pack $ multiPackageHome </> "**/daml.yaml")
      ]

-- The single-package IDE still exposes lsp-types 1.x payloads.
toOldTextDocumentIdentifier :: LSP.TextDocumentIdentifier -> OldLSP.TextDocumentIdentifier
toOldTextDocumentIdentifier (LSP.TextDocumentIdentifier (LSP.Uri uri)) =
  OldLSP.TextDocumentIdentifier $ OldLSP.Uri uri

toOldPosition :: LSP.Position -> OldLSP.Position
toOldPosition (LSP.Position line character) =
  OldLSP.Position (fromIntegral line) (fromIntegral character)

fromOldLocation :: OldLSP.Location -> LSP.Location
fromOldLocation (OldLSP.Location (OldLSP.Uri uri) (OldLSP.Range start end)) =
  LSP.Location (LSP.Uri uri) $ LSP.Range (position start) (position end)
  where
    position (OldLSP.Position line character) =
      LSP.Position (fromIntegral line) (fromIntegral character)

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
      hasDamlYaml <- elem packageConfigName <$> listDirectory path
      if hasDamlYaml
        then shouldHandleDamlYamlChange <$> T.readFileUtf8 (path </> packageConfigName)
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
    package <- lift $ readPackageConfig $ toPackagePath path
    dataDeps <- except $ maybe [] (mapMaybe getSimplePathOrName) <$> queryPackageConfig ["data-dependencies"] package
    directDeps <- except $ maybe [] (mapMaybe getSimplePathOrName) <$> queryPackageConfig ["dependencies"] package
    let darDeps = filter (\dep -> takeExtension dep == ".dar") directDeps <> dataDeps
    canonDeps <- lift $ withCurrentDirectory (unPackageHome path) $ traverse canonicalizePath darDeps
    name <- except $ queryPackageConfigRequired ["name"] package
    version <- except $ queryPackageConfigRequired ["version"] package
    sdkVersion <- except $ queryPackageConfig ["sdk-version"] package

    let overrideMapToMaybe :: T.Text -> Map.Map T.Text T.Text -> Either ConfigError (Maybe T.Text)
        overrideMapToMaybe componentName m =
          case (Map.lookup "version" m, Map.lookup "local-path" m) of
            (Just _, Just _) -> Left $ makeConfigError "Both version and local-path fields specified, only one can be provided"
            (Just v, Nothing) -> Right $ Just v
            (Nothing, Just _) -> Right Nothing
            (Nothing, Nothing) -> Left $ makeConfigError "Missing version or local-path field"
          where
            makeConfigError :: String -> ConfigError
            makeConfigError = ConfigFieldInvalid "package" ["component-overrides", componentName]
    componentOverrides <-
      except $ queryPackageConfig @(Map.Map T.Text (Map.Map T.Text T.Text)) ["override-components"] package
        >>= maybe (pure mempty) (Map.traverseWithKey overrideMapToMaybe)
    
    pure PackageSummary
      { psUnitId = UnitId $ name <> "-" <> version
      , psDeps = DarFile . toPosixFilePath <$> canonDeps
      , psSdkVersionData = SdkVersionData
          { svdVersion = sdkVersion
          , svdOverrides = componentOverrides
          }
      }

-- LSP requires all requests are replied to. When we don't have a working IDE (say the daml.yaml is malformed), we need to reply
-- We don't want to reply with LSP errors, as there will be too many. Instead, we show our error in diagnostics, and send empty replies
noIDEReply :: LSP.FromClientMessage -> Maybe LSP.FromServerMessage
noIDEReply (LSP.FromClientMess method params) =
  case (method, params) of
    (LSP.SMethod_TextDocumentWillSaveWaitUntil, _) -> makeRes params $ LSP.InL []
    (LSP.SMethod_TextDocumentCompletion, _) -> makeRes params $ LSP.InL []
    (LSP.SMethod_TextDocumentHover, _) -> makeRes params $ LSP.InR LSP.Null
    (LSP.SMethod_TextDocumentSignatureHelp, _) -> makeRes params $ LSP.InL $ LSP.SignatureHelp [] Nothing Nothing
    (LSP.SMethod_TextDocumentDeclaration, _) -> makeRes params $ LSP.InL $ LSP.Declaration $ LSP.InR []
    (LSP.SMethod_TextDocumentDefinition, _) -> makeRes params $ LSP.InL $ LSP.Definition $ LSP.InR []
    (LSP.SMethod_TextDocumentDocumentSymbol, _) -> makeRes params $ LSP.InR $ LSP.InL []
    (LSP.SMethod_TextDocumentCodeAction, _) -> makeRes params $ LSP.InL []
    (LSP.SMethod_TextDocumentCodeLens, _) -> makeRes params $ LSP.InL []
    (LSP.SMethod_TextDocumentDocumentLink, _) -> makeRes params $ LSP.InL []
    (LSP.SMethod_TextDocumentColorPresentation, _) -> makeRes params []
    (LSP.SMethod_TextDocumentOnTypeFormatting, _) -> makeRes params $ LSP.InL []
    (LSP.SMethod_WorkspaceExecuteCommand, _) -> makeRes params $ LSP.InR LSP.Null
    (LSP.SMethod_CustomMethod (symbolVal -> "daml/tryGetDefinition"), LSP.ReqMess params) -> noDefinitionRes params
    (LSP.SMethod_CustomMethod (symbolVal -> "daml/gotoDefinitionByName"), LSP.ReqMess params) -> noDefinitionRes params
    _ -> Nothing
  where
    makeRes :: forall (m :: LSP.Method 'LSP.ClientToServer 'LSP.Request). LSP.TRequestMessage m -> LSP.MessageResult m -> Maybe LSP.FromServerMessage
    makeRes params result = Just $ LSP.FromServerRsp (params ^. LSP.method) $ LSP.TResponseMessage "2.0" (Just $ params ^. LSP.id) (Right result)
    noDefinitionRes :: forall (m :: LSP.Method 'LSP.ClientToServer 'LSP.Request). LSP.TRequestMessage m -> Maybe LSP.FromServerMessage
    noDefinitionRes params = Just $ LSP.FromServerRsp LSP.SMethod_TextDocumentDefinition $ LSP.TResponseMessage "2.0" (Just $ castLspId $ params ^. LSP.id) $
      Right $ LSP.InL $ LSP.Definition $ LSP.InR []
noIDEReply _ = Nothing

-- | Publishes an error diagnostic for a file containing the given message
fullFileDiagnostic :: LSP.DiagnosticSeverity -> String -> FilePath -> LSP.FromServerMessage
fullFileDiagnostic severity message path = LSP.FromServerMess LSP.SMethod_TextDocumentPublishDiagnostics $ LSP.TNotificationMessage "2.0" LSP.SMethod_TextDocumentPublishDiagnostics
  $ LSP.PublishDiagnosticsParams (LSP.filePathToUri path) Nothing [LSP.Diagnostic
    { _range = LSP.Range (LSP.Position 0 0) (LSP.Position 0 1000)
    , _severity = Just severity
    , _code = Nothing
    , _codeDescription = Nothing
    , _source = Just "Daml Multi-IDE"
    , _message = T.pack message
    , _tags = Nothing
    , _relatedInformation = Nothing
    , _data_ = Nothing
    }]

-- | Clears diagnostics for a given file
clearDiagnostics :: FilePath -> LSP.FromServerMessage
clearDiagnostics path = LSP.FromServerMess LSP.SMethod_TextDocumentPublishDiagnostics $ LSP.TNotificationMessage "2.0" LSP.SMethod_TextDocumentPublishDiagnostics
  $ LSP.PublishDiagnosticsParams (LSP.filePathToUri path) Nothing []

showMessageRequest :: LSP.LspId 'LSP.Method_WindowShowMessageRequest -> LSP.MessageType -> T.Text -> [T.Text] -> LSP.FromServerMessage
showMessageRequest lspId messageType message options = LSP.FromServerMess LSP.SMethod_WindowShowMessageRequest $
  LSP.TRequestMessage "2.0" lspId LSP.SMethod_WindowShowMessageRequest $ LSP.ShowMessageRequestParams messageType message $
    Just $ LSP.MessageActionItem <$> options

showMessage :: LSP.MessageType -> T.Text -> LSP.FromServerMessage
showMessage messageType message = LSP.FromServerMess LSP.SMethod_WindowShowMessage $
  LSP.TNotificationMessage "2.0" LSP.SMethod_WindowShowMessage $ LSP.ShowMessageParams messageType message

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
