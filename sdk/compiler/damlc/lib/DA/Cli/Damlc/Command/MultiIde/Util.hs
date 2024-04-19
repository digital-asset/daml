-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE ApplicativeDo       #-}
{-# LANGUAGE RankNTypes       #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE GADTs #-}

module DA.Cli.Damlc.Command.MultiIde.Util (
  module DA.Cli.Damlc.Command.MultiIde.Util
) where

import Control.Concurrent.MVar
import Control.Concurrent.STM.TMVar
import Control.Exception (handle)
import Control.Lens ((^.))
import Control.Monad.STM
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except
import DA.Daml.Project.Config (readProjectConfig, queryProjectConfig, queryProjectConfigRequired)
import DA.Daml.Project.Consts (projectConfigName)
import DA.Daml.Project.Types (ConfigError, ProjectPath (..))
import Data.Aeson (Value (Null))
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import qualified Language.LSP.Types as LSP
import qualified Language.LSP.Types.Lens as LSP
import qualified Language.LSP.Types.Capabilities as LSP
import System.Directory (doesDirectoryExist, listDirectory, withCurrentDirectory, canonicalizePath)
import System.FilePath (takeDirectory, takeExtension)

er :: Show x => String -> Either x a -> a
er _msg (Right a) = a
er msg (Left e) = error $ msg <> ": " <> show e

makeIOBlocker :: IO (IO a -> IO a, IO ())
makeIOBlocker = do
  sendBlocker <- newEmptyMVar @()
  let unblock = putMVar sendBlocker ()
      onceUnblocked = (readMVar sendBlocker >>)
  pure (onceUnblocked, unblock)

modifyTMVar :: TMVar a -> (a -> a) -> STM ()
modifyTMVar var f = modifyTMVarM var (pure . f)

modifyTMVarM :: TMVar a -> (a -> STM a) -> STM ()
modifyTMVarM var f = do
  x <- takeTMVar var
  x' <- f x
  putTMVar var x'

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

registerFileWatchersMessage :: LSP.RequestMessage 'LSP.ClientRegisterCapability
registerFileWatchersMessage =
  LSP.RequestMessage "2.0" (LSP.IdString "MultiIdeWatchedFiles") LSP.SClientRegisterCapability $ LSP.RegistrationParams $ LSP.List
    [ LSP.SomeRegistration $ LSP.Registration "MultiIdeWatchedFiles" LSP.SWorkspaceDidChangeWatchedFiles $ LSP.DidChangeWatchedFilesRegistrationOptions $ LSP.List
      [ LSP.FileSystemWatcher "**/multi-package.yaml" Nothing
      , LSP.FileSystemWatcher "**/daml.yaml" Nothing
      , LSP.FileSystemWatcher "**/*.dar" Nothing
      , LSP.FileSystemWatcher "**/*.daml" Nothing
      ]
    ]

castLspId :: LSP.LspId m -> LSP.LspId m'
castLspId (LSP.IdString s) = LSP.IdString s
castLspId (LSP.IdInt i) = LSP.IdInt i

-- Given a file path, move up directory until we find a daml.yaml and give its path (if it exists)
findHome :: FilePath -> IO (Maybe FilePath)
findHome path = do
  exists <- doesDirectoryExist path
  if exists then aux path else aux (takeDirectory path)
  where
    aux :: FilePath -> IO (Maybe FilePath)
    aux path = do
      hasDamlYaml <- elem projectConfigName <$> listDirectory path
      if hasDamlYaml
        then pure $ Just path
        else do
          let newPath = takeDirectory path
          if path == newPath
            then pure Nothing
            else aux newPath

unitIdAndDepsFromDamlYaml :: FilePath -> IO (Either ConfigError (String, [FilePath]))
unitIdAndDepsFromDamlYaml path = do
  handle (\(e :: ConfigError) -> return $ Left e) $ runExceptT $ do
    project <- lift $ readProjectConfig $ ProjectPath path
    dataDeps <- except $ fromMaybe [] <$> queryProjectConfig ["data-dependencies"] project
    directDeps <- except $ fromMaybe [] <$> queryProjectConfig ["dependencies"] project
    let directDarDeps = filter (\dep -> takeExtension dep == ".dar") directDeps
    canonDeps <- lift $ withCurrentDirectory path $ traverse canonicalizePath $ dataDeps <> directDarDeps
    name <- except $ queryProjectConfigRequired ["name"] project
    version <- except $ queryProjectConfigRequired ["version"] project
    pure (name <> "-" <> version, canonDeps)

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
fullFileDiagnostic :: String -> FilePath -> LSP.FromServerMessage
fullFileDiagnostic message path = LSP.FromServerMess LSP.STextDocumentPublishDiagnostics $ LSP.NotificationMessage "2.0" LSP.STextDocumentPublishDiagnostics 
  $ LSP.PublishDiagnosticsParams (LSP.filePathToUri path) Nothing $ LSP.List [LSP.Diagnostic 
    { _range = LSP.Range (LSP.Position 0 0) (LSP.Position 0 1000)
    , _severity = Just LSP.DsError
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
