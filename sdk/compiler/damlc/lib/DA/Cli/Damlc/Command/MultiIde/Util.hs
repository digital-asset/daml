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
import Control.Monad.STM
import DA.Daml.Project.Config (readProjectConfig, queryProjectConfigRequired)
import DA.Daml.Project.Types (ConfigError, ProjectPath (..))
import qualified Language.LSP.Types as LSP
import qualified Language.LSP.Types.Capabilities as LSP
import System.Directory (doesDirectoryExist, listDirectory)
import System.FilePath (takeDirectory)
import System.IO.Extra
import System.IO.Unsafe (unsafePerformIO)

-- Stop mangling my prints! >:(
{-# ANN printLock ("HLint: ignore Avoid restricted function" :: String) #-}
{-# NOINLINE printLock #-}
printLock :: MVar ()
printLock = unsafePerformIO $ newMVar ()

debugPrint :: String -> IO ()
debugPrint msg = withMVar printLock $ \_ -> do
  hPutStrLn stderr msg
  hFlush stderr

er :: Show x => String -> Either x a -> a
er _msg (Right a) = a
er msg (Left e) = error $ msg <> ": " <> show e

modifyTMVar :: TMVar a -> (a -> a) -> STM ()
modifyTMVar var f = do
  x <- takeTMVar var
  putTMVar var (f x)

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
      hasDamlYaml <- elem "daml.yaml" <$> listDirectory path
      if hasDamlYaml
        then pure $ Just path
        else do
          let newPath = takeDirectory path
          if path == newPath
            then pure Nothing
            else aux newPath

unitIdFromDamlYaml :: FilePath -> IO (Either ConfigError String)
unitIdFromDamlYaml path = do
  handle (\(e :: ConfigError) -> return $ Left e) $ do
    project <- readProjectConfig $ ProjectPath path
    pure $ do
      name <- queryProjectConfigRequired ["name"] project
      version <- queryProjectConfigRequired ["version"] project
      pure $ name <> "-" <> version
