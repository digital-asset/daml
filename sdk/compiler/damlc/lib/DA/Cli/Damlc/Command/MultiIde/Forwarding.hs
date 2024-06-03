-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE MultiParamTypeClasses #-}

-- We generate missing instances for SignatureHelpParams
{-# OPTIONS_GHC -fno-warn-orphans #-}

module DA.Cli.Damlc.Command.MultiIde.Forwarding (
  getMessageForwardingBehaviour,
  filePathFromParamsWithTextDocument,
  Forwarding (..),
  ForwardingBehaviour (..),
  ResponseCombiner,
) where

import Control.Applicative ((<|>))
import Control.Lens
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Lens as Aeson
import qualified Data.HashMap.Strict as HM
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import Development.IDE.Core.Rules.Daml (uriToVirtualResource)
import Development.IDE.Core.RuleTypes.Daml (VirtualResource (..))
import qualified Language.LSP.Types as LSP
import qualified Language.LSP.Types.Lens as LSP
import qualified Network.URI as URI
import DA.Cli.Damlc.Command.MultiIde.Types

{-# ANN module ("HLint: ignore Avoid restricted flags" :: String) #-}

-- SignatureHelpParams has no lenses from Language.LSP.Types.Lens
-- We just need this one, so we'll write it ourselves
makeLensesFor [("_textDocument", "signatureHelpParamsTextDocumentLens")] ''LSP.SignatureHelpParams
instance LSP.HasTextDocument LSP.SignatureHelpParams LSP.TextDocumentIdentifier where
  textDocument = signatureHelpParamsTextDocumentLens

pullMonadThroughTuple :: Monad m => (a, m b) -> m (a, b)
pullMonadThroughTuple (a, mb) = (a,) <$> mb

-- Takes a natural transformation of responses and lifts it to forward the first error
assumeSuccessCombiner
  :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
  .  ([(PackageHome, LSP.ResponseResult m)] -> LSP.ResponseResult m)
  -> ResponseCombiner m
assumeSuccessCombiner f res = f <$> mapM pullMonadThroughTuple res

ignore :: Forwarding m
ignore = ExplicitHandler $ \_ _ -> pure ()

showError :: T.Text -> Forwarding m
showError err = ExplicitHandler $ \sendClient _ ->
  sendClient $ LSP.FromServerMess LSP.SWindowShowMessage
    $ LSP.NotificationMessage "2.0" LSP.SWindowShowMessage
      $ LSP.ShowMessageParams LSP.MtError err

showFatal :: T.Text -> Forwarding m
showFatal err = showError $ "FATAL ERROR:\n" <> err <> "\nPlease report this on the daml forums."

handleElsewhere :: T.Text -> Forwarding m
handleElsewhere name = showFatal $ "Got unexpected " <> name <> " message in forwarding handler, this message should have been handled elsewhere."

unsupported :: T.Text -> Forwarding m
unsupported name = showFatal $ "Attempted to call a method that is unsupported by the underlying IDEs: " <> name

uriFilePathPrism :: Prism' LSP.Uri FilePath
uriFilePathPrism = prism' LSP.filePathToUri LSP.uriToFilePath

getMessageForwardingBehaviour
  :: forall t (m :: LSP.Method 'LSP.FromClient t)
  .  MultiIdeState
  -> LSP.SMethod m
  -> LSP.Message m
  -> Forwarding m
getMessageForwardingBehaviour miState meth params =
  case meth of
    LSP.SInitialize -> handleElsewhere "Initialize"
    LSP.SInitialized -> ignore
    -- send to all then const reply
    LSP.SShutdown -> ForwardRequest params $ AllRequest (assumeSuccessCombiner @m $ const LSP.Empty)
    LSP.SExit -> handleElsewhere "Exit"
    LSP.SWorkspaceDidChangeWorkspaceFolders -> ForwardNotification params AllNotification
    LSP.SWorkspaceDidChangeConfiguration -> ForwardNotification params AllNotification
    LSP.SWorkspaceDidChangeWatchedFiles -> ForwardNotification params AllNotification
    LSP.STextDocumentDidOpen -> ForwardNotification params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.STextDocumentDidChange -> ForwardNotification params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.STextDocumentWillSave -> ForwardNotification params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.STextDocumentWillSaveWaitUntil -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.STextDocumentDidSave -> ForwardNotification params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.STextDocumentDidClose -> ForwardNotification params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.STextDocumentCompletion -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.STextDocumentHover -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.STextDocumentSignatureHelp -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.STextDocumentDeclaration -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.STextDocumentDefinition -> handleElsewhere "TextDocumentDefinition"
    LSP.STextDocumentDocumentSymbol -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.STextDocumentCodeAction -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.STextDocumentCodeLens -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.STextDocumentDocumentLink -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.STextDocumentColorPresentation -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.STextDocumentOnTypeFormatting -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    
    LSP.SCustomMethod "daml/keepAlive" ->
      case params of
        LSP.ReqMess LSP.RequestMessage {_id, _method, _params} -> ExplicitHandler $ \sendClient _ ->
          sendClient $ LSP.FromServerRsp _method $ LSP.ResponseMessage "2.0" (Just _id) (Right Aeson.Null)
        _ -> showFatal "Got unpexpected daml/keepAlive response type from client"

    -- Other custom messages are notifications from server
    LSP.SCustomMethod _ -> ignore

    -- We only add the typesignature.add command, which simply sends a WorkspaceEdit with a single file modification
    -- We can take the file path from that modification
    LSP.SWorkspaceExecuteCommand ->
      case params ^. LSP.params . LSP.command of
        "typesignature.add" ->
          -- Fun lens:
          -- RequestMessage -> ExecuteCommandParams -> Aeson.Value -> WorkspaceEdit -> WorkspaceEditMap -> Uri
          let path = 
                fromMaybe "Invalid arguments from typesignature.add" $
                  params 
                    ^? LSP.params 
                     . LSP.arguments
                     . _Just
                     . to (\(LSP.List a) -> a)
                     . _head
                     . Aeson._JSON @Aeson.Value @LSP.WorkspaceEdit
                     . LSP.changes
                     . _Just
                     . to HM.keys
                     . _head
                     . uriFilePathPrism
           in ForwardRequest params $ Single path
        cmd -> showFatal $ "Unknown execute command: " <> cmd

    LSP.SWindowWorkDoneProgressCancel -> handleElsewhere "WindowWorkDoneProgressCancel"
    LSP.SCancelRequest -> ForwardNotification params AllNotification
    -- Unsupported by GHCIDE:
    LSP.SWorkspaceSymbol -> unsupported "WorkspaceSymbol"
    LSP.STextDocumentTypeDefinition -> unsupported "TextDocumentTypeDefinition"
    LSP.STextDocumentImplementation -> unsupported "TextDocumentImplementation"
    LSP.STextDocumentReferences -> unsupported "TextDocumentReferences"
    LSP.STextDocumentDocumentHighlight -> unsupported "TextDocumentDocumentHighlight"
    LSP.STextDocumentDocumentColor -> unsupported "TextDocumentDocumentColor"
    LSP.SDocumentLinkResolve -> unsupported "DocumentLinkResolve"
    LSP.STextDocumentFormatting -> unsupported "TextDocumentFormatting"
    LSP.STextDocumentRangeFormatting -> unsupported "TextDocumentRangeFormatting"
    LSP.STextDocumentRename -> unsupported "TextDocumentRename"
    LSP.STextDocumentPrepareRename -> unsupported "TextDocumentPrepareRename"
    LSP.STextDocumentFoldingRange -> unsupported "TextDocumentFoldingRange"
    LSP.STextDocumentSelectionRange -> unsupported "TextDocumentSelectionRange"
    LSP.STextDocumentPrepareCallHierarchy -> unsupported "TextDocumentPrepareCallHierarchy"
    LSP.SCompletionItemResolve -> unsupported "CompletionItemResolve"
    LSP.SCodeLensResolve -> unsupported "CodeLensResolve"
    LSP.SCallHierarchyIncomingCalls -> unsupported "CallHierarchyIncomingCalls"
    LSP.SCallHierarchyOutgoingCalls -> unsupported "CallHierarchyOutgoingCalls"
    LSP.STextDocumentSemanticTokens -> unsupported "TextDocumentSemanticTokens"
    LSP.STextDocumentSemanticTokensFull -> unsupported "TextDocumentSemanticTokensFull"
    LSP.STextDocumentSemanticTokensFullDelta -> unsupported "TextDocumentSemanticTokensFullDelta"
    LSP.STextDocumentSemanticTokensRange -> unsupported "TextDocumentSemanticTokensRange"
    LSP.SWorkspaceSemanticTokensRefresh -> unsupported "WorkspaceSemanticTokensRefresh"

filePathFromParamsWithTextDocument :: (LSP.HasParams p a, LSP.HasTextDocument a t, LSP.HasUri t LSP.Uri) => MultiIdeState -> p -> FilePath
filePathFromParamsWithTextDocument miState params =
  let uri = params ^. LSP.params . LSP.textDocument . LSP.uri
   in fromMaybe (error $ "Failed to extract path: " <> show uri) $ filePathFromURI miState uri

forwardingBehaviourFromParamsWithTextDocument :: (LSP.HasParams p a, LSP.HasTextDocument a t, LSP.HasUri t LSP.Uri) => MultiIdeState -> p -> ForwardingBehaviour m
forwardingBehaviourFromParamsWithTextDocument miState params = Single $ filePathFromParamsWithTextDocument miState params

-- Attempts to convert the URI directly to a filepath
-- If the URI is a virtual resource, we instead parse it as such and extract the file from that
filePathFromURI :: MultiIdeState -> LSP.Uri -> Maybe FilePath
filePathFromURI miState uri = 
  LSP.uriToFilePath uri
    <|> do
      parsedUri <- URI.parseURI $ T.unpack $ LSP.getUri uri
      case URI.uriScheme parsedUri of
        "daml:" -> do
          vr <- uriToVirtualResource parsedUri
          pure $ LSP.fromNormalizedFilePath $ vrScenarioFile vr
        "untitled:" ->
          pure $ unPackageHome $ misDefaultPackagePath miState
        _ -> Nothing
