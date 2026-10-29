-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE AllowAmbiguousTypes #-}

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
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import Development.IDE.Core.Rules.Daml (uriToVirtualResource)
import Development.IDE.Core.RuleTypes.Daml (VirtualResource (..))
import qualified Language.LSP.Protocol.Types as LSP
import qualified Language.LSP.Protocol.Message as LSP
import qualified Language.LSP.Protocol.Lens as LSP
import qualified Language.LSP.Types as OldLSP
import GHC.TypeLits (symbolVal)
import qualified Network.URI as URI
import DA.Cli.Damlc.Command.MultiIde.Types

{-# ANN module ("HLint: ignore Avoid restricted flags" :: String) #-}

pullMonadThroughTuple :: Monad m => (a, m b) -> m (a, b)
pullMonadThroughTuple (a, mb) = (a,) <$> mb

-- Takes a natural transformation of responses and lifts it to forward the first error
assumeSuccessCombiner
  :: forall (m :: LSP.Method 'LSP.ClientToServer 'LSP.Request)
  .  ([(PackageHome, LSP.MessageResult m)] -> LSP.MessageResult m)
  -> ResponseCombiner m
assumeSuccessCombiner f res = f <$> mapM pullMonadThroughTuple res

ignore :: Forwarding m
ignore = ExplicitHandler $ \_ _ -> pure ()

showError :: T.Text -> Forwarding m
showError err = ExplicitHandler $ \sendClient _ ->
  sendClient $ LSP.FromServerMess LSP.SMethod_WindowShowMessage
    $ LSP.TNotificationMessage "2.0" LSP.SMethod_WindowShowMessage
      $ LSP.ShowMessageParams LSP.MessageType_Error err

showFatal :: T.Text -> Forwarding m
showFatal err = showError $ "FATAL ERROR:\n" <> err <> "\nPlease report this on the daml forums."

handleElsewhere :: T.Text -> Forwarding m
handleElsewhere name = showFatal $ "Got unexpected " <> name <> " message in forwarding handler, this message should have been handled elsewhere."

unsupported :: T.Text -> Forwarding m
unsupported name = showFatal $ "Attempted to call a method that is unsupported by the underlying IDEs: " <> name

uriFilePathPrism :: Prism' LSP.Uri FilePath
uriFilePathPrism = prism' LSP.filePathToUri LSP.uriToFilePath

getMessageForwardingBehaviour
  :: forall t (m :: LSP.Method 'LSP.ClientToServer t)
  .  MultiIdeState
  -> LSP.SMethod m
  -> LSP.TMessage m
  -> Forwarding m
getMessageForwardingBehaviour miState meth params =
  case meth of
    LSP.SMethod_Initialize -> handleElsewhere "Initialize"
    LSP.SMethod_Initialized -> ignore
    -- send to all then const reply
    LSP.SMethod_Shutdown -> ForwardRequest params $ AllRequest (assumeSuccessCombiner @m $ const LSP.Null)
    LSP.SMethod_Exit -> handleElsewhere "Exit"
    LSP.SMethod_WorkspaceDidChangeWorkspaceFolders -> ForwardNotification params AllNotification
    LSP.SMethod_WorkspaceDidChangeConfiguration -> ForwardNotification params AllNotification
    LSP.SMethod_WorkspaceDidChangeWatchedFiles -> ForwardNotification params AllNotification
    LSP.SMethod_TextDocumentDidOpen -> ForwardNotification params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.SMethod_TextDocumentDidChange -> ForwardNotification params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.SMethod_TextDocumentWillSave -> ForwardNotification params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.SMethod_TextDocumentWillSaveWaitUntil -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.SMethod_TextDocumentDidSave -> ForwardNotification params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.SMethod_TextDocumentDidClose -> ForwardNotification params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.SMethod_TextDocumentCompletion -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.SMethod_TextDocumentHover -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.SMethod_TextDocumentSignatureHelp -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.SMethod_TextDocumentDeclaration -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.SMethod_TextDocumentDefinition -> handleElsewhere "TextDocumentDefinition"
    LSP.SMethod_TextDocumentDocumentSymbol -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.SMethod_TextDocumentCodeAction -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.SMethod_TextDocumentCodeLens -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.SMethod_TextDocumentDocumentLink -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.SMethod_TextDocumentColorPresentation -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    LSP.SMethod_TextDocumentOnTypeFormatting -> ForwardRequest params $ forwardingBehaviourFromParamsWithTextDocument miState params
    
    LSP.SMethod_CustomMethod (symbolVal -> "daml/keepAlive") ->
      case params of
        LSP.ReqMess LSP.TRequestMessage {_id, _method, _params} -> ExplicitHandler $ \sendClient _ ->
          sendClient $ LSP.FromServerRsp _method $ LSP.TResponseMessage "2.0" (Just _id) (Right Aeson.Null)
        _ -> showFatal "Got unpexpected daml/keepAlive response type from client"

    -- Other custom messages are notifications from server
    LSP.SMethod_CustomMethod _ -> ignore

    -- We only add the typesignature.add command, which simply sends a WorkspaceEdit with a single file modification
    -- We can take the file path from that modification
    LSP.SMethod_WorkspaceExecuteCommand ->
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
                     . _head
                     . Aeson._JSON @Aeson.Value @LSP.WorkspaceEdit
                     . LSP.changes
                     . _Just
                     . to Map.keys
                     . _head
                     . uriFilePathPrism
           in ForwardRequest params $ Single path
        cmd -> showFatal $ "Unknown execute command: " <> cmd

    LSP.SMethod_WindowWorkDoneProgressCancel -> handleElsewhere "WindowWorkDoneProgressCancel"
    LSP.SMethod_CancelRequest -> ForwardNotification params AllNotification
    LSP.SMethod_Progress -> ignore
    LSP.SMethod_SetTrace -> ignore
    LSP.SMethod_WorkspaceDidCreateFiles -> ignore
    LSP.SMethod_WorkspaceDidRenameFiles -> ignore
    LSP.SMethod_WorkspaceDidDeleteFiles -> ignore
    LSP.SMethod_NotebookDocumentDidOpen -> ignore
    LSP.SMethod_NotebookDocumentDidChange -> ignore
    LSP.SMethod_NotebookDocumentDidSave -> ignore
    LSP.SMethod_NotebookDocumentDidClose -> ignore
    -- Unsupported by GHCIDE:
    LSP.SMethod_WorkspaceSymbol -> unsupported "WorkspaceSymbol"
    LSP.SMethod_TextDocumentTypeDefinition -> unsupported "TextDocumentTypeDefinition"
    LSP.SMethod_TextDocumentImplementation -> unsupported "TextDocumentImplementation"
    LSP.SMethod_TextDocumentReferences -> unsupported "TextDocumentReferences"
    LSP.SMethod_TextDocumentDocumentHighlight -> unsupported "TextDocumentDocumentHighlight"
    LSP.SMethod_TextDocumentDocumentColor -> unsupported "TextDocumentDocumentColor"
    LSP.SMethod_DocumentLinkResolve -> unsupported "DocumentLinkResolve"
    LSP.SMethod_TextDocumentFormatting -> unsupported "TextDocumentFormatting"
    LSP.SMethod_TextDocumentRangeFormatting -> unsupported "TextDocumentRangeFormatting"
    LSP.SMethod_TextDocumentRename -> unsupported "TextDocumentRename"
    LSP.SMethod_TextDocumentPrepareRename -> unsupported "TextDocumentPrepareRename"
    LSP.SMethod_TextDocumentFoldingRange -> unsupported "TextDocumentFoldingRange"
    LSP.SMethod_TextDocumentSelectionRange -> unsupported "TextDocumentSelectionRange"
    LSP.SMethod_TextDocumentPrepareCallHierarchy -> unsupported "TextDocumentPrepareCallHierarchy"
    LSP.SMethod_CompletionItemResolve -> unsupported "CompletionItemResolve"
    LSP.SMethod_CodeLensResolve -> unsupported "CodeLensResolve"
    LSP.SMethod_CallHierarchyIncomingCalls -> unsupported "CallHierarchyIncomingCalls"
    LSP.SMethod_CallHierarchyOutgoingCalls -> unsupported "CallHierarchyOutgoingCalls"
    LSP.SMethod_TextDocumentSemanticTokensFull -> unsupported "TextDocumentSemanticTokensFull"
    LSP.SMethod_TextDocumentSemanticTokensFullDelta -> unsupported "TextDocumentSemanticTokensFullDelta"
    LSP.SMethod_TextDocumentSemanticTokensRange -> unsupported "TextDocumentSemanticTokensRange"
    LSP.SMethod_TextDocumentLinkedEditingRange -> unsupported "TextDocumentLinkedEditingRange"
    LSP.SMethod_WorkspaceWillCreateFiles -> unsupported "WorkspaceWillCreateFiles"
    LSP.SMethod_WorkspaceWillRenameFiles -> unsupported "WorkspaceWillRenameFiles"
    LSP.SMethod_WorkspaceWillDeleteFiles -> unsupported "WorkspaceWillDeleteFiles"
    LSP.SMethod_TextDocumentMoniker -> unsupported "TextDocumentMoniker"
    LSP.SMethod_TextDocumentPrepareTypeHierarchy -> unsupported "TextDocumentPrepareTypeHierarchy"
    LSP.SMethod_TypeHierarchySupertypes -> unsupported "TypeHierarchySupertypes"
    LSP.SMethod_TypeHierarchySubtypes -> unsupported "TypeHierarchySubtypes"
    LSP.SMethod_TextDocumentInlineValue -> unsupported "TextDocumentInlineValue"
    LSP.SMethod_TextDocumentInlayHint -> unsupported "TextDocumentInlayHint"
    LSP.SMethod_InlayHintResolve -> unsupported "InlayHintResolve"
    LSP.SMethod_TextDocumentDiagnostic -> unsupported "TextDocumentDiagnostic"
    LSP.SMethod_WorkspaceDiagnostic -> unsupported "WorkspaceDiagnostic"
    LSP.SMethod_CodeActionResolve -> unsupported "CodeActionResolve"
    LSP.SMethod_WorkspaceSymbolResolve -> unsupported "WorkspaceSymbolResolve"

filePathFromParamsWithTextDocument :: (LSP.HasParams p a, LSP.HasTextDocument a t, LSP.HasUri t LSP.Uri) => MultiIdeState -> p -> Maybe FilePath
filePathFromParamsWithTextDocument miState params =
  let uri = params ^. LSP.params . LSP.textDocument . LSP.uri
   in filePathFromURI miState uri

forwardingBehaviourFromParamsWithTextDocument :: (LSP.HasParams p a, LSP.HasTextDocument a t, LSP.HasUri t LSP.Uri) => MultiIdeState -> p -> ForwardingBehaviour m
forwardingBehaviourFromParamsWithTextDocument miState params = maybe CannotForwardRequest Single $ filePathFromParamsWithTextDocument miState params

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
          pure $ OldLSP.fromNormalizedFilePath $ vrScriptFile vr
        "untitled:" ->
          pure $ unPackageHome $ misDefaultPackagePath miState
        _ -> Nothing
