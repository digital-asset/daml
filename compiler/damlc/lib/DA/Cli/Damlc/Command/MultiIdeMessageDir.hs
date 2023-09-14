-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GADTs #-}

-- We generate missing instances for SignatureHelpParams
{-# OPTIONS_GHC -fno-warn-orphans #-}

module DA.Cli.Damlc.Command.MultiIdeMessageDir (
  getMessageForwardingBehaviour,
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

{-# ANN module "HLint: ignore Avoid restricted flags" #-}

-- SignatureHelpParams has no lenses from Language.LSP.Types.Lens
-- We just need this one, so we'll write it ourselves
makeLensesFor [("_textDocument", "signatureHelpParamsTextDocumentLens")] ''LSP.SignatureHelpParams
instance LSP.HasTextDocument LSP.SignatureHelpParams LSP.TextDocumentIdentifier where
  textDocument = signatureHelpParamsTextDocumentLens

type ResponseCombiner (m :: LSP.Method 'LSP.FromClient 'LSP.Request) =
  [(FilePath, Either LSP.ResponseError (LSP.ResponseResult m))] -> Either LSP.ResponseError (LSP.ResponseResult m)

pullMonadThroughTuple :: Monad m => (a, m b) -> m (a, b)
pullMonadThroughTuple (a, mb) = (a,) <$> mb

-- Takes a natural transformation of responses and lifts it to forward the first error
assumeSuccessCombiner
  :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
  .  ([(FilePath, LSP.ResponseResult m)] -> LSP.ResponseResult m)
  -> ResponseCombiner m
assumeSuccessCombiner f res = f <$> sequence (pullMonadThroughTuple <$> res)

{-
Types of behaviour we want:

Regularly handling by a single IDE - works for requests and notifications
  e.g. TextDocumentDidOpen
Ignore it
  e.g. Initialize
Forward a notification to all IDEs
  e.g. workspace folders changed, exit
Forward a request to all IDEs and somehow combine the result
  e.g.
    symbol lookup -> combine monoidically
    shutdown -> response is empty, so identity after all responses
  This is the hard one as we need some way to define the combination logic
    which will ideally wait for all IDEs to reply to the request and apply this function over the (possibly failing) result
  This mostly covers FromClient requests that we can't pull a filepath from

  Previously thought we would need this more, now we only really use it for shutdown - ensuring all SubIdes shutdown before replying.
  We'll keep it in though since we'll likely get more capabilities supported when we upgrade ghc/move to HLS
-}

-- TODO: Consider splitting this into one data type for request and one for notification
-- rather than reusing the Single constructor over both and restricting via types
data ForwardingBehaviour (m :: LSP.Method 'LSP.FromClient t) where
  Single
    :: forall t (m :: LSP.Method 'LSP.FromClient t)
    .  FilePath
    -> ForwardingBehaviour m
  AllRequest
    :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
    .  ResponseCombiner m
    -> ForwardingBehaviour m
  AllNotification
    :: ForwardingBehaviour (m :: LSP.Method 'LSP.FromClient 'LSP.Notification)

-- Akin to ClientNotOrReq tagged with ForwardingBehaviour, and CustomMethod realised to req/not
data Forwarding (m :: LSP.Method 'LSP.FromClient t) where
  ForwardRequest
    :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Request)
    .  LSP.RequestMessage m
    -> ForwardingBehaviour m
    -> Forwarding m
  ForwardNotification
    :: forall (m :: LSP.Method 'LSP.FromClient 'LSP.Notification)
    .  LSP.NotificationMessage m
    -> ForwardingBehaviour m
    -> Forwarding m
  ExplicitHandler 
    :: (  (LSP.FromServerMessage -> IO ())
       -> (FilePath -> LSP.FromClientMessage -> IO ())
       -> IO ()
       )
    -> Forwarding (m :: LSP.Method 'LSP.FromClient t)

ignore :: Forwarding m
ignore = ExplicitHandler $ \_ _ -> pure ()

unsupported :: String -> Forwarding m
unsupported name = ExplicitHandler $ \_ _ -> error $ "Attempted to call a method that is unsupported by the underlying IDEs: " <> name

uriFilePathPrism :: Prism' LSP.Uri FilePath
uriFilePathPrism = prism' LSP.filePathToUri LSP.uriToFilePath

getMessageForwardingBehaviour
  :: forall t (m :: LSP.Method 'LSP.FromClient t)
  .  LSP.SMethod m
  -> LSP.Message m
  -> Forwarding m
getMessageForwardingBehaviour meth params =
  case meth of
    LSP.SInitialize -> error "Forwarding of Initialize must be handled elsewhere."
    LSP.SInitialized -> ignore
    -- send to all then const reply
    LSP.SShutdown -> ForwardRequest params $ AllRequest (assumeSuccessCombiner @m $ const LSP.Empty)
    LSP.SExit -> ForwardNotification params AllNotification
    LSP.SWorkspaceDidChangeWorkspaceFolders -> ForwardNotification params AllNotification
    LSP.SWorkspaceDidChangeConfiguration -> ForwardNotification params AllNotification
    LSP.SWorkspaceDidChangeWatchedFiles -> ForwardNotification params AllNotification
    LSP.STextDocumentDidOpen -> ForwardNotification params $ fromParamsWithTextDocument params
    LSP.STextDocumentDidChange -> ForwardNotification params $ fromParamsWithTextDocument params
    LSP.STextDocumentWillSave -> ForwardNotification params $ fromParamsWithTextDocument params
    LSP.STextDocumentWillSaveWaitUntil -> ForwardRequest params $ fromParamsWithTextDocument params
    LSP.STextDocumentDidSave -> ForwardNotification params $ fromParamsWithTextDocument params
    LSP.STextDocumentDidClose -> ForwardNotification params $ fromParamsWithTextDocument params
    LSP.STextDocumentCompletion -> ForwardRequest params $ fromParamsWithTextDocument params
    LSP.STextDocumentHover -> ForwardRequest params $ fromParamsWithTextDocument params
    LSP.STextDocumentSignatureHelp -> ForwardRequest params $ fromParamsWithTextDocument params
    LSP.STextDocumentDeclaration -> ForwardRequest params $ fromParamsWithTextDocument params
    LSP.STextDocumentDefinition -> ForwardRequest params $ fromParamsWithTextDocument params
    LSP.STextDocumentDocumentSymbol -> ForwardRequest params $ fromParamsWithTextDocument params
    LSP.STextDocumentCodeAction -> ForwardRequest params $ fromParamsWithTextDocument params
    LSP.STextDocumentCodeLens -> ForwardRequest params $ fromParamsWithTextDocument params
    LSP.STextDocumentDocumentLink -> ForwardRequest params $ fromParamsWithTextDocument params
    LSP.STextDocumentColorPresentation -> ForwardRequest params $ fromParamsWithTextDocument params
    LSP.STextDocumentOnTypeFormatting -> ForwardRequest params $ fromParamsWithTextDocument params
    
    LSP.SCustomMethod "daml/keepAlive" ->
      case params of
        LSP.ReqMess LSP.RequestMessage {_id, _method, _params} -> ExplicitHandler $ \sendClient _ ->
          sendClient $ LSP.FromServerRsp _method $ LSP.ResponseMessage "2.0" (Just _id) (Right Aeson.Null)
        _ -> error "Got unpexpected daml/keepAlive response from client"

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
        cmd -> error $ "Unknown execute command: " <> show cmd

    LSP.SWindowWorkDoneProgressCancel -> ForwardNotification params AllNotification
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

fromParamsWithTextDocument :: (LSP.HasParams p a, LSP.HasTextDocument a t, LSP.HasUri t LSP.Uri) => p -> ForwardingBehaviour m
fromParamsWithTextDocument params =
  let uri = params ^. LSP.params . LSP.textDocument . LSP.uri
   in Single $ fromMaybe (error $ "Failed to extract path: " <> show uri) $ filePathFromURI uri

filePathFromURI :: LSP.Uri -> Maybe FilePath
filePathFromURI uri = 
  LSP.uriToFilePath uri
    <|> do
      parsedUri <- URI.parseURI $ T.unpack $ LSP.getUri uri
      vr <- uriToVirtualResource parsedUri
      pure $ LSP.fromNormalizedFilePath $ vrScenarioFile vr
