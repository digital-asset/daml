-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.LanguageServer.Protocol
    ( module Language.Haskell.LSP.Types
    , ServerRequest(..)
    , ServerNotification(..)
    , ClientNotification(..)
    , serverErrorToErrorObj
    , ServerCapabilities(..)
    , InitializeResult(..)
    , prettyPosition
    ) where

import qualified Data.Aeson       as Aeson
import qualified Data.Text        as T

import           DA.LanguageServer.TH (deriveJSON)
import qualified DA.Service.JsonRpc  as JsonRpc
import qualified DA.Pretty           as P

import Language.Haskell.LSP.Types hiding
    ( CodeLens
    , DocumentSymbol
    , Hover
    , Initialize
    , Shutdown
    , SignatureHelp
    , WorkspaceSymbol
    )

-- | Server capabilities
data ServerCapabilities = ServerCapabilities
    { scTextDocumentSync                 :: !(Maybe TextDocumentSyncKind)
    , scHoverProvider                    :: !Bool
    , scCompletionProvider               :: !(Maybe CompletionOptions)
    , scSignatureHelpProvider            :: !(Maybe SignatureHelpOptions)
    , scDefinitionProvider               :: !Bool
    , scReferencesProvider               :: !Bool
    , scDocumentHighlightProvider        :: !Bool
    , scDocumentSymbolProvider           :: !Bool
    , scWorkspaceSymbolProvider          :: !Bool
    , scCodeActionProvider               :: !Bool
    , scCodeLensProvider                 :: !Bool
    , scDocumentFormattingProvider       :: !Bool
    , scDocumentRangeFormattingProvider  :: !Bool
    , scDocumentOnTypeFormattingProvider :: !Bool
    , scRenameProvider                   :: !Bool
    }

-- | Request sent by the client to the server.
data ServerRequest
    = Initialize      !InitializeParams
    | Shutdown
    | KeepAlive
    | Completion      !TextDocumentPositionParams
    | SignatureHelp   !TextDocumentPositionParams
    | Hover           !TextDocumentPositionParams
    | Definition      !TextDocumentPositionParams
    | References      !ReferenceParams
    | CodeLens        !CodeLensParams
    | Rename          !RenameParams
    | DocumentSymbol  !DocumentSymbolParams
    | WorkspaceSymbol !WorkspaceSymbolParams
    | Formatting      !DocumentFormattingParams
    | UnknownRequest  !T.Text !Aeson.Value

serverErrorToErrorObj :: ErrorCode -> JsonRpc.ErrorObj
serverErrorToErrorObj = \case
    ParseError     -> JsonRpc.ErrorObj "Parse error"        (-32700) Aeson.Null
    InvalidRequest -> JsonRpc.ErrorObj "Invalid request"    (-32600) Aeson.Null
    MethodNotFound -> JsonRpc.ErrorObj "Method not found"   (-32601) Aeson.Null
    InvalidParams  -> JsonRpc.ErrorObj "Invalid parameters" (-32602) Aeson.Null
    InternalError  -> JsonRpc.ErrorObj "Internal error"     (-32603) Aeson.Null
    ServerErrorStart -> JsonRpc.ErrorObj "Server error start" (-32099) Aeson.Null
    ServerErrorEnd -> JsonRpc.ErrorObj "Server error end" (-32000) Aeson.Null
    ServerNotInitialized -> JsonRpc.ErrorObj "Server not initialized" (-32002) Aeson.Null
    UnknownErrorCode -> JsonRpc.ErrorObj "Unknown error code" (-32001) Aeson.Null
    RequestCancelled -> JsonRpc.ErrorObj "Request cancelled" (-32800) Aeson.Null

data ServerNotification
    = DidOpenTextDocument   DidOpenTextDocumentParams
    | DidChangeTextDocument DidChangeTextDocumentParams
    | DidCloseTextDocument  DidCloseTextDocumentParams
    | DidSaveTextDocument   DidSaveTextDocumentParams
    | UnknownNotification   T.Text Aeson.Value

instance JsonRpc.FromRequest ServerRequest where
    parseParams = \case
        "initialize"                         -> parseTo Initialize
        "shutdown"                           -> Just $ const $ return Shutdown
        "textDocument/completion"            -> parseTo Completion
        "textDocument/hover"                 -> parseTo Hover
        "textDocument/signatureHelp"         -> parseTo SignatureHelp
        "textDocument/definition"            -> parseTo Definition
        "textDocument/references"            -> parseTo References
        "textDocument/codeLens"              -> parseTo CodeLens
        "textDocument/rename"                -> parseTo Rename
        "textDocument/formatting"            -> parseTo Formatting
        "textDocument/documentSymbol"        -> parseTo DocumentSymbol
        "workspace/symbol"                   -> parseTo WorkspaceSymbol
        "daml/keepAlive"                     -> Just $ const $ return KeepAlive
        method                               -> Just $ return . UnknownRequest method
      where
        parseTo ctor = Just $ fmap ctor . Aeson.parseJSON

instance JsonRpc.FromRequest ServerNotification where
    parseParams = \case
        "textDocument/didOpen"    -> parseTo DidOpenTextDocument
        "textDocument/didChange"  -> parseTo DidChangeTextDocument
        "textDocument/didClose"   -> parseTo DidCloseTextDocument
        "textDocument/didSave"    -> parseTo DidSaveTextDocument
        method                    -> Just $ return . UnknownNotification method
      where
        parseTo ctor = Just $ fmap ctor . Aeson.parseJSON

-- | Initialization result sent to the client.
data InitializeResult = InitializeResult
    { irCapabilities :: !ServerCapabilities
      -- ^ The provided language server capabilities.
      --
    }
-- | Notification sent by the language server to the client.
data ClientNotification
    = ShowMessage LogMessageParams
    | LogMessage LogMessageParams
    | SendTelemetry Aeson.Value
    | PublishDiagnostics PublishDiagnosticsParams
    | CustomNotification T.Text Aeson.Value

instance JsonRpc.ToRequest ClientNotification where
    requestMethod (ShowMessage _)         = "window/showMessage"
    requestMethod (LogMessage _)          = "window/logMessage"
    requestMethod (SendTelemetry _)       = "telemetry/event"
    requestMethod (PublishDiagnostics _)  = "textDocument/publishDiagnostics"
    requestMethod (CustomNotification method _) = method
    requestIsNotif = const True

instance Aeson.ToJSON ClientNotification where
    toJSON req = case req of
      ShowMessage params        -> Aeson.toJSON params
      LogMessage params         -> Aeson.toJSON params
      SendTelemetry value       -> value
      PublishDiagnostics params -> Aeson.toJSON params
      CustomNotification _ value -> value

-------------------------------------------------------------------------
-- Instances
-------------------------------------------------------------------------

fmap concat $ sequenceA
    [ deriveJSON ''InitializeResult
    , deriveJSON ''ServerCapabilities
    , deriveJSON ''ServerRequest
    ]

----------------------------------------------------------------------------------------------------
-- Pretty printing
----------------------------------------------------------------------------------------------------

prettyPosition :: Position -> P.Doc a
prettyPosition Position{..} = P.int (_line + 1) <> P.colon <> P.int (_character + 1)
