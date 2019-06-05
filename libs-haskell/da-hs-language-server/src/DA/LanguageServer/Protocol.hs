-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.LanguageServer.Protocol
    ( module Language.Haskell.LSP.Types
    , ServerRequest(..)
    , ServerNotification(..)
    , ClientNotification(..)
    , prettyPosition
    ) where

import qualified Data.Aeson       as Aeson
import qualified Data.Text        as T

import qualified DA.Pretty           as P

import Language.Haskell.LSP.Types hiding
    ( CodeLens
    , DocumentSymbol
    , Hover
    , Shutdown
    , SignatureHelp
    , WorkspaceSymbol
    )

-- | Request sent by the client to the server.
data ServerRequest
    = Shutdown
    | KeepAlive
    | Completion      !CompletionParams
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
    deriving Show

data ServerNotification
    = DidOpenTextDocument   DidOpenTextDocumentParams
    | DidChangeTextDocument DidChangeTextDocumentParams
    | DidCloseTextDocument  DidCloseTextDocumentParams
    | DidSaveTextDocument   DidSaveTextDocumentParams
    | UnknownNotification   T.Text Aeson.Value

-- | Notification sent by the language server to the client.
data ClientNotification
    = ShowMessage ShowMessageParams
    | LogMessage LogMessageParams
    | SendTelemetry Aeson.Value
    | PublishDiagnostics PublishDiagnosticsParams
    | CustomNotification T.Text Aeson.Value

----------------------------------------------------------------------------------------------------
-- Pretty printing
----------------------------------------------------------------------------------------------------

prettyPosition :: Position -> P.Doc a
prettyPosition Position{..} = P.int (_line + 1) <> P.colon <> P.int (_character + 1)
