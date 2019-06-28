-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DuplicateRecordFields #-}

-- | Go to the definition of a variable.
module Development.IDE.LSP.CodeAction
    ( setHandlersCodeAction
    ) where

import           Language.Haskell.LSP.Types

import Development.IDE.Core.Rules
import Development.IDE.LSP.Server
import qualified Data.HashMap.Strict as Map
import qualified Language.Haskell.LSP.Core as LSP
import Language.Haskell.LSP.VFS
import Language.Haskell.LSP.Messages
import qualified Data.Rope.UTF16 as Rope

import qualified Data.Text as T

-- | Generate code actions.
codeAction
    :: LSP.LspFuncs ()
    -> IdeState
    -> CodeActionParams
    -> IO (List CAResult)
codeAction lsp _ CodeActionParams{_textDocument=TextDocumentIdentifier uri,_context=CodeActionContext{_diagnostics=List xs}} = do
    -- disable logging as its quite verbose
    -- logInfo (ideLogger ide) $ T.pack $ "Code action req: " ++ show arg
    contents <- LSP.getVirtualFileFunc lsp $ toNormalizedUri uri
    let text = Rope.toText . (_text :: VirtualFile -> Rope.Rope) <$> contents
    pure $ List
        [ CACodeAction $ CodeAction title (Just CodeActionQuickFix) (Just $ List [x]) (Just edit) Nothing
        | x <- xs, (title, tedit) <- suggestAction text x
        , let edit = WorkspaceEdit (Just $ Map.singleton uri $ List tedit) Nothing
        ]


suggestAction :: Maybe T.Text -> Diagnostic -> [(T.Text, [TextEdit])]
suggestAction _ Diagnostic{..}
-- File.hs:16:1: warning:
--     The import of `Data.List' is redundant
--       except perhaps to import instances from `Data.List'
--     To import instances alone, use: import Data.List()
    | "The import of " `T.isInfixOf` _message
    , " is redundant" `T.isInfixOf` _message
    = [("Remove import", [TextEdit _range ""])]
suggestAction _ _ = []

setHandlersCodeAction :: PartialHandlers
setHandlersCodeAction = PartialHandlers $ \WithMessage{..} x -> return x{
    LSP.codeActionHandler = withResponse RspCodeAction codeAction
    }
