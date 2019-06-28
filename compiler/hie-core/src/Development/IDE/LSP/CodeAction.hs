-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DuplicateRecordFields #-}

-- | Go to the definition of a variable.
module Development.IDE.LSP.CodeAction
    ( setHandlersCodeAction
    ) where

import           Language.Haskell.LSP.Types

import Development.IDE.Types.Logger
import Development.IDE.Core.Rules
import Development.IDE.Core.Service
import Development.IDE.LSP.Server
import qualified Data.HashMap.Strict as Map
import qualified Language.Haskell.LSP.Core as LSP
import qualified Language.Haskell.LSP.Types as LSP
import Language.Haskell.LSP.Messages

import qualified Data.Text as T

-- | Generate code actions.
codeAction
    :: IdeState
    -> CodeActionParams
    -> IO (List CAResult)
codeAction ide arg@(CodeActionParams{_textDocument=TextDocumentIdentifier uri,_context=CodeActionContext{_diagnostics=List xs}}) = do
    logWarning (ideLogger ide) $ T.pack $ "Code action: " ++ show arg
    pure $ List
        [ CACodeAction $ CodeAction title (Just CodeActionQuickFix) (Just $ List [x]) (Just edit) Nothing
        | x <- xs, (title, edit) <- suggestAction uri x]


suggestAction :: Uri -> Diagnostic -> [(T.Text, LSP.WorkspaceEdit)]
suggestAction uri Diagnostic{..}
    | "The import of " `T.isInfixOf` _message
    , " is redundant" `T.isInfixOf` _message
    = [("Remove import", WorkspaceEdit (Just $ Map.singleton uri $ List [TextEdit _range ""]) Nothing)]


setHandlersCodeAction :: PartialHandlers
setHandlersCodeAction = PartialHandlers $ \WithMessage{..} x -> return x{
    LSP.codeActionHandler = withResponse RspCodeAction codeAction
    }
