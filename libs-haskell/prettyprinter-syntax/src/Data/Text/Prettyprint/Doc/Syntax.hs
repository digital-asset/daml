-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Data.Text.Prettyprint.Doc.Syntax
    ( SyntaxClass(..)
    ) where

import qualified Data.Text as T

-- | Classes of syntax elements, which are used for highlighting.
data SyntaxClass
    = -- ^ Annotation to use as a no-op for highlighting.
      OperatorSC
    | KeywordSC
    | PredicateSC
    | ConstructorSC
    | TypeSC
    | ErrorSC
    | WarningSC
    | InfoSC
    | HintSC
    | LinkSC T.Text T.Text
    -- ^ @LinkSC url title@: Create a link to the 'url'
    | IdSC T.Text
    -- ^ @IdSC id@: Identifier for the node. For linking into.
    | OnClickSC T.Text
    deriving (Eq, Ord, Show)
