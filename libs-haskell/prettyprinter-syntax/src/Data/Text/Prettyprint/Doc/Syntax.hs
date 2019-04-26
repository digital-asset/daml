-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Data.Text.Prettyprint.Doc.Syntax
    ( module Data.Text.Prettyprint.Doc
    , SyntaxClass(..)
    , label_
    , comment_
    , reflow
    , renderPlain
    , renderColored
    ) where

import Data.Text.Prettyprint.Doc
import Data.Text.Prettyprint.Doc.Render.Text
import qualified Data.Text.Prettyprint.Doc.Render.Terminal as Terminal
import Data.Text.Prettyprint.Doc.Render.Terminal (Color(..), color, colorDull)
import Data.Text.Prettyprint.Doc.Util
import qualified Data.Text as T

-- | Classes of syntax elements, which are used for highlighting.
data SyntaxClass
    = NoAnnotationSC
      -- ^ Annotation to use as a no-op for highlighting.
    | OperatorSC
    | KeywordSC
    | ParensSC
    | PredicateSC
    | ConstructorSC
    | CommentSC
    | ProofStepSC
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

-- | Label a document.
label_ :: String -> Doc a -> Doc a
label_ t d = nest 2 $ sep [pretty t, d]

comment_ :: Doc SyntaxClass -> Doc SyntaxClass
comment_ = annotate CommentSC

-- | The layout options used for the SDK assistant.
cliLayout ::
       Int
    -- ^ Rendering width of the pretty printer.
    -> LayoutOptions
cliLayout renderWidth = LayoutOptions
    { layoutPageWidth = AvailablePerLine renderWidth 0.9
    }

-- | Render without any syntax annotations
renderPlain :: Doc ann -> T.Text
renderPlain = renderStrict . layoutSmart (cliLayout defaultTermWidth)

-- | Render a 'Document' as an ANSII colored string.
renderColored :: Doc SyntaxClass -> T.Text
renderColored =
    Terminal.renderStrict .
    layoutSmart defaultLayoutOptions { layoutPageWidth = AvailablePerLine 100 1.0 } .
    fmap toAnsiStyle
  where
    toAnsiStyle ann = case ann of
        OperatorSC -> colorDull Red
        KeywordSC -> colorDull Green
        ParensSC -> colorDull Yellow
        CommentSC -> colorDull White
        PredicateSC -> colorDull Magenta
        ConstructorSC -> color Blue
        ProofStepSC -> colorDull Blue
        TypeSC -> color Green
        ErrorSC -> color Red
        WarningSC -> color Yellow
        InfoSC -> color Blue
        HintSC -> color Magenta
        LinkSC _ _ -> color Green
        NoAnnotationSC -> mempty
        IdSC _ -> mempty
        OnClickSC _ -> mempty

defaultTermWidth :: Int
defaultTermWidth = 80
