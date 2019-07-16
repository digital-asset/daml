-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings, DerivingStrategies #-}

-- | Monoid with which to render documentation.
module DA.Daml.Doc.Render.Monoid
  ( module DA.Daml.Doc.Render.Monoid
  ) where

import DA.Daml.Doc.Types
import qualified Data.Set as Set
import qualified Data.Text as T

-- | Environment in which to generate final documentation.
newtype RenderEnv = RenderEnv (Set.Set Anchor)
    deriving newtype (Semigroup, Monoid)

-- | Is the anchor available in the rendering environment? Renderers should avoid
-- generating links to anchors that don't actually exist.
--
-- One reason an anchor may be unavailable is because of a @-- | HIDE@ directive.
-- Another possibly reason is that the anchor refers to a definition in another
-- package (and at the moment it's not possible to link accross packages).
renderAnchorAvailable :: RenderEnv -> Anchor -> Bool
renderAnchorAvailable (RenderEnv anchors) anchor = Set.member anchor anchors

-- | Renderer output. This is the set of anchors that were generated, and a
-- list of output functions that depend on that set. The goal is to prevent
-- the creation of spurious anchors links (i.e. links to anchors that don't
-- exist).
--
-- (In theory this could be done in two steps, but that seems more error prone
-- than building up both steps at the same time, and combining them at the
-- end, as is done here.)
--
-- Using a newtype here so we can derive the semigroup / monoid instances we
-- want automatically. :-)
newtype RenderOut = RenderOut (RenderEnv, [RenderEnv -> [T.Text]])
    deriving newtype (Semigroup, Monoid)

renderFinish :: RenderOut -> T.Text
renderFinish (RenderOut (xs, fs)) = T.unlines (concatMap ($ xs) fs)

-- | Declare an anchor for the purposes of rendering output.
renderDeclareAnchor :: Anchor -> RenderOut
renderDeclareAnchor anchor = RenderOut (RenderEnv $ Set.singleton anchor, [])

renderLine :: T.Text -> RenderOut
renderLine l = renderLines [l]

renderLines :: [T.Text] -> RenderOut
renderLines ls = renderLinesDep (const ls)

renderLineDep :: (RenderEnv -> T.Text) -> RenderOut
renderLineDep f = renderLinesDep (pure . f)

renderLinesDep :: (RenderEnv -> [T.Text]) -> RenderOut
renderLinesDep f = RenderOut (mempty, [f])

-- | Prefix every output line by a particular text.
renderPrefix :: T.Text -> RenderOut -> RenderOut
renderPrefix p (RenderOut (env, fs)) =
    RenderOut (env, map (map (p <>) .) fs)

-- | Indent every output line by a particular amount.
renderIndent :: Int -> RenderOut -> RenderOut
renderIndent n = renderPrefix (T.replicate n " ")
