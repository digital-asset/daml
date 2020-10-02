-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | This module provides functions to perform the static party literal check
-- for DAML-LF. Party literals are neither allowed in templates nor in
-- values used in templates directly or indirectly. To perform this check we
-- need to infer for each top-level value definition if it contains party
-- literals directly or indirectly. This information is then stored in the
-- 'ModuleInterface'. To perform this inference in an incremental fashion,
-- we also provide 'augmentInterface'.
--
-- Checking whether all template definition comply with the DAML-LF party
-- literal restriction is straightfoward. It is implemented in 'checkModule',
-- which assumes that the party literal information in the 'ModuleInterface's
-- has already been inferrred. In other words, the environment must contain the
--  'ModuleInterface' produced by 'augmentInterface'.
module DA.Daml.LF.TypeChecker.PartyLiterals
  ( checkModule
  ) where

import           Control.Lens
import Control.Monad
import           Data.Foldable (for_)
import           Data.List.Extra

import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Optics (exprPartyLiteral, exprValueRef, templateExpr)
import DA.Daml.LF.TypeChecker.Env
import DA.Daml.LF.TypeChecker.Error

checkExprs :: MonadGamma m => [Expr] -> m ()
checkExprs expr = do
  let parties = toListOf (folded . exprPartyLiteral) expr
  let refs = toListOf (folded . exprValueRef) expr
  badRefs <- filterM (fmap (not . getHasNoPartyLiterals . dvalNoPartyLiterals) . inWorld . lookupValue) refs
  unless (null parties && null badRefs) $
    throwWithContext (EForbiddenPartyLiterals (nubOrd parties) (nubOrd badRefs))

checkValue :: MonadGamma m => Module -> DefValue -> m ()
checkValue mod0 val = withContext (ContextDefValue mod0 val) $ do
  when (getHasNoPartyLiterals (dvalNoPartyLiterals val)) $
    checkExprs [dvalBody val]

-- | Check that a template is free of party literals.
checkTemplate :: MonadGamma m => Module -> Template -> m ()
checkTemplate mod0 tpl = withContext (ContextTemplate mod0 tpl TPWhole) $
  checkExprs (toListOf templateExpr tpl)

-- | Check that all templates in a module are free of party literals.
checkModule :: MonadGamma m => Module -> m ()
checkModule mod0 = do
  for_ (moduleValues mod0) (checkValue mod0)
  for_ (moduleTemplates mod0) (checkTemplate mod0)
