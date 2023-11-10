-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | This module provides a function to check that a Daml-LF module does not
-- contain recursive data type definitions.
module DA.Daml.LF.TypeChecker.Recursion
  ( checkModule
  ) where

import Control.Lens (matching)
import Data.Foldable (for_, toList)
import Data.Functor.Foldable (cata)
import Data.HashSet qualified as HS
import Data.Graph qualified as G
import Data.List.Extra (nubOrd)
import Data.NameMap qualified as NM

import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Optics (_PRSelfModule)
import DA.Daml.LF.Ast.Type (referencedSyns)
import DA.Daml.LF.Ast.Recursive
import DA.Daml.LF.TypeChecker.Env
import DA.Daml.LF.TypeChecker.Error

-- | Collect all references to values defined in a given module in the current
-- package that are /not/ under a lambda.
exprRefs :: ModuleName -> Expr -> HS.HashSet ExprValName
exprRefs modName = cata go
  where
    go :: ExprF (HS.HashSet ExprValName) -> HS.HashSet ExprValName
    go = \case
      EValF qref
        | Right ref <- matching (_PRSelfModule modName) qref -> HS.singleton ref
      ETmLamF{} -> HS.empty
      e -> HS.unions (toList e)

-- | Collect all references to values defined in a given moule in the current
-- package that are /not/ under a lambda, update or scenario block.
valueRefs :: ModuleName -> DefValue -> HS.HashSet ExprValName
valueRefs modName = exprRefs modName . dvalBody

synRefs :: ModuleName -> DefTypeSyn -> [TypeSynName]
synRefs modName DefTypeSyn{synType} = do
  -- Synonym-cycles which cross modules boundaries are impossible
  -- because mutually recusive modules are impossible.  Therefore,
  -- when checking for cycles within a given module, we stop following
  -- references to synonyns defined in another module.
  [ syn | Qualified{qualPackage=PRSelf,qualModule=m,qualObject=syn} <- HS.toList $ referencedSyns synType
        , m == modName ]

-- | Check that a module contains neither recursive data type definitions nor
-- recursive value definition.
--
-- A data type definition like
--
-- > data Tpl = { previous :: Maybe (ContractId Tpl); ... }
--
-- is /not/ considered recursive because the @Tpl@ in @ContractId Tpl@ does
-- not refer to the data type @Tpl@ but to the associated template.
--
-- A value definition like
--
-- def fact (n : Integer) = if n <= 0 then 1 else n * fact (n-1)
--
-- is /not/ considered recursive because the recursive call is guarded by a
-- lambda.
checkModule :: MonadGamma m => Module -> m ()
checkModule mod0 = do
  let modName = moduleName mod0
  let values = NM.toList (moduleValues mod0)
  let synonyms = NM.toList (moduleSynonyms mod0)
  checkAcyclic EValueCycle dvalName (HS.toList . valueRefs modName) values
  checkAcyclic ETypeSynCycle synName (synRefs modName) synonyms

-- | Check whether a directed graph given by its adjacency list is acyclic. If
-- it is not, throw an error.
checkAcyclic
  :: (Ord k, MonadGamma m)
  => ([k] -> Error)  -- ^ Make an error from the names of nodes forming a cycle.
  -> (a -> k)    -- ^ Map a node to its name.
  -> (a -> [k])  -- ^ Map a node to the names of its adjacent nodes.
  -> [a]         -- ^ Nodes of the graph.
  -> m ()
checkAcyclic mkError name adjacent objs = do
  let graph = map (\obj -> (obj, name obj, nubOrd (adjacent obj))) objs
  for_ (G.stronglyConnComp graph) $ \case
    G.AcyclicSCC _ -> pure ()
    G.CyclicSCC cycle -> throwWithContext (mkError (map name cycle))
