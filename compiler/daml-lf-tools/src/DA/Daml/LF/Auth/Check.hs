-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | Check a package, generating lots of errors. Some of those errors are later solved.
module DA.Daml.LF.Auth.Check(
    checkPackage
    ) where

import           DA.Daml.LF.Ast
import           DA.Daml.LF.Auth.Type
import           DA.Daml.LF.Auth.Environment
import           Data.Generics.Uniplate.Data
import qualified Data.HashSet as HS


checkPackage :: Environment -> Package -> [Error]
checkPackage env (Package _version ms) = concatMap (checkModule s) ms
    where s = S env [] [] [] HS.empty

data S = S
    {env :: Environment
    ,auth :: [Expr] -- people who have authorised us (signatory or controllers)
    ,ctx :: [Ctx] -- what we have descended down (in reverse)
    ,frames :: [Frame] -- the callstack, in reverse
    ,_recursive :: HS.HashSet (Qualified ExprValName) -- things we have recursed into already
    }

addAuth :: Expr -> S -> S
addAuth x s = s{auth = x : auth s}

addCtx :: Ctx -> S -> S
addCtx x s = s{ctx = x : ctx s}

addFrame :: Frame -> S -> S
addFrame x s = s{frames = x : frames s}

solver :: S -> Pred -> [Error]
-- return failed to solve, and try and solve them later
solver S{..} p = [EFailedToSolve (reverse frames) (reverse ctx) p]


checkModule :: S -> Module -> [Error]
checkModule s Module{..} =
    concatMap (checkTemplate s moduleName) moduleTemplates

checkTemplate :: S -> ModuleName -> Template -> [Error]
checkTemplate s mname Template{..} =
    solver s2 (PredNotNull tplSignatories) ++
    concatMap (\c -> checkChoice (addFrame (FrameChoice (chcName c)) s2) c) tplChoices
    where s2 = addFrame (FrameTemplate $ Qualified PRSelf mname tplTypeCon) $ addCtx (CtxTrue tplPrecondition) $ addAuth tplSignatories s

checkChoice :: S -> TemplateChoice -> [Error]
checkChoice s TemplateChoice{..} =
    solver s2 (PredNotNull chcControllers) ++
    checkExpr s2 chcUpdate
    where s2 = addAuth chcControllers s

checkExpr :: S -> Expr -> [Error]
checkExpr s = \case
    EVal _ -> [] -- TODO, with regards to recursion and following
    EUpdate (UCreate tpl arg) -> withTemplate (env s) tpl $ \Template{..} ->
        solver (addCtx (CtxLet tplParam arg) s) $ PredSubsetEq tplSignatories $ auth s
    EUpdate UExercise{..} -> [] -- TODO, add an obligation condition
    ELet (Binding (v,_) e) x -> checkExpr s e ++ checkExpr (addCtx (CtxLet v e) s) x
    ECase x alts -> checkExpr s x ++ concat [checkExpr (addCtx (CtxCase pat x) s) e | CaseAlternative pat e <- alts]
    -- TODO, lots more besides
    x -> concatMap (checkExpr s) $ children x
