-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiWayIf #-}

module DA.Daml.LF.Optimize (optimize) where

{-
The goal of this Optimization is to remove redexs in (LF)expressions contained in an
(LF)module. Any redex which we reduce at compile-time, will not have to be reduced at run-rime by
an evaluator/engine. Because we are reducing under lambdas (inside function bodies), a single
compile-time reduction may correspond to multiple run-time reductions avoided.

The main category of redex to reduce is the applied-term-lambda (beta-redex).  Additionally we
reduce literal-struct-projections. Between them, this can remove much of the overhead of DAML
type-class polymorphism as supported by passing method-dictionaries as runtime arguments. Because
LF is explicitly typed, we also deal with redexes corresponding to applied-type-lambda.

In symbols, these are the reductions:

(1)     (\(v:T) . E1) E2                --> E1[v/E2]    -- applied-term-lambda (beta-redex)
(2)     < x1:E1 ... xn:En > . xi        --> Ei          -- literal-struct-projection
(3)     (/\(tv:K) . E)[T]               --> E[tv/T]     -- applied-type-lambda

However, when lambda-terms (and literal-structs) are defined as top-level definitions, the above
redexs wont be syntactically explicit in the LF. To handle these cases requites we _inline_ the
definition of `v` at each application. i.e.

        def v = \(v:T) . E1
        ... v E2 ...                    --> (\(v:T) . E1) E2

The optimizer works in the manner of an evaluator, with `SemValue` the type of the resulting
_semantic value _ of the _evaluation_. The implementation proceeds via the interplay of two
functions: `reflect` and `reify`.

        reflect :: LF.Expr -> Effect SemValue
        reify :: SemValue -> Effect LF.Expr

Both transformations take place in the `Effect` monad which manages context information, and state
required for fresh name generation. (And the current packageId, but that might not be necessary).

The type of (LF)expressions is embedded within the type of `SemValue`, via the `Syntax`
constructor. Each use of `Syntax` represents a case where we are giving up attempting to normalise
further.

        data SemValue = Syntax LF.Expr | ...

The other constructors: `Macro`, `Struct` and `TyMacro` correspond to the three kinds of redex we
which to handle specially. And for each there is a corresponding _apply_ function:

        termApply :: (SemValue,SemValue) -> Effect SemValue
        typeApply :: SemValue -> LF.Type -> Effect SemValue
        applyProjection :: LF.FieldName -> SemValue -> Effect SemValue

This is where the redex reduction is actually performed when possible.  When the reduction is not
possible (because the applicant is `Syntax` rather than the desired special form) then the
Syntactic-level application is reconstructed, reifying any normalized expression back to syntactic
LF expressions as required.

This code is just proof-of-concept/WIP, and not all forms of LF are yet handled. In particular it
currently only performs optimization of expressions bound as top level definitions.

The approach to (mutual)recursion is to track which top level definitions we have already expanded,
and not recursively expand a definition within the scope of its own expansion. See `WithDontInline`


Here is a (very) small example of the effect of optimizing a DAML function to decrement an integer:
with the appoximate DAML-LF shown (with nicer vars names for explication!)

    DAML:
        decrement x = x - 1

    DAML-LF (original):

        def decrement = \(x:Int64) -> (-)[Int64] $fAdditiveInt x 1

        def (-) = \[t].(\(dict:GHC.Num:Additive t) -> dict.m_- ())

        def $fAdditiveInt =
          { m_+      = \(_:Unit) -> ADDI
          , m_aunit  = \(_:Unit) -> $caunit
          , m_-      = \(_:Unit) -> SUBI
          , m_negate = \(_:Unit) -> $cnegate
          }

        def $caunit = 0

        def $cnegate = \(x:Int64) -> (-)[Int64] $fAdditiveInt 0 x


    DAML-LF (optimized):

        def decrement = \(_v:Int64) -> SUBI _v 1


One potential problem with this approach to optimization is blow up in the size of the generated
code. So far I haven't witnessed this, but I have only run quite small examples.

Potential speedups.. Using the classic "nfib" function DAML:

    nfib : Int -> Int
    nfib 0 = 1
    nfib 1 = 1
    nfib n = nfib (n-1) + nfib (n-2) + 1

I have witnessed a speed up of around x5 using my own simplistic evaluator:

When using the real scala evalator/engine, running on a sandbox, the speedup is still > x2.
It's harder to measure, but it is real.

-}

import Control.Monad (ap,liftM,forM,(>=>))
import Control.Lens (toListOf)
import Data.List (intercalate)
import Data.Map.Strict (Map)
import Data.Set (Set)
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Ast.Type as LF
import qualified DA.Daml.LF.Ast.Optics as LF
import qualified Data.Map.Strict as Map
import qualified Data.NameMap as NM
import qualified Data.Set as Set
import qualified Data.Text as Text


-- | Optimize an LF.Module using NbE
optimize :: [LF.ExternalPackage] -> LF.Module -> IO LF.Module
optimize pkgs mod@LF.Module{moduleName,moduleValues} = do
  moduleValues <- NM.traverse optimizeDef moduleValues
  return mod {LF.moduleValues}
    where
      optimizeDef :: LF.DefValue -> IO LF.DefValue
      optimizeDef dval = do
        let LF.DefValue{dvalBinder=(name,_),dvalBody=expr} = dval
        let qval = LF.Qualified{qualPackage=LF.PRSelf, qualModule=moduleName, qualObject=name}
        expr <- pure $ do -- TODO: no need to be in IO anymore!
          -- run the optimization effect separately for the body of each top-level definition
          runEffect pkgs mod $ do
            WithDontInline qval $ normExpr expr
        return dval {LF.dvalBody=expr}


-- | An expression is normalised by first reflecting into the SemValue domain, and then reifying
normExpr :: LF.Expr -> Effect LF.Expr
normExpr = reflect >=> reify


-- | The type of normalized expressions
data SemValue
  = Syntax LF.Expr
-- TODO: introduce `Variable` to replace `Syntax`; cleaning up handling of `duplicatable`
--  | Variable LF.ExprVarName
  | Struct Context [(LF.FieldName,SemValue)]
  | Macro LF.Type (SemValue -> Effect SemValue)
  | TyMacro LF.Kind (LF.Type -> Effect SemValue)


type QVal = LF.ValueRef -- TODO: inline & rename code

getQValBody :: QVal -> Effect LF.Expr
getQValBody qval = do
  let LF.Qualified{qualPackage=pref, qualModule=moduleName, qualObject=name} = qval
  case pref of
    LF.PRSelf -> do
      mod <- GetTheModule
      getExprValNameFromModule mod name
    LF.PRImport pid -> do
      mod <- getModule pid moduleName
      getExprValNameFromModule mod name

getModule :: LF.PackageId -> LF.ModuleName -> Effect LF.Module
getModule pid moduleName = do
  package <- GetPackage pid
  let LF.Package{packageModules} = package
  case NM.lookup moduleName packageModules of
    Nothing -> error $ "getModule, " <> show (pid,moduleName)
    Just mod -> return mod

getExprValNameFromModule :: LF.Module -> LF.ExprValName -> Effect LF.Expr
getExprValNameFromModule mod name = do
  let LF.Module{moduleName,moduleValues} = mod
  case NM.lookup name moduleValues of
    Nothing -> error $ "getExprValNameFromModule, " <> show (moduleName,name)
    Just dval -> do
      let LF.DefValue{dvalBody=expr} = dval
      return expr


reflect :: LF.Expr -> Effect SemValue
reflect = \case

  -- We must traverse deeply on any structure which contains nested expressions or types.
  -- This is because vars are renamed, as are type-vars.

  LF.EVar name -> do
    env <- GetEnv
    case Map.lookup name env of
      Just v -> return v
      Nothing -> error $ "reflect, unbound var: " <> show name

  LF.EVal qval -> do
    body <- getQValBody qval
    let isDirectlyRecursive = qval `elem` toListOf LF.exprValueRef body
    dontInline <- DontInline qval
    if
      | isDirectlyRecursive || dontInline -> do
          return $ Syntax $ LF.EVal qval

      | otherwise -> do
          WithDontInline qval $ reflect body

  x@LF.EBuiltin{} -> return $ Syntax x

  LF.ERecCon{recTypeCon=tca,recFields} -> do
    -- TODO: special support for records, like for structs?
    tca <- normTypeConApp tca
    recFields <- forM recFields $ \(fieldName,expr) -> do
      expr <- normExpr expr
      return (fieldName,expr)
    return $ Syntax $ LF.ERecCon{recTypeCon=tca,recFields}

  LF.ERecProj{recTypeCon=tca,recField=fieldName,recExpr=expr} -> do
    -- TODO: special support for records, like for structs?
    tca <- normTypeConApp tca
    expr <- normExpr expr
    return $ Syntax $ LF.ERecProj{recTypeCon=tca,recField=fieldName,recExpr=expr}

  LF.ERecUpd{recTypeCon,recField,recExpr=e,recUpdate=u} -> do
    e <- normExpr e
    u <- normExpr u
    return $ Syntax $ LF.ERecUpd{recTypeCon,recField,recExpr=e,recUpdate=u}

  LF.EVariantCon{varTypeCon=tca,varVariant,varArg=expr} -> do
    tca <- normTypeConApp tca
    expr <- normExpr expr
    return $ Syntax $ LF.EVariantCon{varTypeCon=tca,varVariant,varArg=expr}

  x@LF.EEnumCon{} -> return $ Syntax x

  LF.EStructCon{structFields} -> do
    rp <- Save
    structFields <- forM structFields $ \(fieldName,expr) -> do
      expr <- reflect expr
      return (fieldName,expr)
    return $ Struct rp structFields

  LF.EStructProj{structField=field,structExpr=expr} -> do
    expr <- reflect expr
    applyProjection field expr

  LF.EStructUpd{} -> undefined -- TODO, support this final case!

  LF.ETmApp{tmappFun=e1,tmappArg=e2} -> do
    v1 <- reflect e1
    v2 <- reflect e2
    termApply (v1,v2)

  LF.ETyApp{tyappExpr=expr,tyappType=ty} -> do
    expr <- reflect expr
    ty <- normType ty
    typeApply expr ty

  LF.ETmLam{tmlamBinder=binder,tmlamBody=body} -> do
    let (name,ty) = binder
    ty <- normType ty
    rp <- Save
    return $ Macro ty $ \arg ->
      Restore rp $
      ModEnv (Map.insert name arg) $ reflect body

  LF.ETyLam{tylamBinder=binder, tylamBody=expr} -> do
    let (tv,kind) = binder
    rp <- Save
    return $ TyMacro kind $ \ty ->
      Restore rp $ do
      ModSubst (Map.insert tv ty) $ reflect expr

  LF.ECase{casScrutinee=scrut, casAlternatives=alts} -> do
    scrut <- normExpr scrut
    if
      | allowDupK -> do
          -- Grab and duplicate the continuation for better optimization.
          ShiftK $ \(k :: SemValue -> LF.Expr) -> do
            alts <- forM alts $ \LF.CaseAlternative{altPattern=pat,altExpr=expr} -> do
              freshenCasePattern pat $ \pat -> do
                expr <- ResetK (k <$> reflect expr)
                return $ LF.CaseAlternative{altPattern=pat,altExpr=expr}
            return $ LF.ECase{casScrutinee=scrut, casAlternatives=alts}

      | otherwise -> do
          -- Previous simpler behaviour if it turns out the code blowup from above is unacceptable.
          alts <- forM alts $ \LF.CaseAlternative{altPattern=pat,altExpr=expr} -> do
            freshenCasePattern pat $ \pat -> do
              expr <- ResetK $ normExpr expr
              return $ LF.CaseAlternative{altPattern=pat,altExpr=expr}
          return $ Syntax $ LF.ECase{casScrutinee=scrut, casAlternatives=alts}

    where allowDupK = True

  LF.ELet{letBinding=bind,letBody=body} -> do
    let LF.Binding{bindingBinder=binder,bindingBound=rhs} = bind
    reflect $ LF.ETmApp (LF.ETmLam binder body) rhs

  LF.ENil{nilType=ty} -> do
    -- If we forget to normalize the type here (and other places), the bug is not (yet) detected by
    -- unit tests, but only when building a DAR (--run-optimizer); fails to TC (unknown type var)
    ty <- normType ty
    return $ Syntax LF.ENil{nilType=ty}

  LF.ECons{consType=ty,consHead=h,consTail=t} -> do
    ty <- normType ty
    h <- normExpr h
    t <- normExpr t
    return $ Syntax $ LF.ECons{consType=ty,consHead=h,consTail=t}

  LF.ESome{someType=ty,someBody=expr} -> do
    ty <- normType ty
    expr <- normExpr expr
    return $ Syntax $ LF.ESome{someType=ty,someBody=expr}

  LF.ENone{noneType=ty} -> do
    ty <- normType ty
    return $ Syntax $ LF.ENone{noneType=ty}

  LF.EUpdate upd -> do
    upd <- normUpdate upd
    return $ Syntax $ LF.EUpdate upd

  x@LF.EScenario{} -> return $ Syntax x -- TODO: traverse deeply

  LF.EToAny{toAnyType=ty,toAnyBody=expr} -> do
    -- Coverage of LF.EToAny (etc) is detected by unit tests, so long as Examples contain a template
    ty <- normType ty
    expr <- normExpr expr
    return $ Syntax LF.EToAny{toAnyType=ty,toAnyBody=expr}

  LF.EFromAny{fromAnyType=ty,fromAnyBody=expr} -> do
    ty <- normType ty
    expr <- normExpr expr
    return $ Syntax LF.EFromAny{fromAnyType=ty,fromAnyBody=expr}

  LF.ETypeRep ty -> do
    ty <- normType ty
    return $ Syntax $ LF.ETypeRep ty

  LF.ELocation _loc expr -> do
    -- If LF.ELocation were to block normalization, the bug would be detected in unit-testing, via
    -- an unexpected number of app-counts (too high!)
    reflect expr
    --expr <- normExpr expr
    --return $ Syntax $ LF.ELocation _loc expr


freshenCasePattern :: LF.CasePattern -> (LF.CasePattern -> Effect a) -> Effect a
freshenCasePattern pat k = case pat of
  LF.CPVariant{patTypeCon,patVariant,patBinder=x} -> do
    freshenExpVarName x $ \x -> do
      k $ LF.CPVariant{patTypeCon,patVariant,patBinder=x}

  LF.CPCons {patHeadBinder=x1,patTailBinder=x2} -> do
    freshenExpVarName x1 $ \x1 -> do
      freshenExpVarName x2 $ \x2 -> do
        k $ LF.CPCons{patHeadBinder=x1,patTailBinder=x2}

  LF.CPSome{patBodyBinder=x} -> do
    freshenExpVarName x $ \x -> do
      k $ LF.CPSome{patBodyBinder=x}

  -- no binders in these
  x@LF.CPEnum{} -> k x
  x@LF.CPUnit -> k x
  x@LF.CPBool{} -> k x
  x@LF.CPNil -> k x
  x@LF.CPNone -> k x
  x@LF.CPDefault -> k x

freshenExpVarName :: LF.ExprVarName -> (LF.ExprVarName -> Effect a) -> Effect a
freshenExpVarName x k = do
  let name :: String = (Text.unpack . LF.unExprVarName) x
  x' <- Fresh name -- use old name as base for new
  let f = Map.insert x (Syntax (LF.EVar x'))
  ModEnv f $ k x'


normUpdate :: LF.Update -> Effect LF.Update
normUpdate = \case
  -- TODO: finish work to traverse all cases...
  x@LF.UPure{} -> return x
  x@LF.UBind{} -> return x

  LF.UCreate{creTemplate,creArg=expr} -> do
    expr <- normExpr expr
    return $ LF.UCreate{creTemplate,creArg=expr}

  LF.UExercise{exeTemplate,exeChoice,exeContractId=e1,exeActors=e2,exeArg=e3} -> do
    e1 <- normExpr e1
    e2 <- traverse normExpr e2 -- e2 is Maybe
    e3 <- normExpr e3
    return $ LF.UExercise{exeTemplate,exeChoice,exeContractId=e1,exeActors=e2,exeArg=e3}

  LF.UFetch{fetTemplate,fetContractId=expr} -> do
    expr <- normExpr expr
    return $ LF.UFetch{fetTemplate,fetContractId=expr}

  x@LF.UGetTime{} -> return x
  x@LF.UEmbedExpr{} -> return x
  x@LF.ULookupByKey{} -> return x
  x@LF.UFetchByKey{} -> return x


-- TODO: have specialized error function to use when LF is badly types

-- TODO: The LF.Type contained `Macro` must e normalized. Use a newtype to capture this.

nameIt :: String -> LF.Type -> LF.Expr -> Effect LF.ExprVarName
nameIt reason ty exp = do
  name <- Fresh reason
  let bind = LF.Binding{bindingBinder=(name,ty),bindingBound=exp}
  WrapK (\body -> LF.ELet{letBinding=bind,letBody=body}) (return name)

termApply :: (SemValue,SemValue) -> Effect SemValue
termApply = \case
  (TyMacro{},_) -> error "termApply, TyMacro"
  (Syntax func, arg) -> do
    arg <- reify arg
    return $ Syntax $ LF.ETmApp{tmappFun=func,tmappArg=arg}
  (Struct _ _, _) -> error "SemValue,termApply,struct"
  (Macro ty func, arg) -> do
    case duplicatable arg of
      Nothing -> func arg -- beta-reduction here
      Just reason -> do
        arg <- reify arg
        name <- nameIt reason ty arg
        func (Syntax (LF.EVar name)) -- beta-reduction here


typeApply :: SemValue -> LF.Type -> Effect SemValue
typeApply expr ty = case expr of -- This ty is already normalized
  Syntax expr -> do
    return $ Syntax $ LF.ETyApp{tyappExpr=expr,tyappType=ty}
  Struct{} -> error "typeApply, Struct"
  Macro{} -> error "typeApply, Macro"
  TyMacro _ f -> f ty

-- A normalized-expression can be duplicated if it has no computation effect.
-- Indicated by return of `Nothing`
-- If it mustn't be duplicated, the returned Maybe String indicates why.

duplicatable :: SemValue -> Maybe String
duplicatable = \case

  -- We always regard a TyMacro as having no computational effect (as the spec says) and thus able
  -- to be freely duplicated/dropped.
  TyMacro{} -> Nothing

  -- TODO: we should only consider a Struct duplicatable when all its fields are.
  Struct{} -> Nothing
  Macro{} -> Nothing
  Syntax expr -> duplicatableExpr expr

duplicatableExpr :: LF.Expr -> Maybe String
duplicatableExpr = \case
  LF.EBuiltin{} -> Nothing
  LF.EVar{} -> Nothing
  LF.EVal{} -> Nothing
  LF.ELocation _ expr -> duplicatableExpr expr
  LF.ETmApp{} -> Just "_A"
  LF.ELet{} -> Just "_L"
  _ -> Just "_v"

-- Note: every call to `reify` is a barrier to normalization (most come via `normExpr`)
reify :: SemValue -> Effect LF.Expr
reify = \case

  Syntax exp -> return exp

  TyMacro kind f -> do
    tv <- FreshTV
    let ty = LF.TVar tv
    body <- ResetK (f ty >>= reify)
    return $ LF.ETyLam{tylamBinder=(tv,kind), tylamBody=body}

  Struct rp xs -> do
    Restore rp $ do
      structFields <- forM xs $ \(name,n) -> do
        exp <- reify n
        return (name,exp)
      return $ LF.EStructCon{structFields}

  Macro ty f -> do
    name <- Fresh "_r"
    body <- ResetK (f (Syntax (LF.EVar name)) >>= reify)
    return $ LF.ETmLam{tmlamBinder=(name,ty),tmlamBody=body}

applyProjection :: LF.FieldName -> SemValue -> Effect SemValue
applyProjection field = \case
  TyMacro{} -> error "applyProjection,TyMacro"
  Macro{} -> error "applyProjection,Macro"
  Syntax expr -> return $ Syntax $ LF.EStructProj{structField=field,structExpr=expr}
  Struct _ xs -> do -- TODO: dont loose restore-point
    case lookup field xs of
      Nothing -> error $ "applyProjection, " <> show field
      Just v -> return v -- insert restore-point here.

normTypeConApp :: LF.TypeConApp -> Effect LF.TypeConApp
normTypeConApp LF.TypeConApp{tcaTypeCon,tcaArgs=tys} = do
  tys <- mapM normType tys
  return LF.TypeConApp{tcaTypeCon,tcaArgs=tys}

normType :: LF.Type -> Effect LF.Type
normType typ = do
  subst <- GetSubst
  return $ LF.substitute subst typ


data Effect a where
  Ret :: a -> Effect a
  Bind :: Effect a -> (a -> Effect b) -> Effect b
  Save :: Effect Context
  Restore :: Context -> Effect a -> Effect a
  GetSubst :: Effect LF.Subst
  ModSubst :: (LF.Subst -> LF.Subst) -> Effect a -> Effect a
  GetEnv :: Effect Env
  ModEnv :: (Env -> Env) -> Effect a -> Effect a
  Fresh :: String -> Effect LF.ExprVarName
  FreshTV :: Effect LF.TypeVarName
  GetTheModule :: Effect LF.Module
  GetPackage :: LF.PackageId -> Effect LF.Package
  DontInline :: QVal -> Effect Bool
  WithDontInline :: QVal -> Effect a -> Effect a

  -- Operator which manipulate the continuation
  WrapK :: (LF.Expr -> LF.Expr) -> Effect a -> Effect a
  ResetK :: Effect LF.Expr -> Effect LF.Expr
  ShiftK :: ((a -> LF.Expr) -> Effect LF.Expr) -> Effect a


instance Functor Effect where fmap = liftM
instance Applicative Effect where pure = return; (<*>) = ap
instance Monad Effect where return = Ret; (>>=) = Bind

type Res = (LF.Expr,State)

runEffect :: [LF.ExternalPackage] -> LF.Module -> Effect LF.Expr -> LF.Expr
runEffect pkgs theModule eff0 = fst $ loop context0 state0 eff0 k0
  where
    context0 = Context { scope = Set.empty, subst = Map.empty, env = Map.empty }
    k0 state e = (e,state)

    loop :: Context -> State -> Effect a -> (State -> a -> Res) -> Res
    loop context@Context{scope,subst,env} state eff k = case eff of

      Ret x -> k state x
      Bind eff f -> loop context state eff $ \state v -> loop context state (f v) k

      Save -> k state context
      Restore context eff -> loop context state eff k

      GetSubst -> k state subst
      ModSubst f eff -> loop context { subst = f subst } state eff k

      GetEnv -> k state env
      ModEnv f eff -> loop context { env = f env } state eff k

      Fresh tag -> do
        let (name,state') = generateName state
        let x = LF.ExprVarName $ Text.pack (tag <> name)
        k state' x

      FreshTV -> do -- share the same unique name source as for term-vars
        let tag = "_tv"
        let (name,state') = generateName state
        let tv = LF.TypeVarName $ Text.pack (tag <> name)
        k state' tv

      GetTheModule -> do
        k state theModule

      GetPackage pid -> do
        k state (getPackage pid)

      DontInline qval -> do
        k state (Set.member qval scope)

      WithDontInline qval eff -> do
        loop context { scope = Set.insert qval scope } state eff k

      WrapK f eff ->
        loop context state eff $ \state v -> f' (k state v) where f' (e,s) = (f e, s)

      ResetK eff -> do
        let (v,state') = loop context state eff k0
        k state' v

      ShiftK f -> do
        let (state1,state2) = splitState state
        loop context state2 (f (fst . k state1)) k0

    packageMap = Map.fromList [ (pkgId,pkg) | LF.ExternalPackage pkgId pkg <- pkgs ]

    getPackage :: LF.PackageId -> LF.Package
    getPackage pid =
      case Map.lookup pid packageMap of
        Just v -> v
        Nothing -> error $ "getPackage, " <> show pid


data Context = Context
  { scope :: Set QVal
  , subst :: LF.Subst
  , env :: Env
  }

type Env = Map LF.ExprVarName SemValue


-- | Splittable state. Currently contains a single unique name source.
data State = State { unique :: Unique }

state0 :: State
state0 = State { unique = unique0 }

splitState :: State -> (State,State)
splitState State{unique=u} = let (u1,u2) = splitUnique u in (State{unique=u1}, State{unique=u2})

generateName :: State -> (String,State)
generateName State{unique=u} = let (name,u') = generateUniqueName u in (name, State {unique = u'})


-- | Splittable source of unique names
data Unique = Unique Int [Int]

unique0 :: Unique
unique0 = Unique 0 []

-- | Split a unique name source in two. Prefer the first in the pair as it is more concise.
splitUnique :: Unique -> (Unique,Unique)
splitUnique (Unique n xs) = (Unique (n+1) xs, Unique 0 (n:xs))

generateUniqueName :: Unique -> (String,Unique)
generateUniqueName (Unique n xs) = do
  let name = intercalate "_" $ map show (reverse (n:xs))
  (name, Unique (n+1) xs)
