-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

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

module DA.Daml.LF.Optimize
  ( World(..) -- TODO: subsume this World with [LF.ExternalPackage]
  , optimizeWorld -- testing entry point
  , optimizeModule
  ) where

import Control.Monad (ap,liftM,forM,(>=>))
import Data.Map.Strict (Map)
import Data.Set (Set)
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Ast.Type as LF
import qualified Data.Map.Strict as Map
import qualified Data.NameMap as NM
import qualified Data.Set as Set
import qualified Data.Text as Text

-- The type of normalized expressions
data SemValue
  = Syntax LF.Expr
  | Struct Context [(LF.FieldName,SemValue)]
  | Macro LF.Type (SemValue -> Effect SemValue)
  | TyMacro LF.Kind (LF.Type -> Effect SemValue)


-- An expression is normalised by first reflecting into the SemValue domain, and then reifying
normExpr :: LF.Expr -> Effect LF.Expr
normExpr = reflect >=> reify


-- TODO: The LF.Type contained `Macro` must e normalized. Use a newtype to capture this.

optimizeWorld :: World -> IO World
optimizeWorld world = do
  let World {mainIdM,packageMap} = world
  case mainIdM of
    Nothing -> error "optimizeWorld, mainIdM = Nothing"
    Just mainId -> do
      -- TODO: dont expect to find mainPkg in the packageMap
      let mainPkg =
            case Map.lookup mainId packageMap of
              Just v -> v
              Nothing -> error $ "lookup mainId failed, " <> show mainId
      -- we only optimize (and replace) the main package here
      mainPackageO <- optimizePackage world mainPkg
      let otherPairs = [ (pid,pkg) | (pid,pkg) <- Map.toList packageMap, pid /= mainId ]
      let packageMapO = Map.fromList $ (mainId,mainPackageO) : otherPairs
      return World { mainIdM
                   , packageMap = packageMapO
                   , mainPackageM = Just mainPackageO
                   }

optimizePackage :: World -> LF.Package -> IO LF.Package
optimizePackage world pkg =
  run world $ normPackage pkg

optimizeModule :: World -> LF.Module -> IO LF.Module
optimizeModule world mod = do
  run world $ normModule mod

data World = World -- TODO: rethink the components of this type. Will [LF.ExternalPackage] do?
  { mainIdM :: Maybe LF.PackageId
  , packageMap :: Map LF.PackageId LF.Package
  , mainPackageM :: Maybe LF.Package
  }

normPackage :: LF.Package -> Effect LF.Package
normPackage pkg = do
  let LF.Package{packageModules} = pkg
  packageModules <- NM.traverse normModule packageModules
  return $ pkg {LF.packageModules}

normModule :: LF.Module -> Effect LF.Module
normModule mod = do
  let LF.Module{moduleName,moduleValues} = mod -- TODO: traverse over templates
  moduleValues <- NM.traverse (normDef moduleName) moduleValues
  return mod {LF.moduleValues}

-- TODO: not great as set items because the qualPackage can be implicit(Self) or explicit
type QVal = LF.Qualified LF.ExprValName

normDef :: LF.ModuleName -> LF.DefValue -> Effect LF.DefValue
normDef moduleName dval = do
  let LF.DefValue{dvalBinder=(name,_),dvalBody=expr} = dval
  let qval = LF.Qualified{qualPackage=LF.PRSelf, qualModule=moduleName, qualObject=name}
  expr <- (WithDontInline qval $ reflect expr) >>= reify
  return dval {LF.dvalBody=expr}

-- TODO: better to start from a World where all external packages have no PRSelf
explicateSelfPid :: QVal -> Effect QVal
explicateSelfPid qval = do
  let LF.Qualified{qualPackage=pref, qualModule=moduleName, qualObject=name} = qval
  (case pref of
    LF.PRImport pid -> return $ Just pid
    LF.PRSelf -> do
      GetPid >>= \case
        Just pid -> return $ Just pid
        Nothing -> return Nothing --error "explicateSelfPid,Nothing"
    ) >>= \case
    Nothing -> return qval
    Just pid -> do
      let pref = LF.PRImport pid
      return $ LF.Qualified{qualPackage=pref, qualModule=moduleName, qualObject=name}

reflectQualifiedExprValName :: QVal -> Effect SemValue
reflectQualifiedExprValName qval = do
  let LF.Qualified{qualPackage=pref, qualModule=moduleName, qualObject=name} = qval
  case pref of

    LF.PRSelf -> do
      pidm <- GetPid
      WithPid pidm $ do
        getExprValName pidm moduleName name >>= \case
          Just expr -> reflect expr
          Nothing -> return $ Syntax $ LF.EVal qval

    LF.PRImport pid -> do
      let pidm = Just pid
      WithPid pidm $ do
        getExprValName pidm moduleName name >>= \case
          Just expr -> reflect expr
          Nothing -> return $ Syntax $ LF.EVal qval

getExprValName :: Maybe LF.PackageId -> LF.ModuleName -> LF.ExprValName -> Effect (Maybe LF.Expr)
getExprValName pid moduleName name = do
  getModule pid moduleName >>= \case
    Nothing -> return Nothing
    Just mod -> do
      let LF.Module{moduleValues} = mod
      case NM.lookup name moduleValues of
        Nothing -> error $ "simpExprValName, " <> show name
        Just dval -> do
          let LF.DefValue{dvalBody=expr} = dval
          return $ Just expr

getModule :: Maybe LF.PackageId -> LF.ModuleName -> Effect (Maybe LF.Module)
getModule pid moduleName = do
  GetPackage pid >>= \case
    Nothing -> return Nothing
    Just package -> do
      let LF.Package{packageModules} = package
      case NM.lookup moduleName packageModules of
        Nothing -> error $ "getModule, " <> show (pid,moduleName)
        Just mod -> return $ Just mod


reflect :: LF.Expr -> Effect SemValue
reflect = \case

  -- We must traverse deeply on any structure which contains nested expressions or types.
  -- This is because vars are renamed, as are type-vars.

  LF.EVar name -> do
    env <- GetEnv
    case Map.lookup name env of
      Just v -> return v
      Nothing ->
        -- TODO: We ought to be able to error here.
        -- When normalizing under lambda, we should always rename vars (and add them to the env)
        -- and then we can this error should be ok.
        --error $ "reflect, " <> show name
        return $ Syntax $ LF.EVar name

  LF.EVal qval -> do
    ShouldInline qval >>= \case
      True -> do
        WithDontInline qval $
          reflectQualifiedExprValName qval
      False -> do
        qval <- explicateSelfPid qval
        return $ Syntax $ LF.EVal qval

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

  LF.ECase{casScrutinee=expr, casAlternatives=alts} -> do
    expr <- normExpr expr
    alts <- mapM normAlt alts
    return $ Syntax $ LF.ECase{casScrutinee=expr, casAlternatives=alts}

  LF.ELet{letBinding=bind,letBody=body} -> do
    let LF.Binding{bindingBinder=(name,ty),bindingBound=rhs} = bind
    rhs <- reflect rhs
    if duplicatable rhs then ModEnv (Map.insert name rhs) $ reflect body else do
      rhs <- reify rhs
      body <- normExpr body
      ty <- normType ty
      let bind = LF.Binding{bindingBinder=(name,ty),bindingBound=rhs}
      return $ Syntax $ LF.ELet{letBinding=bind,letBody=body}

  LF.ENil{nilType=ty} -> do
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

  LF.ELocation loc expr -> do
    expr <- normExpr expr
    return $ Syntax $ LF.ELocation loc expr

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

normAlt :: LF.CaseAlternative -> Effect LF.CaseAlternative
normAlt = \case
  -- TODO: rename vars in the pattern?
  LF.CaseAlternative{altPattern=pat,altExpr=expr} -> do
    expr <- normExpr expr
    return $ LF.CaseAlternative{altPattern=pat,altExpr=expr}

-- TODO: have specialized error function to use when LF is badly types

termApply :: (SemValue,SemValue) -> Effect SemValue
termApply = \case
  (TyMacro{},_) -> error "termApply, TyMacro"
  (Syntax func, arg) -> do
    arg <- reify arg
    return $ Syntax $ LF.ETmApp{tmappFun=func,tmappArg=arg}
  (Struct _ _, _) -> error "SemValue,termApply,struct"
  (Macro ty func, arg) -> do
    let doInline = duplicatable arg
    -- if doInline is True, then the beta-reduction effectively occurs...
    if doInline then func arg else do
      -- because we dont reconstruct any syntax
      name <- Fresh
      rhs <- reify arg
      body <- func (Syntax (LF.EVar name)) >>= reify
      let bind = LF.Binding{bindingBinder=(name,ty),bindingBound=rhs}
      return $ Syntax $ LF.ELet{letBinding=bind,letBody=body}


typeApply :: SemValue -> LF.Type -> Effect SemValue
typeApply expr ty = case expr of -- This ty is already normalized
  Syntax expr -> do
    return $ Syntax $ LF.ETyApp{tyappExpr=expr,tyappType=ty}
  Struct{} -> error "typeApply, Struct"
  Macro{} -> error "typeApply, Macro"
  TyMacro _ f -> f ty

-- A normalized-expression can be duplicated if it has no computation effect
duplicatable :: SemValue -> Bool
duplicatable = \case
  TyMacro{} -> True
  -- TODO: we should only consider a Struct duplicatable when all its fields are.
  Struct{} -> True
  Macro{} -> True
  Syntax exp -> case exp of
    LF.EBuiltin{} -> True
    LF.EVar{} -> True
    LF.EVal{} -> True
    _ -> False

-- Note: every call to `reify` is a barrier to normalization (most come via `normExpr`)
reify :: SemValue -> Effect LF.Expr
reify = \case

  Syntax exp -> return exp

  TyMacro kind f -> do
    tv <- FreshTV
    let ty = LF.TVar tv
    body <- f ty >>= reify
    return $ LF.ETyLam{tylamBinder=(tv,kind), tylamBody=body}

  Struct rp xs -> do
    Restore rp $ do
      structFields <- forM xs $ \(name,n) -> do
        exp <- reify n
        return (name,exp)
      return $ LF.EStructCon{structFields}

  Macro ty f -> do
    name <- Fresh
    body <- f (Syntax (LF.EVar name)) >>= reify
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
  Fresh :: Effect LF.ExprVarName
  FreshTV :: Effect LF.TypeVarName
  GetPid :: Effect (Maybe LF.PackageId)
  WithPid :: Maybe LF.PackageId -> Effect a -> Effect a
  GetPackage :: Maybe LF.PackageId -> Effect (Maybe LF.Package)
  ShouldInline :: QVal -> Effect Bool
  WithDontInline :: QVal -> Effect a -> Effect a

instance Functor Effect where fmap = liftM
instance Applicative Effect where pure = return; (<*>) = ap
instance Monad Effect where return = Ret; (>>=) = Bind

run :: World -> Effect a -> IO a -- Only in IO for debug
run world eff = do
  (v,_state) <- loop pidm0 scope0 subst0 env0 state0 eff
  return v

  where
    World{mainIdM,packageMap,mainPackageM} = world

    pidm0 = mainIdM
    scope0 = Set.empty
    subst0 = Map.empty
    env0 = Map.empty
    state0 = State { unique = 0 }

    loop :: Maybe LF.PackageId -> InlineScope -> LF.Subst -> Env -> State -> Effect a -> IO (a,State)
    loop pidm scope subst env state = \case
      Ret x -> return (x,state)
      Bind eff f -> do (v,state') <- loop pidm scope subst env state eff; loop pidm scope subst env state' (f v)

      -- TODO: pidm/scope/env -- always travel together? so keep as 1 arg? --of type Context
      Save -> return (Context pidm scope subst env, state)
      Restore (Context pidm scope subst env) eff -> loop pidm scope subst env state eff

      GetSubst -> return (subst,state)
      ModSubst f eff -> loop pidm scope (f subst) env state eff

      GetEnv -> return (env,state)
      ModEnv f eff -> loop pidm scope subst (f env) state eff

      Fresh -> do -- take original name as a base which we can uniquify
        let tag = "_v"
        let State{unique} = state
        let state' = state { unique = unique + 1 }
        let x = LF.ExprVarName $ Text.pack (tag <> show unique)
        return (x,state')

      FreshTV -> do -- share the same counter as for term-vars
        let tag = "_tv"
        let State{unique} = state
        let state' = state { unique = unique + 1 }
        let tv = LF.TypeVarName $ Text.pack (tag <> show unique)
        return (tv,state')

      GetPid -> do
        return (pidm,state)

      WithPid pidm eff -> do
        loop pidm scope subst env state eff

      GetPackage pidm -> do
        return (getPackage pidm, state)

      ShouldInline qval -> do
        -- TODO : not if isDirectlyRecursive (code it!) -- need to see body
        let answer = not (Set.member qval scope)
        return (answer, state)

      WithDontInline qval eff -> do
        loop pidm (Set.insert qval scope) subst env state eff

    getPackage :: Maybe LF.PackageId -> Maybe LF.Package
    getPackage pidm =
      case pidm of
        Nothing -> mainPackageM
        Just pid ->
          case Map.lookup pid packageMap of
            Just v -> Just v
            Nothing -> error $ "getPackage, " <> show pid

-- TODO: rename as Context
data Context = Context (Maybe LF.PackageId) InlineScope LF.Subst Env

type Env = Map LF.ExprVarName SemValue
data State = State { unique :: Unique }
type Unique = Int
type InlineScope = Set QVal
