-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Daml.LF.Evaluator.Simp (simplify) where

import Control.Monad (ap,liftM,forM)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.NameMap as NM

import DA.Daml.LF.Evaluator.Exp (Prog,Exp,Alt)
import DA.Daml.LF.Evaluator.Value (Value)
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Evaluator.Exp as Exp
import qualified DA.Daml.LF.Evaluator.Value as Value

simplify :: [LF.ExternalPackage] -> LF.Module -> LF.ExprValName -> Prog
simplify pkgs mod name = do
  runEffect pkgs mod $ do
    simpModuleValName mod name

simpModuleValName :: LF.Module -> LF.ExprValName -> Effect Exp
simpModuleValName mod name = do
  let LF.Module{moduleValues} = mod
  case NM.lookup name moduleValues of
    Nothing -> error $ "simpModuleValName, " <> show name
    Just dval -> do
      let LF.DefValue{dvalBody=expr} = dval
      let key = Exp.DefKey (Nothing,name)
      i <- Share key $ simpExpr expr
      return $ Exp.Ref i

simpExpr :: LF.Expr -> Effect Exp
simpExpr expr = case expr of

  LF.EVar name -> return $ Exp.Var name
  LF.EVal q -> simpQualifiedExprValName q
  LF.EBuiltin builtin -> return $ Exp.Lit $ simpBuiltin builtin

  LF.ERecCon{recTypeCon=_,recFields} -> do
    xs <- forM recFields $ \(fieldName,expr) -> do
      e <- simpExpr expr
      return (fieldName,e)
    return $ Exp.Rec xs

  LF.ERecProj{recTypeCon=_,recField=fieldName,recExpr} -> do
    e <- simpExpr recExpr
    return $ Exp.Dot e fieldName

  LF.ERecUpd{} -> todo "ERecUpd"

  LF.EVariantCon{varVariant=name,varArg} -> do
    exp <- simpExpr varArg
    return $ Exp.Con (Value.mkTag name) [exp]

  LF.EEnumCon{enumDataCon=name} -> do
    return $ Exp.Con (Value.mkTag name) []

  LF.EStructCon{structFields} -> do
    xs <- forM structFields $ \(fieldName,expr) -> do
      e <- simpExpr expr
      return (fieldName,e)
    return $ Exp.Rec xs

  LF.EStructProj{structField=fieldName,structExpr} -> do
    e <- simpExpr structExpr
    return $ Exp.Dot e fieldName

  LF.EStructUpd{} -> todo "EStructUpd"

  LF.ETmApp{tmappFun=func,tmappArg=arg} -> do
    f <- simpExpr func
    a <- simpExpr arg
    return $ Exp.App f a

  LF.ETyApp{tyappExpr=expr,tyappType=typ} -> do
    expr <- simpExpr expr
    return $ Exp.TypeApp expr typ

  LF.ETmLam{tmlamBinder=(name,typ),tmlamBody} -> do
    body <- simpExpr tmlamBody
    return $ Exp.Lam name typ body

  LF.ETyLam{tylamBinder=(tv,_), tylamBody=expr} -> do
    expr <- simpExpr expr
    return $ Exp.TypeLam tv expr

  LF.ECase{casScrutinee, casAlternatives} -> do
    scrut <- simpExpr casScrutinee
    alts <- mapM simpAlternative casAlternatives
    return $ Exp.Match {scrut,alts}

  LF.ELet{letBinding,letBody} -> do
    f <- simpBinding letBinding
    body <- simpExpr letBody
    return $ f body

  LF.ENil{} ->
    return $ Exp.Con Value.nilTag []

  LF.ECons{consHead,consTail} -> do
    h <- simpExpr consHead
    t <- simpExpr consTail
    return $ Exp.Con Value.consTag [h,t]

  LF.ESome{someBody} -> do
    exp <- simpExpr someBody
    return $ Exp.Con Value.someTag [exp]

  LF.ENone{} ->
    return $ Exp.Con Value.noneTag []

  LF.EToAny{} -> todo "EToAny"
  LF.EFromAny{} -> todo "EFromAny"
  LF.ETypeRep{} -> todo "ETypeRep"
  LF.EUpdate{} -> todo "EUpdate"
  LF.EScenario{} -> todo "EScenario"

  LF.ELocation _sl expr -> do
    -- drop location info
    simpExpr expr

  where todo s = error $ "todo: simpExpr(" <> s <> "), " <> show expr

simpAlternative :: LF.CaseAlternative -> Effect Alt
simpAlternative = \case
  LF.CaseAlternative{altPattern,altExpr} -> do
    rhs <- simpExpr altExpr
    return $ simpPattern rhs altPattern

simpPattern :: Exp -> LF.CasePattern -> Alt
simpPattern rhs pat = case pat of
  LF.CPBool True -> Exp.Alt {tag = Value.trueTag, bound = [], rhs}
  LF.CPBool False -> Exp.Alt {tag = Value.falseTag, bound = [], rhs}
  LF.CPNil -> Exp.Alt {tag = Value.nilTag, bound = [], rhs}
  LF.CPCons{patHeadBinder=h,patTailBinder=t} -> Exp.Alt {tag = Value.consTag, bound = [h,t], rhs}
  LF.CPVariant{patVariant=name,patBinder=x} -> Exp.Alt {tag = Value.mkTag name, bound = [x], rhs}
  LF.CPUnit -> todo "CPUnit"
  LF.CPNone -> Exp.Alt {tag = Value.noneTag, bound = [], rhs}
  LF.CPSome{patBodyBinder=x} -> Exp.Alt {tag = Value.someTag, bound = [x], rhs}
  LF.CPEnum{} -> todo "CPEnum"
  LF.CPDefault -> todo "CPDefault"

  where todo s = error $ "todo: simpPattern(" <> s <> "), " <> show pat

simpBinding :: LF.Binding -> Effect (Exp -> Exp)
simpBinding = \case
  LF.Binding{bindingBinder=(name,_),bindingBound=rhs} -> do
    v <- simpExpr rhs
    return $ Exp.Let name v

simpQualifiedExprValName :: LF.Qualified LF.ExprValName -> Effect Exp
simpQualifiedExprValName q = do
  let LF.Qualified{qualPackage=pref, qualModule=moduleName, qualObject=name} = q
  mod <-
    case pref of
      LF.PRSelf -> GetTheModule
      LF.PRImport pid -> do
        pkg <- GetPackage pid
        return $ lookupModule moduleName pkg
  simpModuleValName mod name

lookupModule :: LF.ModuleName -> LF.Package -> LF.Module
lookupModule moduleName pkg = do
  let LF.Package{packageModules} = pkg
  case NM.lookup moduleName packageModules of
    Nothing -> error $ "lookupModule, " <> show moduleName
    Just mod -> mod

simpBuiltin :: LF.BuiltinExpr -> Value
simpBuiltin = \case

  LF.BEUnit -> Value.B0 Value.Unit
  LF.BEInt64 n -> Value.B0 (Value.Num n)
  LF.BEText t -> Value.B0 (Value.Text t)
  LF.BEBool b -> Value.bool b

  LF.BEFoldl -> Value.B3 Value.FOLDL
  LF.BEFoldr -> Value.B3 Value.FOLDR
  LF.BEAddInt64 -> Value.B2 Value.ADDI
  LF.BESubInt64 -> Value.B2 Value.SUBI
  LF.BEMulInt64 -> Value.B2 Value.MULI
  LF.BEModInt64 -> Value.B2 Value.MODI
  LF.BEExpInt64 -> Value.B2 Value.EXPI

  LF.BELess LF.BTInt64 -> Value.B2 Value.LESSI
  LF.BELessEq LF.BTInt64 -> Value.B2 Value.LESSEQI
  LF.BEGreater LF.BTInt64 -> Value.B2 Value.GREATERI
  LF.BEGreaterEq LF.BTInt64 -> Value.B2 Value.GREATEREQI
  LF.BEEqual LF.BTInt64 -> Value.B2 Value.EQUALI

  LF.BEError -> Value.B1 Value.ERROR

  be -> error $ "todo: simpBuiltin, " <> show be

instance Functor Effect where fmap = liftM
instance Applicative Effect where pure = return; (<*>) = ap
instance Monad Effect where return = Ret; (>>=) = Bind

data Effect a where
  Ret :: a -> Effect a
  Bind :: Effect a -> (a -> Effect b) -> Effect b
  GetPackage :: LF.PackageId -> Effect LF.Package
  GetTheModule :: Effect LF.Module
  Share :: Exp.DefKey -> Effect Exp -> Effect Int


runEffect :: [LF.ExternalPackage] -> LF.Module -> Effect Exp -> Prog
runEffect pkgs the_module e = do
  let state0 = (0,Map.empty)
  let (start,(_,m')) = run state0 e
  let defs = foldr (\(name,(i,exp)) m -> Map.insert i (name,exp) m) Map.empty (Map.toList m')
  Exp.Prog {defs,start}

  where
    run :: State -> Effect a -> (a,State)
    run state = \case
      Ret x -> (x,state)
      Bind e f -> do
        let (v1,state1) = run state e
        run state1 (f v1)
      GetPackage pid -> (getPackage pid, state)
      GetTheModule -> (the_module, state)
      Share name e -> do
        let (_,m) = state
        case Map.lookup name m of
          Just (i,_) -> do
            (i,state)
          Nothing -> do
            let (i,m) = state
            let state' = (i+1, Map.insert name (i,exp) m)
                (exp,state'') = run state' e
            (i,state'')

    packageMap = Map.fromList [ (pkgId,pkg) | LF.ExternalPackage pkgId pkg <- pkgs ]

    getPackage :: LF.PackageId -> LF.Package
    getPackage pid =
      case Map.lookup pid packageMap of
        Just v -> v
        Nothing -> error $ "getPackage, " <> show pid

type State = (Int, Map Exp.DefKey (Int,Exp))

