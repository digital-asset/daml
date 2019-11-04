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
import DA.Daml.LF.Optimize (World(..))
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Evaluator.Exp as Exp
import qualified DA.Daml.LF.Evaluator.Value as Value

simplify :: World -> LF.ModuleName -> LF.ExprValName -> Prog
simplify world@World{mainIdM} moduleName name = do
  case mainIdM of
    Nothing -> error "simplify, mainIdM = Nothing"
    Just mainId -> do
      runEffect mainId world $ do
        simpExprValName mainId moduleName name >>= \case
          Just exp -> return exp
          Nothing -> Fail "simplify, Nothing"

simpExprValName :: LF.PackageId -> LF.ModuleName -> LF.ExprValName -> Effect (Maybe Exp)
simpExprValName pid moduleName name = do
  getModule pid moduleName >>= \case
    Nothing -> return Nothing
    Just mod -> do
      let LF.Module{moduleValues} = mod
      case NM.lookup name moduleValues of
        Nothing -> Fail $ "simpExprValName, " <> show name
        Just dval -> do
          let LF.DefValue{dvalBody=expr} = dval
          let key = Exp.DefKey (pid,moduleName,name)
          i <- Share key $ simpExpr expr
          return $ Just $ Exp.Ref i

getModule :: LF.PackageId -> LF.ModuleName -> Effect (Maybe LF.Module)
getModule pid moduleName = do
  package <- GetPackage pid
  let LF.Package{packageModules} = package
  case NM.lookup moduleName packageModules of
    Nothing -> return Nothing -- Fail $ "simp-getModule, " <> show (pid,moduleName)
    Just mod -> return $ Just mod

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

  where todo s = Fail $ "todo: simpExpr(" <> s <> "), " <> show expr

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
simpQualifiedExprValName q = do -- trace (show q) $ do
  let LF.Qualified{qualPackage=pref, qualModule=moduleName, qualObject=name} = q
  pid <- case pref of
    LF.PRSelf -> GetPid
    LF.PRImport pid -> return pid
  WithPid pid $ do
    simpExprValName pid moduleName name >>= \case
      Just exp -> return exp
      Nothing -> Fail $ "simpQualifiedExprValName, pid = " <> show pid <> ", q = " <> show q


simpBuiltin :: LF.BuiltinExpr -> Value
simpBuiltin = \case

  LF.BEUnit -> Value.B0 Value.Unit
  LF.BEInt64 n -> Value.B0 (Value.Num n)
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

  be -> error $ "todo: simpBuiltin, " <> show be

instance Functor Effect where fmap = liftM
instance Applicative Effect where pure = return; (<*>) = ap
instance Monad Effect where return = Ret; (>>=) = Bind

data Effect a where
  Ret :: a -> Effect a
  Bind :: Effect a -> (a -> Effect b) -> Effect b
  Fail :: String -> Effect a
  GetPackage :: LF.PackageId -> Effect LF.Package
  GetPid :: Effect LF.PackageId
  WithPid :: LF.PackageId -> Effect a -> Effect a
  Share :: Exp.DefKey -> Effect Exp -> Effect Int


runEffect :: LF.PackageId -> World -> Effect Exp -> Prog
runEffect mainId World{packageMap} e = do
  let state0 = (0,Map.empty)
  let (main,(_,m')) = run mainId state0 e
  let defs = foldr (\(name,(i,exp)) m -> Map.insert i (name,exp) m) Map.empty (Map.toList m')
  Exp.Prog {defs,main}

  where
    run :: LF.PackageId -> State -> Effect a -> (a,State)
    run pid state = \case
      Fail mes -> error $ "Simp.Fail, " <> mes
      Ret x -> (x,state)
      Bind e f -> do
        let (v1,state1) = run pid state e
        run pid state1 (f v1)
      GetPackage pid -> (getPackage pid, state)
      GetPid -> (pid,state)
      WithPid pid e -> run pid state e
      Share name e -> do
        let (_,m) = state
        case Map.lookup name m of
          Just (i,_) -> do
            (i,state)
          Nothing -> do
            let (i,m) = state
            let state' = (i+1, Map.insert name (i,exp) m)
                (exp,state'') = run pid state' e
            (i,state'')

    getPackage k =
      case Map.lookup k packageMap of
        Just v -> v
        Nothing -> error $ "getPackage, " <> show k

type State = (Int, Map Exp.DefKey (Int,Exp))

