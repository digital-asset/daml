
module Util(topSort, transitive) where

import Data.Tuple.Extra
import Data.List
import Data.Maybe


topSort :: (Show k, Eq k) => [(k, [k], v)] -> [v]
topSort [] = []
topSort xs
    | null now = error $ "topSort failed to order things\n" ++ unlines (map (\(a,b,_) -> show (a,b)) xs)
    | otherwise = map thd3 now ++ topSort [(k, ks \\ map fst3 now, v) | (k, ks, v) <- later]
    where (now,later) = partition (null . snd3) xs


transitive :: Ord k => [(k, [k])] -> k -> [k]
transitive xs k = f [] [k]
    where
        f seen [] = seen
        f seen (t:odo)
            | t `elem` seen = f seen odo
            | otherwise = f (t:seen) (fromMaybe [] (lookup t xs) ++ odo)
