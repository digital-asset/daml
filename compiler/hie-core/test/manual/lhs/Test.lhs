
\subsection{Testing LHS}

\begin{code}
{-# LANGUAGE CPP #-}

module Test
    (
      main
    ) where

import Bird
\end{code}


our main procedure

\begin{code}

main :: IO ()
main = do
  putStrLn "hello world."
  fly

\end{code}

