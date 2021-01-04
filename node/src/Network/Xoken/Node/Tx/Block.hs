module Network.Xoken.Node.Validation.Block where

{-

    Reject if duplicate of block we have in any of the three categories
    For each transaction, apply "tx" checks 2-4
    --For the coinbase (first) transaction, scriptSig length must be 2-100
    --Reject if sum of transaction sig opcounts > MAX_BLOCK_SIGOPS
    Check if prev block (matching prev hash) is in main branch or side branches. If not, add this to orphan blocks, then query peer we got this from for 1st missing orphan block in prev chain; done with block
    Check that nBits value matches the difficulty rules
    Reject if timestamp is the median time of the last 11 blocks or before
    For certain old blocks (i.e. on initial block download) check that hash matches known values
    Add block into the tree. There are three cases: 1. block further extends the main branch; 2. block extends a side branch but does not add enough difficulty to make it become the new main branch; 3. block extends a side branch and makes it the new main branch.
    For case 1, adding to main branch:
        For all but the coinbase transaction, apply the following:
            For each input, look in the main branch to find the referenced output transaction. Reject if the output transaction is missing for any input.
            For each input, if we are using the nth output of the earlier transaction, but it has fewer than n+1 outputs, reject.
            For each input, if the referenced output transaction is coinbase (i.e. only 1 input, with hash=0, n=-1), it must have at least COINBASE_MATURITY (100) confirmations; else reject.
            Verify crypto signatures for each input; reject if any are bad
            For each input, if the referenced output has already been spent by a transaction in the main branch, reject
            Using the referenced output transactions to get input values, check that each input value, as well as the sum, are in legal money range
            Reject if the sum of input values < sum of output values
        Reject if coinbase value > sum of block creation fee and transaction fees
        (If we have not rejected):
        For each transaction, "Add to wallet if mine"
        For each transaction in the block, delete any matching transaction from the transaction pool
        If we rejected, the block is not counted as part of the main branch
    For case 3, a side branch becoming the main branch:
        Find the fork block on the main branch which this side branch forks off of
        Redefine the main branch to only go up to this fork block
        For each block on the side branch, from the child of the fork block to the leaf, add to the main branch:
            Do "branch" checks 3-11
            For all but the coinbase transaction, apply the following:
                For each input, look in the main branch to find the referenced output transaction. Reject if the output transaction is missing for any input.
                For each input, if we are using the nth output of the earlier transaction, but it has fewer than n+1 outputs, reject.
                For each input, if the referenced output transaction is coinbase (i.e. only 1 input, with hash=0, n=-1), it must have at least COINBASE_MATURITY (100) confirmations; else reject.
                Verify crypto signatures for each input; reject if any are bad
                For each input, if the referenced output has already been spent by a transaction in the main branch, reject
                Using the referenced output transactions to get input values, check that each input value, as well as the sum, are in legal money range
                Reject if the sum of input values < sum of output values
            Reject if coinbase value > sum of block creation fee and transaction fees
            (If we have not rejected):
            For each transaction, "Add to wallet if mine"
        If we reject at any point, leave the main branch as what it was originally, done with block
        For each block in the old main branch, from the leaf down to the child of the fork block:
            For each non-coinbase transaction in the block:
                Apply "tx" checks 2-9, except in step 8, only look in the transaction pool for duplicates, not the main branch
                Add to transaction pool if accepted, else go on to next transaction
        For each block in the new main branch, from the child of the fork node to the leaf:
            For each transaction in the block, delete any matching transaction from the transaction pool
    For each orphan block for which this block is its prev, run all these steps (including this one) recursively on that orphan

-}

-- Transaction list must be non-empty
nonEmptyTxList :: [Tx] -> Bool
nonEmptyTxList [] = False
nonEmptyTxList _ = True

-- Block hash must satisfy claimed nBits proof of work
-- isValidPOW

-- Block timestamp must not be more than two hours in the future
validTimestamp :: BlockHeader -> IO Bool
validTimestamp hdr = do
    ct <- getPOSIXTime
    return $ blockTimestamp hdr < (fromIntegral $ ct + 7200)

-- First transaction must be coinbase (i.e. only 1 input, with hash=0, n=-1), the rest must not be
coinbaseCheck :: [Tx] -> Bool
coinbaseCheck (tx:txs) = isCoinbase tx && (not $ any (isCoinbase) txs)
coinbaseCheck [] = False

-- Verify Merkle hash
verifyMerkleHash :: Hash256 -> [Tx] -> Bool
verifyMerkleHash mh = (== mh) . buildMerkleRoot . fmap txHash 