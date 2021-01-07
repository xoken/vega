module Network.Xoken.Node.Exception where

import Control.Exception

data BlockSyncException
    = BlocksNotChainedException
    | MessageParsingException
    | KeyValueDBInsertException
    | BlockHashNotFoundException
    | DuplicateBlockHeaderException
    | InvalidMessageTypeException
    | InvalidBlocksException
    | EmptyHeadersMessageException
    | InvalidStreamStateException
    | InvalidBlockIngestStateException
    | InvalidMetaDataException
    | InvalidBlockHashException
    | UnexpectedDuringBlockProcException String
    | InvalidBlockSyncStatusMapException
    | InvalidBlockInfoException
    | OutpointAddressNotFoundException
    | InvalidAddressException
    | TxIDNotFoundException
    | InvalidOutpointException
    | DBTxParseException
    | MerkleTreeComputeException
    | InvalidCreateListException
    | MerkleSubTreeAlreadyExistsException
    | MerkleSubTreeDBInsertException
    | ResourcePoolFetchException
    | DBInsertTimeOutException
    | MerkleTreeInvalidException
    | MerkleQueueNotFoundException
    | BlockAlreadySyncedException
    | OutputAlreadySpentException
    | InvalidTxSatsValueException
    | InvalidDAGEdgeException
    | ZUnknownHandler
    | ColumnFamilyNotFoundException
    | ZInvalidColumnFamily
    deriving (Show, Read)

instance Exception BlockSyncException

data PeerMessageException
    = SocketReadException
    | SocketConnectException SockAddr
    | DeflatedBlockParseException
    | ConfirmedTxParseException
    | PeerSocketNotConnectedException
    | ZeroLengthSocketReadException
    | NetworkMagicMismatchException
    | UnresponsivePeerException
    deriving (Show)

instance Exception PeerMessageException

data AriviServiceException
    = KeyValueDBLookupException
    | GraphDBLookupException
    | InvalidOutputAddressException
    deriving (Show)

instance Exception AriviServiceException

data WorkerRemoteException =
    WorkerConnectionRejectException
    deriving (Show, Read)

instance Exception WorkerRemoteException