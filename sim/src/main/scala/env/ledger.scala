package sim.env
import cats.effect.{ExitCode, IO, IOApp}

case class Address(v: String)

case class VrfPublicKey(value: String)  // HexString
case class KesPublicKey(value: String)  // HexString
case class DsigPublicKey(value: String) // HexString

case class Account(
    address: Address,
    stake: Long,
    vrfPk: VrfPublicKey,
    kesPk: KesPublicKey,
    dsigPk: DsigPublicKey
)

case class Transaction(id: Long, account: Account, change: Long)

case class BlockId(value: Long)

case class Block(id: BlockId, seqNum: Int)

case class State(verAccounts: Vector[Account]) // set of verification keys to store in the Ledger

trait Ledger[F[_]]:
  /** Add a new block to the ledger.
    */
  def add(block: Block): F[Unit]

  /** Get all blocks at the given height.
    */
  def getAll(height: Int): F[List[Block]]

  /** Get specific block.
    */
  def get(id: BlockId): F[Option[Block]]
  
  def addParticipant(acc: Account): F[Unit]
