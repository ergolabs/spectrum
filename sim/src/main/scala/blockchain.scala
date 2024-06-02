package sim

import cats.Monad
import cats.effect.std.Random
import cats.effect.{Clock, Sync, Temporal}
import cats.mtl.{Ask, Stateful}
import cats.syntax.flatMap.*
import cats.syntax.functor.*
import fs2.Chunk.Queue
import fs2.Stream

import scala.concurrent.duration.*

import sim.env.*

/** L1 robust ledger system.
 */
trait Blockchain[F[_]]:
  def pullNextUpdate: F[Block]
  def submitTx(tx: Transaction): F[Unit]

object LedgerSim:
  case class SimConfig(
      accountPool: Vector[Account],
      maxBlockCapacity: Int,
      meanBlockTimeMillis: Long
  )
  case class SimState(
      mempool: Vector[Transaction],
      lastUpdateAt: Long,
      lastBlockSeqNum: Int
  )

  def make[F[_]: Monad](
      conf: SimConfig,
      rnd: Random[F]
  )(using
      F: Stateful[F, SimState],
      C: Clock[F],
      T: Temporal[F]
  ): Blockchain[F] =
    new Blockchain[F]:
      override def pullNextUpdate: F[Block] =
        for
          st      <- F.get
          now     <- C.realTime.map(_.toMillis)
          waitFor <- rnd.nextLongBounded((now - st.lastUpdateAt) * 2)
          _       <- T.sleep(waitFor.millis)
          block   <- genNextBlock(conf, st, rnd)
          _ <- F.modify(st =>
            st.copy(
              mempool = st.mempool.filterNot(block.txs.contains),
              lastUpdateAt = now + waitFor,
              lastBlockSeqNum = block.seqNum
            )
          )
        yield block

      override def submitTx(tx: Transaction): F[Unit] =
        F.modify(st => st.copy(mempool = tx +: st.mempool))

  private def genNextBlock[F[_]: Monad](
      conf: SimConfig,
      st: SimState,
      rnd: Random[F]
  ): F[Block] =
    for
      n <- rnd.nextIntBounded(conf.maxBlockCapacity)
      blockTxs   = st.mempool.takeRight(n)
      nextSeqNum = st.lastBlockSeqNum + 1
    yield Block(blockTxs, nextSeqNum)
