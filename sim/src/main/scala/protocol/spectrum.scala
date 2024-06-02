package sim.spectrum

import cats.Monad
import cats.effect.IO
import cats.effect.kernel.{Async, Clock}
import cats.effect.std.{AtomicCell, Queue}
import cats.syntax.applicative.*
import cats.syntax.flatMap.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import sim.*
import sim.env.*

import scala.concurrent.duration.*
import scala.language.postfixOps


sealed trait SpectrumMsg
case class NewBlock(block: Block) extends SpectrumMsg
case class GetBlock(blockId: BlockId)  extends SpectrumMsg
case class Register(newAcc: Account)  extends SpectrumMsg


final class Spectrum[F[_]: Monad](
                                   selfAddr: Addr,
                                   // abstract interface to Ledger, so you dont have to care about impl.
                                   ledger: Ledger[F],
                                   // queue of pending output commands waiting to be returned from poll().
                                   pendingOutputs: Queue[F, (Addr, AgentOut[SpectrumMsg])]
                                 ) extends Agent[SpectrumMsg, F]:
  def getAddr: F[Addr] = selfAddr.pure

  def injectMessage(srcAddr: Addr, m: SpectrumMsg): F[Unit] =
    m match
      case NewBlock(block) => ledger.add(block)
      case Register(account) => ledger.addParticipant(account)
      case GetBlock(blockId) =>
        for
          maybeBlock <- ledger.get(blockId)
          _ <- maybeBlock match
            case None =>
              // do nothing
              ().pure
            case Some(block) =>
              // if the requested block is found locally we send it to the peer which requested it
              pendingOutputs.offer(srcAddr -> SendMessage(NewBlock(block)))
        yield ()

  def injectConnReq(peerAddr: Addr, handshake: SpectrumMsg): F[Either[Reject, SpectrumMsg]] = ???

  def injectConnLost(peerAddr: Addr): F[Unit] = ???

  def injectConnEstablished(peerAddr: Addr, handshake: SpectrumMsg): F[Unit] = ???

  def poll: F[Option[(Addr, AgentOut[SpectrumMsg])]] =
    pendingOutputs.tryTake