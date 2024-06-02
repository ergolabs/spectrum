import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.show.*

import scala.concurrent.duration.*

import sim.*

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    for
      swarm <- Swarm.make[PingMessage, IO]
      a1    <- Ping.make[IO](Addr(0), Set(Addr(1)), 2.seconds)
      a2    <- Ping.make[IO](Addr(1), Set(), 3.seconds)
      _     <- swarm.spawn(a1)
      _     <- swarm.spawn(a2)
      _ <- swarm.stats
        .flatMap(s => IO.delay(println(s"Network stats: ${s.show}")) >> IO.sleep(5.seconds))
        .foreverM
    yield ExitCode.Success
}
