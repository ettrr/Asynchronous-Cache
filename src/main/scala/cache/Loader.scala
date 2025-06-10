package cache

import cats.Monad
import cats.effect.std.Console
import cats.effect.{Async, Deferred, Ref}
import cats.syntax.all._

import scala.collection.immutable.HashMap

// A = Either[E, V]
case class Loader[F[_]: Async: Console, K, A](
    private val load: K => F[A],
    private val keysForLoad: Ref[F, HashMap[K, (Boolean, Deferred[F, F[A]])]]
) {

  def asyncLoad(key: K): F[A] = {
    for {
      newDeferred <- Deferred[F, F[A]]
      deferred <- keysForLoad.modify { map =>
        map.get(key) match {
          case Some((isOld, deferred)) =>
            if (isOld) (map + (key -> (false, newDeferred)), Right(newDeferred))
            else (map, Left(deferred))
          case None => (map + (key -> (false, newDeferred)), Right(newDeferred))
        }
      }

      result <- deferred match {
        case Left(d) => d.get.flatten
        case Right(d) =>
          for {
            value  <- load(key)
            _      <- d.complete(Monad[F].pure(value))
            _      <- keysForLoad.update(_.updated(key, (true, d)))
            result <- d.get.flatten
          } yield result
      }
    } yield result
  }

  def deleteKey(key: K): F[Unit] =
    keysForLoad.update(map => map - key)
}
