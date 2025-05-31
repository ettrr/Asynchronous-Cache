package cache

import cats.Monad
import cats.effect.Ref
import cats.effect.std.Console

import scala.concurrent.duration.FiniteDuration
import scala.collection.immutable.HashMap
import cats.syntax.all._

trait Cache[F[_], K, V] {
  def put(key: K, value: V): F[Unit]
  def get(key: K): F[Option[V]]
}

case class SyncCache[F[_]: Ref.Make: Monad: Console, K, V, E](
    maxSize: Long,
    ttl: FiniteDuration,
    rttl: FiniteDuration, // refreshAfterWriterTtl
    load: K => F[Either[E, V]],
    private val storage: Storage[F, K, V]
) extends Cache[F, K, V] {

  override def put(key: K, value: V): F[Unit] =
    Console[F].println(s"Inserting: ($key, $value)") >> storage.put(key, value)

  override def get(key: K): F[Option[V]] = {
    val getTime = System.currentTimeMillis()
    for {
      pair <- storage.get(key)
      vLoad <- pair match {
        case Some((v, time)) =>
          if (getTime - time > ttl.toMillis) syncLoadValueAndReturn(key)
          else if (getTime - time > rttl.toMillis) syncLoadValue(key) >> Monad[F].pure(Right(v))
          else storage.put(key, v) >> Monad[F].pure(Right(v))
        case None => load(key)
      }
      vOption = vLoad match {
        case Right(v) => Some(v)
        case Left(_)  => None
      }
    } yield vOption
  }

  private def syncLoadValueAndReturn(key: K): F[Either[E, V]] = {
    for {
      newVal <- load(key)
      _ <- newVal match {
        case Right(v) => storage.put(key, v)
        case Left(_)  => Monad[F].unit
      }
    } yield newVal
  }

  private def syncLoadValue(key: K): F[Unit] = {
    for {
      newVal <- load(key)
      _ <- newVal match {
        case Right(v) => storage.put(key, v)
        case Left(_)  => Monad[F].unit
      }
    } yield ()
  }
}

object SyncCache {
  def apply[F[_]: Ref.Make: Monad: Console, K, V, E](
      maxSize: Long,
      ttl: FiniteDuration,
      rttl: FiniteDuration,
      load: K => F[Either[E, V]]
  ): F[SyncCache[F, K, V, E]] = {
    for {
      data      <- Ref.of[F, HashMap[K, (V, Long)]](HashMap.empty)
      timeStamp <- Ref.of[F, HashMap[Long, K]](HashMap.empty)
      storage = Storage(maxSize, data, timeStamp)
    } yield new SyncCache(maxSize, ttl, rttl, load, storage)
  }
}
