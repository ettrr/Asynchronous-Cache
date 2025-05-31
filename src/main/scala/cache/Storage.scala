package cache

import cats.Monad
import cats.effect.std.Console
import cats.effect.Ref
import cats.syntax.all._

import scala.collection.immutable.HashMap

case class Storage[F[_]: Ref.Make: Monad: Console, K, V](
    maxSize: Long,
    storage: Ref[F, HashMap[K, (V, Long)]],
    timeStamp: Ref[F, HashMap[Long, K]]
) {

  private val context = Monad[F]
  private val console = Console[F]

  def put(key: K, value: V): F[Unit] = {
    val timeAdd = System.currentTimeMillis()
    for {
      data   <- storage.get
      time   <- timeStamp.get
      inside <- contains(key)
      _ <-
        if (data.size == maxSize) {
          val minTime = time.keySet.min
          time.get(minTime) match {
            case Some(kMT) =>
              deleteFromTimeStamp(key, minTime) >> deleteFromRef(storage, if (inside) key else kMT)
            case None => context.unit
          }
        } else context.unit
      _ <- putInRef(storage, key, (value, timeAdd)) >> putInRef(timeStamp, timeAdd, key)
    } yield ()
  }

  private def putInRef[T, D](ref: Ref[F, HashMap[T, D]], key: T, value: D): F[Unit] = {
    ref.update(map => map + (key -> value))
  }

  private def deleteFromRef[T, D](ref: Ref[F, HashMap[T, D]], key: T): F[Unit] =
    ref.update(map => map - key)

  private def deleteFromTimeStamp(key: K, minTime: Long): F[Unit] = {
    for {
      inside <- contains(key)
      time <-
        if (inside) get(key).map(_.get._2)
        else context.pure(minTime)
      _ <- deleteFromRef(timeStamp, time)
    } yield ()
  }

  def get(key: K): F[Option[(V, Long)]] = storage.get.map(_.get(key))

  private def contains(key: K): F[Boolean] = storage.get.map(_.contains(key))

  def printStorage: F[Unit] = {
    for {
      data <- storage.get
      time <- timeStamp.get
      _    <- Console[F].println(data)
      _    <- Console[F].println(time)
    } yield ()
  }
}
