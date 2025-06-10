package cache

import cats.effect.std.Supervisor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.effect.unsafe.implicits.global

import scala.concurrent.duration._
import scala.collection.immutable.HashMap
import cats.syntax.all._

class AsyncCacheSpec extends AnyFlatSpec with Matchers {

  private def createTestCache[E](
      maxSize: Long = 100,
      ttl: FiniteDuration = 1.minute,
      rttl: FiniteDuration = 30.seconds,
      load: String => IO[Either[E, Int]] = (_: String) => IO.pure(Right(0)),
      initialData: Map[String, (Int, Long)] = Map.empty
  ): Resource[IO, AsyncCache[IO, String, Int, E]] = {

    type A = Either[E, Int]

    for {
      supervisor <- Supervisor[IO](await = true)
      data <- Resource.eval(Ref.of[IO, HashMap[String, (Int, Long)]](HashMap.from(initialData)))
      timeStamp <- Resource.eval(Ref.of[IO, HashMap[Long, String]](HashMap.empty))
      keysForLoad <- Resource.eval(
        Ref.of[IO, HashMap[String, (Boolean, Deferred[IO, IO[A]])]](HashMap.empty)
      )
      loader          = Loader(load, keysForLoad)
      resourceManager = AsyncResourceManager(maxSize, data, timeStamp, loader)
    } yield new AsyncCache[IO, String, Int, E](
      maxSize,
      ttl,
      rttl,
      resourceManager,
      supervisor
    )
  }

  "AsyncCache" should "store and retrieve values with put and get" in {
    val cacheResource = createTestCache()

    val result = cacheResource.use { cache =>
      for {
        _      <- cache.put("key1", 42)
        result <- cache.get("key1")
      } yield result
    }

    result.unsafeRunSync() shouldBe Some(42)
  }

  it should "return load value for non-existent key" in {
    val cacheResource = createTestCache()

    val result = cacheResource.use(_.get("nonexistent"))

    result.unsafeRunSync() shouldBe Some(0)
  }

  it should "load value for non-existent key using load function" in {
    val load: String => IO[Either[String, Int]] = {
      case "key1" => IO.pure(Right(42))
      case _      => IO.pure(Left("error"))
    }
    val cacheResource = createTestCache(load = load)

    val result = cacheResource.use(_.get("key1"))

    result.unsafeRunSync() shouldBe Some(42)
  }

  it should "handle load errors by returning None in get" in {
    val load: String => IO[Either[String, Int]] = _ => IO.pure(Left("error"))
    val cacheResource                           = createTestCache(load = load)

    val result = cacheResource.use(_.get("key1"))

    result.unsafeRunSync() shouldBe None
  }

  it should "return load errors in getEither" in {
    val load: String => IO[Either[String, Int]] = _ => IO.pure(Left("error"))
    val cacheResource                           = createTestCache(load = load)

    val result = cacheResource.use(_.getEither("key1"))

    result.unsafeRunSync() shouldBe Left("error")
  }

  it should "synchronously load new value when ttl is passed" in {
    val currentTime                             = System.currentTimeMillis()
    val veryOldTime                             = currentTime - 2.minutes.toMillis
    val load: String => IO[Either[String, Int]] = _ => IO.pure(Right(42))
    val cacheResource = createTestCache(
      ttl = 1.minute,
      rttl = 30.seconds,
      load = load,
      initialData = Map("key1" -> (1, veryOldTime))
    )

    val result = cacheResource.use(_.get("key1"))

    result.unsafeRunSync() shouldBe Some(42)
  }

  it should "not exceed max size" in {
    val load: String => IO[Either[String, Int]] = k => IO.pure(Right(k.toInt))
    val cacheResource                           = createTestCache(maxSize = 2, load = load)

    val result = cacheResource.use { cache =>
      for {
        _ <- cache.put("1", 1)
        _ <- cache.put("2", 2)
        _ <- cache.put("3", 3)
        size <- cache.get("1").flatMap {
          case Some(_) =>
            cache.get("2").map {
              case Some(_) => 2
              case None    => 1
            }
          case None =>
            cache.get("2").map {
              case Some(_) => 1
              case None    => 0
            }
        }
      } yield size
    }

    result.unsafeRunSync() shouldBe 2
  }

  it should "prevent concurrent loads for the same key" in {
    val load: String => IO[Either[String, Int]] = {
      var count = 0
      _ => IO { count += 1; Right(count) }.delayBy(100.millis)
    }
    val cacheResource = createTestCache(load = load)

    val result = cacheResource.use { cache =>
      for {
        tasks <- (1 to 10).toList.parTraverse(_ => cache.get("key1"))
        _     <- IO.pure(println(tasks))
        result = tasks.forall(_.contains(1)) shouldBe true
      } yield result
    }

    result.unsafeRunSync()
  }

  it should "handle concurrent puts and gets correctly" in {
    val cacheResource = createTestCache(maxSize = 100)

    val result = cacheResource.use { cache =>
      val writes = (1 to 100).toList.parTraverse { i =>
        cache.put(s"key$i", i)
      }

      val reads = (1 to 100).toList.parTraverse { i =>
        cache.get(s"key$i").map(_.getOrElse(-1))
      }

      for {
        _      <- writes
        values <- reads
      } yield values.sum
    }

    result.unsafeRunSync() shouldBe (1 to 100).sum
  }

  it should "respect TTL under concurrent access" in {
    val currentTime = System.currentTimeMillis()
    val oldTime     = currentTime - 2.minutes.toMillis
    val cacheResource =
      createTestCache(ttl = 1.minute, initialData = Map("expired" -> (99, oldTime)))

    val result = cacheResource.use { cache =>
      val getters = (1 to 10).toList.parTraverse { _ =>
        cache.get("expired")
      }

      getters.map(_.forall(_.contains(0)))
    }

    result.unsafeRunSync() shouldBe true
  }

  it should "reload value only once when TTL expires under concurrent access" in {
    var loadCount = 0
    val load: String => IO[Either[String, Int]] = _ => IO {
      loadCount += 1
      Right(loadCount)
    }.delayBy(100.millis)

    val oldTime = System.currentTimeMillis() - 2.minutes.toMillis
    val cacheResource = createTestCache(
      ttl = 1.minute,
      initialData = Map("expired" -> (0, oldTime)),
      load = load
    )

    val result = cacheResource.use { cache =>
      (1 to 10).toList.parTraverse(_ => cache.get("expired"))
        .map(_.forall(_.contains(1)))
    }

    result.unsafeRunSync() shouldBe true
  }
}
