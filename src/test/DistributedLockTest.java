package test;

import java.util.concurrent.atomic.AtomicInteger;

import main.DistributedLock;
import main.DistributedLockImpl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

// This test expects that Redis is running locally with the default configuration. Start the redis
// server locally before running the tests.
public class DistributedLockTest {
  private JedisPool jedisPool;
  private Jedis jedis;

  @Before
  public void setup() {
    JedisPoolConfig config = new JedisPoolConfig();
    config.setTestOnBorrow(true);
    this.jedisPool = new JedisPool(config, "localhost", 6379);
    this.jedis = jedisPool.getResource();
  }

  @Test
  public void testAcquireRelease() throws Exception {
    DistributedLock dl1 = new DistributedLockImpl(jedis, "lock1");
    Assert.assertTrue(dl1.acquire(jedis));
    DistributedLockImpl dl2 = new DistributedLockImpl(jedis, "lock1", 1000, 60 * 1000);
    Assert.assertFalse(dl2.acquire(jedis));
    dl1.release(jedis);

    dl2 = new DistributedLockImpl(jedis, "lock1");
    Assert.assertTrue(dl2.acquire(jedis));
    dl2.release(jedis);
  }

  @Test
  public void testConcurrentAcquireRelease() throws InterruptedException {
    final int attempts = 10;
    final AtomicInteger acquiredCount1 = new AtomicInteger();
    final AtomicInteger acquiredCount2 = new AtomicInteger();

    Thread thread1 = new Thread(new Runnable() {
      public void run() {
        for (int i = 0; i < attempts; i++) {
          DistributedLock lock = new DistributedLockImpl(jedis, "lock", 15 * 1000, 200);
          try {
            if (lock.acquire(jedis)) {
              acquiredCount1.incrementAndGet();
              Thread.sleep(250);
              lock.release(jedis);
            }
          } catch (InterruptedException e) {
            System.err.println("Thread thread1 is interrupted");
            return;
          }
        }
      }
    });

    Thread thread2 = new Thread(new Runnable() {
      public void run() {
        for (int i = 0; i < attempts; i++) {
          DistributedLock lock = new DistributedLockImpl(jedis, "lock", 15 * 1000, 200);
          try {
            if (lock.acquire(jedis)) {
              acquiredCount2.incrementAndGet();
              Thread.sleep(250);
              lock.release(jedis);
            }
          } catch (InterruptedException e) {
            System.err.println("Thread thread2 is interrupted");
            return;
          }
        }
      }
    });

    thread1.start();
    thread2.start();

    thread1.join();
    thread2.join();

    Assert.assertEquals(attempts, acquiredCount1.get());
    Assert.assertEquals(attempts, acquiredCount1.get());
  }
}
