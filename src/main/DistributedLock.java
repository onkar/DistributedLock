package main;

import redis.clients.jedis.Jedis;

public interface DistributedLock {
  public boolean acquire(Jedis jedis) throws InterruptedException;

  public void release(Jedis jedis);
}
