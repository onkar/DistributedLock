package main;

import redis.clients.jedis.Jedis;

public class DistributedLockImpl implements DistributedLock {
  private String key;

  private long timeoutInMiliseconds = 30 * 1000;
  private long expiryInMiliseconds = 60 * 1000;

  private long lockExpiry;

  public DistributedLockImpl(Jedis jedis, String key) {
    this.key = key;
    this.lockExpiry = 0;
  }

  public DistributedLockImpl(Jedis jedis, String key, int timeoutInMiliseconds,
      int expiryInMiliseconds) {
    this(jedis, key);
    this.timeoutInMiliseconds = timeoutInMiliseconds;
    this.expiryInMiliseconds = expiryInMiliseconds;
  }

  public synchronized boolean acquire(Jedis jedis) throws InterruptedException {
    long timeout = this.timeoutInMiliseconds;
    while (timeout >= 0) {
      long expiresAt = System.currentTimeMillis() + expiryInMiliseconds + 1;
      String expiresAtString = String.valueOf(expiresAt);
      if (jedis.setnx(key, expiresAtString) == 1) {
        // If lock acquisition is possible, tell others that the lock is locked and return
        lockExpiry = expiresAt;
        return true;
      }
      // If lock cannot be acquired, get the time when lock is going to be expired.
      String expectedExpiryTime = jedis.get(key);
      if (expectedExpiryTime != null
          && System.currentTimeMillis() > Long.parseLong(expectedExpiryTime)) {
        // Current lock is expired. Atomically try to set the value to expiresAt
        String actualExpiryTime = jedis.getSet(key, String.valueOf(expiresAt));
        if (actualExpiryTime != null && actualExpiryTime.equals(expectedExpiryTime)) {
          lockExpiry = expiresAt;
          return true;
        }
      }
      // Otherwise, decrement the timeout counter and sleep
      timeout -= 100;
      Thread.sleep(100);
    }
    return false;
  }

  public synchronized void release(Jedis jedis) {
    long value = Long.parseLong(jedis.get(key));
    if (value == lockExpiry) {
      // Delete the key only when value matches to what you have set initially
      jedis.del(key);
      lockExpiry = 0;
    }
  }
}
