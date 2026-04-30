// lib/redis.js — Redis client (singleton)
import { createClient } from 'redis';

let client = null;

export async function getRedis() {
  if (!client) {
    client = createClient({ url: process.env.REDIS_URL });
    client.on('error', (err) => console.error('Redis error:', err));
    await client.connect();
  }
  return client;
}

// Get parsed JSON from Redis
export async function redisGet(key) {
  try {
    const redis = await getRedis();
    const raw   = await redis.get(key);
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null; // never break the app on Redis failure
  }
}

// Set JSON in Redis with 30 min TTL
export async function redisSet(key, value) {
  try {
    const redis = await getRedis();
    await redis.set(key, JSON.stringify(value), { EX: 1800 }); // 1800s = 30 min
  } catch {}
}

// Key builders
export const redisKey = {
  movie  : (tmdbId)                  => `movie:${tmdbId}`,
  tvMeta : (tmdbId)                  => `tv:meta:${tmdbId}`,
  episode: (tmdbId, season, episode) => `tv:ep:${tmdbId}:${season}:${episode}`,
};
