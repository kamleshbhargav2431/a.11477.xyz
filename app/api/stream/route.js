// app/api/stream/route.js
// Exact same logic as original PHP A11 index.php
// GET /api/stream?tmdb_id=123&type=movie
// GET /api/stream?tmdb_id=25&type=tv&season=1&episode=1

import { getPool } from '@/lib/db';
import { NextResponse } from 'next/server';
import { HttpsProxyAgent } from 'https-proxy-agent';

const TMDB_API_KEY = process.env.TMDB_API_KEY;
const PROXY_HOST   = process.env.PROXY_HOST;
const PROXY_PORT   = process.env.PROXY_PORT;
const PROXY_USER   = process.env.PROXY_USER;
const PROXY_PASS   = process.env.PROXY_PASS;

const PROXY_URL = `http://${PROXY_USER}:${PROXY_PASS}@${PROXY_HOST}:${PROXY_PORT}`;

// ============================================================
//  PROXY FETCH
// ============================================================
async function proxyFetch(url) {
  try {
    const agent = new HttpsProxyAgent(PROXY_URL);
    const res   = await fetch(url, {
      agent,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Accept'    : 'application/json, text/html',
      },
      redirect : 'follow',
      signal   : AbortSignal.timeout(20000),
    });
    return res.ok ? res.text() : null;
  } catch {
    return null;
  }
}

// ============================================================
//  PROXY INFO
// ============================================================
async function getProxyInfo() {
  try {
    const agent = new HttpsProxyAgent(PROXY_URL);
    const res   = await fetch('https://httpbin.org/ip', {
      agent,
      signal: AbortSignal.timeout(8000),
    });
    if (!res.ok) throw new Error('failed');
    const data = await res.json();
    return {
      enabled : true,
      provider: 'Webshare Rotating',
      method  : 'webshare_rotating_proxy',
      endpoint: `${PROXY_HOST}:${PROXY_PORT}`,
      ip      : data.origin ?? null,
      note    : 'Each request uses a different IP address',
    };
  } catch (e) {
    return {
      enabled : false,
      provider: 'Webshare Rotating',
      method  : 'direct_fallback',
      endpoint: `${PROXY_HOST}:${PROXY_PORT}`,
      ip      : null,
      note    : 'Proxy failed, using direct connection. Error: ' + e.message,
    };
  }
}

// ============================================================
//  HELPERS
// ============================================================
function formatSize(bytes) {
  if (!bytes || bytes <= 0) return null;
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i     = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(2)} ${units[i]}`;
}

function encodePathSegments(path) {
  return path.split('/').map(seg => encodeURIComponent(decodeURIComponent(seg))).join('/');
}

function buildLink(item) {
  let path = item.path;
  if (!path.startsWith('/')) path = '/' + path;
  return 'https://a.111477.xyz' + encodePathSegments(path);
}

function normalizeTitle(t) {
  return t.toLowerCase()
    .replace(/[:\-–—]/g, ' ')
    .replace(/[^a-z0-9 ]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function scoreDir(dirName, normTitle, year) {
  const norm     = normalizeTitle(dirName);
  const hasYear  = year && norm.includes(year);
  const hasTitle = norm.includes(normTitle) || normTitle.includes(norm.replace(/\d{4}/g, '').trim());
  if (hasTitle && hasYear) return 3;
  if (hasTitle) return 1;
  return 0;
}

// ============================================================
//  DB HELPERS
// ============================================================
async function dbFindMovie(tmdbId) {
  const pool = getPool();
  const [rows] = await pool.execute(
    'SELECT * FROM movies_cache WHERE tmdb_id = ? LIMIT 1',
    [tmdbId]
  );
  if (!rows.length) return null;
  const row  = rows[0];
  row.genres = typeof row.genres === 'string' ? JSON.parse(row.genres) : (row.genres ?? []);
  row.files  = typeof row.files  === 'string' ? JSON.parse(row.files)  : (row.files  ?? []);
  return row;
}

async function dbFindTVMeta(tmdbId) {
  const pool = getPool();
  const [rows] = await pool.execute(
    'SELECT * FROM tv_meta WHERE tmdb_id = ? LIMIT 1',
    [tmdbId]
  );
  if (!rows.length) return null;
  const row  = rows[0];
  row.genres = typeof row.genres === 'string' ? JSON.parse(row.genres) : (row.genres ?? []);
  return row;
}

async function dbFindEpisode(tmdbId, season, episode) {
  const pool = getPool();
  const [rows] = await pool.execute(
    'SELECT * FROM tv_episodes WHERE tmdb_id = ? AND season = ? AND episode = ? LIMIT 1',
    [tmdbId, season, episode]
  );
  if (!rows.length) return null;
  const row = rows[0];
  row.files = typeof row.files === 'string' ? JSON.parse(row.files) : (row.files ?? []);
  return row;
}

async function dbUpsertMovie(d) {
  const pool = getPool();
  await pool.execute(`
    INSERT INTO movies_cache
      (tmdb_id, title, type, year, genres, rating, overview,
       poster, tmdb_url, directory_url, files, total_files, cached_at)
    VALUES (?, ?, 'movie', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      title         = VALUES(title),
      year          = VALUES(year),
      genres        = VALUES(genres),
      rating        = VALUES(rating),
      overview      = VALUES(overview),
      poster        = VALUES(poster),
      tmdb_url      = VALUES(tmdb_url),
      directory_url = VALUES(directory_url),
      files         = VALUES(files),
      total_files   = VALUES(total_files),
      cached_at     = VALUES(cached_at),
      updated_at    = CURRENT_TIMESTAMP
  `, [
    d.tmdb_id,
    d.title         ?? '',
    d.year          ?? null,
    JSON.stringify(d.genres  ?? []),
    d.rating        ?? null,
    d.overview      ?? null,
    d.poster        ?? null,
    d.tmdb_url      ?? null,
    d.directory_url ?? null,
    JSON.stringify(d.files   ?? []),
    d.total_files   ?? 0,
    d.cached_at     ?? new Date().toISOString(),
  ]);
}

async function dbUpsertTVMeta(d) {
  const pool = getPool();
  await pool.execute(`
    INSERT INTO tv_meta
      (tmdb_id, title, type, year, genres, rating, overview,
       poster, tmdb_url, cached_at)
    VALUES (?, ?, 'tv', ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      title      = VALUES(title),
      year       = VALUES(year),
      genres     = VALUES(genres),
      rating     = VALUES(rating),
      overview   = VALUES(overview),
      poster     = VALUES(poster),
      tmdb_url   = VALUES(tmdb_url),
      cached_at  = VALUES(cached_at),
      updated_at = CURRENT_TIMESTAMP
  `, [
    d.tmdb_id,
    d.title    ?? '',
    d.year     ?? null,
    JSON.stringify(d.genres ?? []),
    d.rating   ?? null,
    d.overview ?? null,
    d.poster   ?? null,
    d.tmdb_url ?? null,
    d.cached_at ?? new Date().toISOString(),
  ]);
}

async function dbUpsertEpisode(tmdbId, d) {
  const pool = getPool();
  await pool.execute(`
    INSERT INTO tv_episodes
      (tmdb_id, season, episode, directory_url, files, total_files, cached_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      directory_url = VALUES(directory_url),
      files         = VALUES(files),
      total_files   = VALUES(total_files),
      cached_at     = VALUES(cached_at),
      updated_at    = CURRENT_TIMESTAMP
  `, [
    tmdbId,
    d.season,
    d.episode,
    d.directory_url ?? null,
    JSON.stringify(d.files ?? []),
    d.total_files   ?? 0,
    d.cached_at     ?? new Date().toISOString(),
  ]);
}

// ============================================================
//  MAIN HANDLER
// ============================================================
export async function GET(request) {
  const { searchParams } = new URL(request.url);
  const tmdbId  = (searchParams.get('tmdb_id')  ?? '').trim();
  const type    = (searchParams.get('type')     ?? '').trim();
  const season  = (searchParams.get('season')   ?? '').trim();
  const episode = (searchParams.get('episode')  ?? '').trim();

  if (!tmdbId || !type) {
    return NextResponse.json({ error: 'tmdb_id and type are required' }, { status: 400 });
  }

  // ── STEP 1: Check DB cache ──────────────────────────────────
  if (type === 'movie') {
    const cached = await dbFindMovie(tmdbId);
    if (cached) {
      return NextResponse.json({ ...cached, cache: 'hit' });
    }
  } else {
    const cachedMeta = await dbFindTVMeta(tmdbId);

    if (cachedMeta && season && episode) {
      const cachedEpisode = await dbFindEpisode(tmdbId, parseInt(season), parseInt(episode));
      if (cachedEpisode) {
        return NextResponse.json({ ...cachedMeta, ...cachedEpisode, cache: 'hit' });
      }
      // Meta cached, episode not — skip TMDB fetch
    } else if (cachedMeta && !season && !episode) {
      return NextResponse.json({ ...cachedMeta, cache: 'hit' });
    }

    var metaFromCache = cachedMeta ?? null;
  }

  // ── STEP 2: Fetch TMDB ─────────────────────────────────────
  let tmdbMeta, title, year;

  if (!metaFromCache) {
    const tmdbUrl = type === 'movie'
      ? `https://api.themoviedb.org/3/movie/${tmdbId}?api_key=${TMDB_API_KEY}`
      : `https://api.themoviedb.org/3/tv/${tmdbId}?api_key=${TMDB_API_KEY}`;

    const tmdbRaw = await proxyFetch(tmdbUrl);
    if (!tmdbRaw) {
      return NextResponse.json({ error: 'Failed to fetch from TMDB' }, { status: 502 });
    }

    const d   = JSON.parse(tmdbRaw);
    title     = type === 'movie' ? (d.title ?? '') : (d.name ?? '');
    year      = type === 'movie'
      ? (d.release_date  ?? '').slice(0, 4)
      : (d.first_air_date ?? '').slice(0, 4);

    tmdbMeta = {
      tmdb_id : tmdbId,
      title,
      type,
      year    : year || null,
      genres  : (d.genres ?? []).map(g => g.name),
      rating  : d.vote_average ? Math.round(d.vote_average * 10) / 10 : null,
      overview: d.overview  ?? null,
      poster  : d.poster_path ? `https://image.tmdb.org/t/p/w500${d.poster_path}` : null,
      tmdb_url: `https://www.themoviedb.org/${type}/${tmdbId}`,
    };
  } else {
    tmdbMeta = metaFromCache;
    title    = metaFromCache.title;
    year     = metaFromCache.year ?? '';
  }

  // ── STEP 3: Search ─────────────────────────────────────────
  let searchQuery = title;
  if (type === 'tv' && season && episode) {
    const s = season.padStart(2, '0');
    const e = episode.padStart(2, '0');
    searchQuery = `${title} s${s}e${e}`;
  }

  const searchUrl = `https://s.111477.xyz/search?q=${encodeURIComponent(searchQuery)}&limit=50&sort=score`;
  const searchRaw = await proxyFetch(searchUrl);
  if (!searchRaw) {
    return NextResponse.json({ error: 'Failed to fetch search results' }, { status: 502 });
  }

  let results;
  try { results = JSON.parse(searchRaw); } catch {
    return NextResponse.json({ error: 'Invalid search response' }, { status: 502 });
  }
  if (!Array.isArray(results)) {
    return NextResponse.json({ error: 'Invalid search response' }, { status: 502 });
  }

  // ── STEP 4: Find best directory ────────────────────────────
  const dirs      = results.filter(r => r.is_dir === true);
  const files     = results.filter(r => r.is_dir === false);
  const normTitle = normalizeTitle(title);
  let bestDir     = null;
  let bestScore   = 0;

  for (const dir of dirs) {
    const score = scoreDir(dir.name, normTitle, year);
    if (score > bestScore) { bestScore = score; bestDir = dir; }
  }

  // ── STEP 5: Fetch directory listing ───────────────────────
  let finalFiles = [];
  let dirUrl     = null;

  if (bestDir && bestScore >= 1) {
    if (type === 'tv' && season) {
      dirUrl = buildLink(bestDir).replace(/\/$/, '') + `/Season%20${parseInt(season)}/`;
    } else {
      dirUrl = buildLink(bestDir);
    }

    const dirHtml = await proxyFetch(dirUrl);
    if (dirHtml) {
      const rowRe = /<tr[^>]*data-name="([^"]+)"[^>]*data-url="([^"]+)"[^>]*>.*?<td class="size"[^>]*data-sort="(\d+)"/gs;
      let match;
      while ((match = rowRe.exec(dirHtml)) !== null) {
        const rawName  = match[1].replace(/&quot;/g, '"').replace(/&amp;/g, '&');
        const filePath = decodeURIComponent(match[2].replace(/&amp;/g, '&'));
        const fileSize = parseInt(match[3]);
        if (fileSize <= 0) continue;
        finalFiles.push({
          name        : rawName,
          size_human  : formatSize(fileSize),
          size_bytes  : fileSize,
          download_url: 'https://a.111477.xyz' + encodePathSegments('/' + filePath.replace(/^\//, '')),
        });
      }
    }
  }

  // ── STEP 6: Fallback — score files from search results ────
  if (!finalFiles.length) {
    const videoExts  = ['mkv', 'mp4', 'avi', 'mov', 'wmv'];
    const s          = (type === 'tv' && season)  ? season.padStart(2, '0')  : '';
    const e          = (type === 'tv' && episode) ? episode.padStart(2, '0') : '';
    const scoredFiles = [];

    for (const item of files) {
      const name = item.name ?? '';
      const path = item.path ?? '';
      const ext  = name.split('.').pop().toLowerCase();
      if (!videoExts.includes(ext)) continue;

      const normPath = '/' + path.replace(/^\//, '');
      if (type === 'tv'    && !/\/tvs\//i.test(normPath))    continue;
      if (type === 'movie' && !/\/movies\//i.test(normPath)) continue;

      const normName      = normalizeTitle(name);
      const normPathLower = normalizeTitle(normPath);
      let   score         = 0;

      // Year mandatory
      if (year && (normName.includes(year) || normPathLower.includes(year))) {
        score += 40;
      } else continue;

      // Folder similarity
      const folderMatch = normPath.match(/\/(?:tvs|movies)\/([^/]+)\//);
      if (folderMatch) {
        const folderNorm = folderMatch[1].replace(/\d{4}/g, '').trim();
        // simple similarity check
        const longer  = Math.max(normTitle.length, folderNorm.length);
        const common  = [...normTitle].filter(c => folderNorm.includes(c)).length;
        const pct     = longer ? (common / longer) * 100 : 0;
        if (pct < 70) continue;
        score += Math.floor(pct * 0.5);
      } else continue;

      if (normPathLower.includes(normTitle)) score += 50;
      if (normName.includes(normTitle))      score += 30;

      if (type === 'tv' && s && e) {
        if (new RegExp(`[Ss]${s}[Ee]${e}`, 'i').test(name)) {
          score += 20;
        } else continue;
      }

      scoredFiles.push({
        name        : name,
        size_human  : formatSize(parseInt(item.size)),
        size_bytes  : parseInt(item.size),
        download_url: 'https://a.111477.xyz' + encodePathSegments('/' + path.replace(/^\//, '')),
        _score      : score,
      });
    }

    scoredFiles.sort((a, b) => b._score !== a._score ? b._score - a._score : b.size_bytes - a.size_bytes);

    if (scoredFiles.length) {
      const best = scoredFiles[0]._score;
      for (const f of scoredFiles) {
        if (f._score >= best - 20) {
          const { _score, ...rest } = f;
          finalFiles.push(rest);
        }
      }
    }
  }

  // Sort by size desc
  finalFiles.sort((a, b) => (b.size_bytes ?? 0) - (a.size_bytes ?? 0));

  // Fix dirUrl if null
  if (!dirUrl && finalFiles.length) {
    const firstUrl = finalFiles[0].download_url ?? '';
    const m1 = firstUrl.match(/https:\/\/[^/]+(\/[^?]+\/Season\s*\d+\/)/i);
    const m2 = firstUrl.match(/https:\/\/[^/]+(\/[^/]+\/[^/]+\/[^/]+\/)/i);
    if (m1) dirUrl = 'https://a.111477.xyz' + m1[1];
    else if (m2) dirUrl = 'https://a.111477.xyz' + m2[1];
  }

  // ── STEP 7: Save to DB ─────────────────────────────────────
  const hasFiles = finalFiles.length > 0;

  if (hasFiles) {
    if (type === 'movie') {
      await dbUpsertMovie({
        ...tmdbMeta,
        directory_url: dirUrl,
        files        : finalFiles,
        total_files  : finalFiles.length,
        cached_at    : new Date().toISOString(),
      });
    } else {
      if (!metaFromCache) {
        await dbUpsertTVMeta({ ...tmdbMeta, cached_at: new Date().toISOString() });
      }
      if (season && episode) {
        await dbUpsertEpisode(tmdbId, {
          season       : parseInt(season),
          episode      : parseInt(episode),
          directory_url: dirUrl,
          files        : finalFiles,
          total_files  : finalFiles.length,
          cached_at    : new Date().toISOString(),
        });
      }
    }
  }

  // ── STEP 8: Proxy info + Response ─────────────────────────
  const proxyInfo = await getProxyInfo();

  const response = {
    ...tmdbMeta,
    directory_url: dirUrl,
    files        : finalFiles,
    total_files  : finalFiles.length,
    proxy        : proxyInfo,
    cache        : hasFiles ? 'stored' : 'miss_no_files',
  };

  if (type === 'tv' && season && episode) {
    response.episode = { season: parseInt(season), episode: parseInt(episode) };
  }

  return NextResponse.json(response);
}
