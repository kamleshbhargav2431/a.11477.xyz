// app/api/stream/route.js
// Cache flow: Redis (30min) → DB → live fetch
// GET /api/stream?tmdb_id=123&type=movie
// GET /api/stream?tmdb_id=25&type=tv&season=1&episode=1

import { getPool }                      from '@/lib/db';
import { redisGet, redisSet, redisKey } from '@/lib/redis';
import { NextResponse }                 from 'next/server';
import { HttpsProxyAgent }              from 'https-proxy-agent';

const TMDB_API_KEY = process.env.TMDB_API_KEY;
const PROXY_URL    = `http://${process.env.PROXY_USER}:${process.env.PROXY_PASS}@${process.env.PROXY_HOST}:${process.env.PROXY_PORT}`;

// ============================================================
//  PROXY FETCH
// ============================================================
async function proxyFetch(url) {
  try {
    const res = await fetch(url, {
      agent  : new HttpsProxyAgent(PROXY_URL),
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Accept'    : 'application/json, text/html',
      },
      redirect: 'follow',
      signal  : AbortSignal.timeout(20000),
    });
    return res.ok ? res.text() : null;
  } catch { return null; }
}

// ============================================================
//  PROXY INFO
// ============================================================
async function getProxyInfo() {
  try {
    const res  = await fetch('https://httpbin.org/ip', {
      agent : new HttpsProxyAgent(PROXY_URL),
      signal: AbortSignal.timeout(8000),
    });
    if (!res.ok) throw new Error('failed');
    const data = await res.json();
    return {
      enabled : true,
      provider: 'Webshare Rotating',
      method  : 'webshare_rotating_proxy',
      endpoint: `${process.env.PROXY_HOST}:${process.env.PROXY_PORT}`,
      ip      : data.origin ?? null,
      note    : 'Each request uses a different IP address',
    };
  } catch (e) {
    return {
      enabled : false,
      provider: 'Webshare Rotating',
      method  : 'direct_fallback',
      endpoint: `${process.env.PROXY_HOST}:${process.env.PROXY_PORT}`,
      ip      : null,
      note    : 'Proxy failed. Error: ' + e.message,
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
  return path.split('/').map(s => encodeURIComponent(decodeURIComponent(s))).join('/');
}

function buildLink(item) {
  let p = item.path;
  if (!p.startsWith('/')) p = '/' + p;
  return 'https://a.111477.xyz' + encodePathSegments(p);
}

function normalizeTitle(t) {
  return t.toLowerCase()
    .replace(/[:\-–—]/g, ' ')
    .replace(/[^a-z0-9 ]/g, '')
    .replace(/\s+/g, ' ').trim();
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
function parseRow(row, jsonFields) {
  for (const f of jsonFields) {
    if (row[f] && typeof row[f] === 'string') row[f] = JSON.parse(row[f]);
    else if (!row[f]) row[f] = Array.isArray([]) ? [] : null;
  }
  return row;
}

async function dbFindMovie(tmdbId) {
  const [rows] = await getPool().execute('SELECT * FROM movies_cache WHERE tmdb_id = ? LIMIT 1', [tmdbId]);
  return rows.length ? parseRow(rows[0], ['genres', 'files']) : null;
}

async function dbFindTVMeta(tmdbId) {
  const [rows] = await getPool().execute('SELECT * FROM tv_meta WHERE tmdb_id = ? LIMIT 1', [tmdbId]);
  return rows.length ? parseRow(rows[0], ['genres']) : null;
}

async function dbFindEpisode(tmdbId, season, episode) {
  const [rows] = await getPool().execute(
    'SELECT * FROM tv_episodes WHERE tmdb_id = ? AND season = ? AND episode = ? LIMIT 1',
    [tmdbId, season, episode]
  );
  return rows.length ? parseRow(rows[0], ['files']) : null;
}

async function dbUpsertMovie(d) {
  await getPool().execute(`
    INSERT INTO movies_cache
      (tmdb_id,title,type,year,genres,rating,overview,poster,tmdb_url,directory_url,files,total_files,cached_at)
    VALUES (?,'movie',?,?,?,?,?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
      title=VALUES(title),year=VALUES(year),genres=VALUES(genres),rating=VALUES(rating),
      overview=VALUES(overview),poster=VALUES(poster),tmdb_url=VALUES(tmdb_url),
      directory_url=VALUES(directory_url),files=VALUES(files),total_files=VALUES(total_files),
      cached_at=VALUES(cached_at),updated_at=CURRENT_TIMESTAMP
  `, [
    d.tmdb_id, d.title??'', d.year??null, JSON.stringify(d.genres??[]),
    d.rating??null, d.overview??null, d.poster??null, d.tmdb_url??null,
    d.directory_url??null, JSON.stringify(d.files??[]),
    d.total_files??0, d.cached_at??new Date().toISOString(),
  ]);
}

async function dbUpsertTVMeta(d) {
  await getPool().execute(`
    INSERT INTO tv_meta
      (tmdb_id,title,type,year,genres,rating,overview,poster,tmdb_url,cached_at)
    VALUES (?,?,'tv',?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
      title=VALUES(title),year=VALUES(year),genres=VALUES(genres),rating=VALUES(rating),
      overview=VALUES(overview),poster=VALUES(poster),tmdb_url=VALUES(tmdb_url),
      cached_at=VALUES(cached_at),updated_at=CURRENT_TIMESTAMP
  `, [
    d.tmdb_id, d.title??'', d.year??null, JSON.stringify(d.genres??[]),
    d.rating??null, d.overview??null, d.poster??null, d.tmdb_url??null,
    d.cached_at??new Date().toISOString(),
  ]);
}

async function dbUpsertEpisode(tmdbId, d) {
  await getPool().execute(`
    INSERT INTO tv_episodes
      (tmdb_id,season,episode,directory_url,files,total_files,cached_at)
    VALUES (?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
      directory_url=VALUES(directory_url),files=VALUES(files),
      total_files=VALUES(total_files),cached_at=VALUES(cached_at),updated_at=CURRENT_TIMESTAMP
  `, [
    tmdbId, d.season, d.episode, d.directory_url??null,
    JSON.stringify(d.files??[]), d.total_files??0,
    d.cached_at??new Date().toISOString(),
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

  // ============================================================
  //  STEP 1 — Redis check (~0.5ms, no DB hit)
  // ============================================================
  if (type === 'movie') {
    const cached = await redisGet(redisKey.movie(tmdbId));
    if (cached) return NextResponse.json({ ...cached, cache: 'redis' });

  } else {
    const redisMeta = await redisGet(redisKey.tvMeta(tmdbId));

    if (redisMeta && season && episode) {
      const redisEp = await redisGet(redisKey.episode(tmdbId, season, episode));
      if (redisEp) return NextResponse.json({ ...redisMeta, ...redisEp, cache: 'redis' });
    } else if (redisMeta && !season && !episode) {
      return NextResponse.json({ ...redisMeta, cache: 'redis' });
    }
  }

  // ============================================================
  //  STEP 2 — DB check (~1-2ms)
  // ============================================================
  let metaFromCache = null;

  if (type === 'movie') {
    const dbRow = await dbFindMovie(tmdbId);
    if (dbRow) {
      await redisSet(redisKey.movie(tmdbId), dbRow); // warm Redis
      return NextResponse.json({ ...dbRow, cache: 'db' });
    }
  } else {
    const dbMeta = await dbFindTVMeta(tmdbId);

    if (dbMeta && season && episode) {
      const dbEp = await dbFindEpisode(tmdbId, parseInt(season), parseInt(episode));
      if (dbEp) {
        await redisSet(redisKey.tvMeta(tmdbId), dbMeta);             // warm Redis
        await redisSet(redisKey.episode(tmdbId, season, episode), dbEp); // warm Redis
        return NextResponse.json({ ...dbMeta, ...dbEp, cache: 'db' });
      }
      metaFromCache = dbMeta; // meta cached but episode not
    } else if (dbMeta && !season && !episode) {
      await redisSet(redisKey.tvMeta(tmdbId), dbMeta);
      return NextResponse.json({ ...dbMeta, cache: 'db' });
    }
  }

  // ============================================================
  //  STEP 3 — Fetch TMDB
  // ============================================================
  let tmdbMeta, title, year;

  if (!metaFromCache) {
    const tmdbUrl = type === 'movie'
      ? `https://api.themoviedb.org/3/movie/${tmdbId}?api_key=${TMDB_API_KEY}`
      : `https://api.themoviedb.org/3/tv/${tmdbId}?api_key=${TMDB_API_KEY}`;

    const tmdbRaw = await proxyFetch(tmdbUrl);
    if (!tmdbRaw) return NextResponse.json({ error: 'Failed to fetch from TMDB' }, { status: 502 });

    const d = JSON.parse(tmdbRaw);
    title   = type === 'movie' ? (d.title ?? '') : (d.name ?? '');
    year    = type === 'movie' ? (d.release_date ?? '').slice(0, 4) : (d.first_air_date ?? '').slice(0, 4);

    tmdbMeta = {
      tmdb_id : tmdbId,
      title,
      type,
      year    : year || null,
      genres  : (d.genres ?? []).map(g => g.name),
      rating  : d.vote_average ? Math.round(d.vote_average * 10) / 10 : null,
      overview: d.overview ?? null,
      poster  : d.poster_path ? `https://image.tmdb.org/t/p/w500${d.poster_path}` : null,
      tmdb_url: `https://www.themoviedb.org/${type}/${tmdbId}`,
    };
  } else {
    tmdbMeta = metaFromCache;
    title    = metaFromCache.title;
    year     = metaFromCache.year ?? '';
  }

  // ============================================================
  //  STEP 4 — Search
  // ============================================================
  let searchQuery = title;
  if (type === 'tv' && season && episode) {
    searchQuery = `${title} s${season.padStart(2,'0')}e${episode.padStart(2,'0')}`;
  }

  const searchRaw = await proxyFetch(`https://s.111477.xyz/search?q=${encodeURIComponent(searchQuery)}&limit=50&sort=score`);
  if (!searchRaw) return NextResponse.json({ error: 'Failed to fetch search results' }, { status: 502 });

  let results;
  try { results = JSON.parse(searchRaw); } catch {
    return NextResponse.json({ error: 'Invalid search response' }, { status: 502 });
  }
  if (!Array.isArray(results)) return NextResponse.json({ error: 'Invalid search response' }, { status: 502 });

  // ============================================================
  //  STEP 5 — Find best directory
  // ============================================================
  const dirs      = results.filter(r => r.is_dir === true);
  const files     = results.filter(r => r.is_dir === false);
  const normTitle = normalizeTitle(title);
  let   bestDir   = null, bestScore = 0;

  for (const dir of dirs) {
    const score = scoreDir(dir.name, normTitle, year);
    if (score > bestScore) { bestScore = score; bestDir = dir; }
  }

  // ============================================================
  //  STEP 6 — Fetch directory listing
  // ============================================================
  let finalFiles = [], dirUrl = null;

  if (bestDir && bestScore >= 1) {
    dirUrl = type === 'tv' && season
      ? buildLink(bestDir).replace(/\/$/, '') + `/Season%20${parseInt(season)}/`
      : buildLink(bestDir);

    const dirHtml = await proxyFetch(dirUrl);
    if (dirHtml) {
      const rowRe = /<tr[^>]*data-name="([^"]+)"[^>]*data-url="([^"]+)"[^>]*>.*?<td class="size"[^>]*data-sort="(\d+)"/gs;
      let match;
      while ((match = rowRe.exec(dirHtml)) !== null) {
        const fileSize = parseInt(match[3]);
        if (fileSize <= 0) continue;
        finalFiles.push({
          name        : match[1].replace(/&quot;/g,'"').replace(/&amp;/g,'&'),
          size_human  : formatSize(fileSize),
          size_bytes  : fileSize,
          download_url: 'https://a.111477.xyz' + encodePathSegments('/' + decodeURIComponent(match[2].replace(/&amp;/g,'&')).replace(/^\//,'')),
        });
      }
    }
  }

  // ============================================================
  //  STEP 7 — Fallback: score files from search results
  // ============================================================
  if (!finalFiles.length) {
    const videoExts   = ['mkv','mp4','avi','mov','wmv'];
    const s           = (type==='tv'&&season)  ? season.padStart(2,'0')  : '';
    const e           = (type==='tv'&&episode) ? episode.padStart(2,'0') : '';
    const scoredFiles = [];

    for (const item of files) {
      const name = item.name ?? '';
      const path = item.path ?? '';
      if (!videoExts.includes(name.split('.').pop().toLowerCase())) continue;

      const normPath = '/' + path.replace(/^\//,'');
      if (type==='tv'    && !/\/tvs\//i.test(normPath))    continue;
      if (type==='movie' && !/\/movies\//i.test(normPath)) continue;

      const normName = normalizeTitle(name);
      const normPL   = normalizeTitle(normPath);
      let   score    = 0;

      if (year && (normName.includes(year)||normPL.includes(year))) score += 40; else continue;

      const fm = normPath.match(/\/(?:tvs|movies)\/([^/]+)\//);
      if (fm) {
        const fn     = fm[1].replace(/\d{4}/g,'').trim();
        const longer = Math.max(normTitle.length, fn.length);
        const common = [...normTitle].filter(c => fn.includes(c)).length;
        const pct    = longer ? (common/longer)*100 : 0;
        if (pct < 70) continue;
        score += Math.floor(pct*0.5);
      } else continue;

      if (normPL.includes(normTitle))   score += 50;
      if (normName.includes(normTitle)) score += 30;

      if (type==='tv'&&s&&e) {
        if (new RegExp(`[Ss]${s}[Ee]${e}`,'i').test(name)) score += 20; else continue;
      }

      scoredFiles.push({
        name, size_human: formatSize(parseInt(item.size)),
        size_bytes: parseInt(item.size),
        download_url: 'https://a.111477.xyz' + encodePathSegments('/'+path.replace(/^\//,'')),
        _score: score,
      });
    }

    scoredFiles.sort((a,b) => b._score!==a._score ? b._score-a._score : b.size_bytes-a.size_bytes);
    if (scoredFiles.length) {
      const best = scoredFiles[0]._score;
      for (const f of scoredFiles) {
        if (f._score >= best-20) { const {_score,...rest}=f; finalFiles.push(rest); }
      }
    }
  }

  finalFiles.sort((a,b) => (b.size_bytes??0)-(a.size_bytes??0));

  if (!dirUrl && finalFiles.length) {
    const fu = finalFiles[0].download_url ?? '';
    const m1 = fu.match(/https:\/\/[^/]+(\/[^?]+\/Season\s*\d+\/)/i);
    const m2 = fu.match(/https:\/\/[^/]+(\/[^/]+\/[^/]+\/[^/]+\/)/i);
    if (m1) dirUrl = 'https://a.111477.xyz'+m1[1];
    else if (m2) dirUrl = 'https://a.111477.xyz'+m2[1];
  }

  // ============================================================
  //  STEP 8 — Save to DB + Redis
  // ============================================================
  const hasFiles = finalFiles.length > 0;

  if (hasFiles) {
    if (type === 'movie') {
      const movieData = { ...tmdbMeta, directory_url:dirUrl, files:finalFiles, total_files:finalFiles.length, cached_at:new Date().toISOString() };
      await dbUpsertMovie(movieData);
      await redisSet(redisKey.movie(tmdbId), movieData);
    } else {
      if (!metaFromCache) {
        const metaData = { ...tmdbMeta, cached_at:new Date().toISOString() };
        await dbUpsertTVMeta(metaData);
        await redisSet(redisKey.tvMeta(tmdbId), metaData);
      }
      if (season && episode) {
        const epData = { season:parseInt(season), episode:parseInt(episode), directory_url:dirUrl, files:finalFiles, total_files:finalFiles.length, cached_at:new Date().toISOString() };
        await dbUpsertEpisode(tmdbId, epData);
        await redisSet(redisKey.episode(tmdbId, season, episode), epData);
      }
    }
  }

  // ============================================================
  //  STEP 9 — Response
  // ============================================================
  const proxyInfo = await getProxyInfo();
  const response  = {
    ...tmdbMeta,
    directory_url: dirUrl,
    files        : finalFiles,
    total_files  : finalFiles.length,
    proxy        : proxyInfo,
    cache        : hasFiles ? 'stored' : 'miss_no_files',
  };

  if (type==='tv' && season && episode) {
    response.episode = { season:parseInt(season), episode:parseInt(episode) };
  }

  return NextResponse.json(response);
}
