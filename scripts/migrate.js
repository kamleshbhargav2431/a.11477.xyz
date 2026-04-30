// scripts/migrate.js
// Run: node scripts/migrate.js
// Imports all A11/ JSON cache files into MariaDB

const fs    = require('fs');
const path  = require('path');
const mysql = require('mysql2/promise');

require('dotenv').config({ path: path.join(__dirname, '../.env.local') });

const DB = {
  host    : process.env.DB_HOST,
  port    : parseInt(process.env.DB_PORT ?? '3306'),
  database: process.env.DB_NAME,
  user    : process.env.DB_USER,
  password: process.env.DB_PASS,
  charset : 'utf8mb4',
};

const CACHE_ROOT = path.join(__dirname, '../A11');

let inserted = 0, failed = 0;
const errors = [];

function readJson(filePath) {
  try { return JSON.parse(fs.readFileSync(filePath, 'utf8')); }
  catch { return null; }
}

async function upsertMovie(conn, d) {
  await conn.execute(`
    INSERT INTO movies_cache
      (tmdb_id, title, type, year, genres, rating, overview,
       poster, tmdb_url, directory_url, files, total_files, cached_at)
    VALUES (?, ?, 'movie', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      title=VALUES(title), year=VALUES(year), genres=VALUES(genres),
      rating=VALUES(rating), overview=VALUES(overview), poster=VALUES(poster),
      tmdb_url=VALUES(tmdb_url), directory_url=VALUES(directory_url),
      files=VALUES(files), total_files=VALUES(total_files),
      cached_at=VALUES(cached_at), updated_at=CURRENT_TIMESTAMP
  `, [
    d.tmdb_id, d.title ?? '', d.year ?? null,
    JSON.stringify(d.genres ?? []), d.rating ?? null,
    d.overview ?? null, d.poster ?? null, d.tmdb_url ?? null,
    d.directory_url ?? null, JSON.stringify(d.files ?? []),
    d.total_files ?? 0, d.cached_at ? new Date(d.cached_at) : new Date(),
  ]);
}

async function upsertTVMeta(conn, d) {
  await conn.execute(`
    INSERT INTO tv_meta
      (tmdb_id, title, type, year, genres, rating, overview, poster, tmdb_url, cached_at)
    VALUES (?, ?, 'tv', ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      title=VALUES(title), year=VALUES(year), genres=VALUES(genres),
      rating=VALUES(rating), overview=VALUES(overview), poster=VALUES(poster),
      tmdb_url=VALUES(tmdb_url), cached_at=VALUES(cached_at), updated_at=CURRENT_TIMESTAMP
  `, [
    d.tmdb_id, d.title ?? '', d.year ?? null,
    JSON.stringify(d.genres ?? []), d.rating ?? null,
    d.overview ?? null, d.poster ?? null, d.tmdb_url ?? null,
    d.cached_at ? new Date(d.cached_at) : new Date(),
  ]);
}

async function upsertTVEpisode(conn, tmdbId, d) {
  await conn.execute(`
    INSERT INTO tv_episodes
      (tmdb_id, season, episode, directory_url, files, total_files, cached_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      directory_url=VALUES(directory_url), files=VALUES(files),
      total_files=VALUES(total_files), cached_at=VALUES(cached_at),
      updated_at=CURRENT_TIMESTAMP
  `, [
    tmdbId, d.season ?? 1, d.episode ?? 1,
    d.directory_url ?? null, JSON.stringify(d.files ?? []),
    d.total_files ?? 0, d.cached_at ? new Date(d.cached_at) : new Date(),
  ]);
}

async function main() {
  console.log('Connecting to MariaDB...');
  const conn = await mysql.createConnection(DB);
  console.log('Connected.\n');

  // Movies
  const moviesDir = path.join(CACHE_ROOT, 'movies');
  if (fs.existsSync(moviesDir)) {
    const files = fs.readdirSync(moviesDir).filter(f => f.endsWith('.json'));
    console.log(`📁 Found ${files.length} movie cache files.`);
    for (const file of files) {
      const data = readJson(path.join(moviesDir, file));
      if (!data?.tmdb_id) { failed++; errors.push(`BAD JSON: ${file}`); continue; }
      try {
        await upsertMovie(conn, data);
        inserted++;
        console.log(`  ✅ Movie [${data.tmdb_id}] ${data.title}`);
      } catch (e) {
        failed++;
        errors.push(`DB ERROR (movie) ${file}: ${e.message}`);
        console.log(`  ❌ Failed: ${file}`);
      }
    }
  } else {
    console.log(`⚠️  No movies dir at: ${moviesDir}`);
  }

  console.log('');

  // TV
  const tvDir = path.join(CACHE_ROOT, 'tv');
  if (fs.existsSync(tvDir)) {
    const showDirs = fs.readdirSync(tvDir).filter(f =>
      fs.statSync(path.join(tvDir, f)).isDirectory()
    );
    console.log(`📁 Found ${showDirs.length} TV series directories.`);

    for (const showDir of showDirs) {
      const showPath = path.join(tvDir, showDir);
      const tmdbId  = showDir.split('_')[0];
      if (!tmdbId) { errors.push(`No tmdbId in dir: ${showDir}`); continue; }

      const meta = readJson(path.join(showPath, 'meta.json'));
      if (meta?.tmdb_id) {
        try {
          await upsertTVMeta(conn, meta);
          console.log(`  📺 Series [${tmdbId}] ${meta.title}`);
        } catch (e) {
          failed++;
          errors.push(`DB ERROR (tv_meta) ${showDir}: ${e.message}`);
        }
      } else {
        console.log(`  ⚠️  No meta.json in ${showDir}`);
      }

      const epFiles = fs.readdirSync(showPath).filter(f => /^s\d+e\d+\.json$/i.test(f));
      console.log(`     ${epFiles.length} episode(s)`);

      for (const epFile of epFiles) {
        const epData = readJson(path.join(showPath, epFile));
        if (!epData) { failed++; errors.push(`BAD JSON: ${epFile}`); continue; }
        try {
          await upsertTVEpisode(conn, tmdbId, epData);
          inserted++;
          console.log(`    ✅ S${epData.season}E${epData.episode}`);
        } catch (e) {
          failed++;
          errors.push(`DB ERROR (tv_ep) ${epFile}: ${e.message}`);
          console.log(`    ❌ Failed: ${epFile}`);
        }
      }
    }
  } else {
    console.log(`⚠️  No tv dir at: ${tvDir}`);
  }

  console.log('\n========================================');
  console.log(`✅ Inserted/Updated : ${inserted}`);
  console.log(`❌ Failed           : ${failed}`);
  if (errors.length) { console.log('\n--- Errors ---'); errors.forEach(e => console.log(`  • ${e}`)); }
  console.log('\n🎉 Migration complete!');
  await conn.end();
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
