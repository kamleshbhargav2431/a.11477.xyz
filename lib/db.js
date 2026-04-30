// lib/db.js — MariaDB connection pool (singleton)
import mysql from 'mysql2/promise';

let pool = null;

export function getPool() {
  if (!pool) {
    pool = mysql.createPool({
  host               : process.env.DB_HOST,
  port               : parseInt(process.env.DB_PORT ?? '3306'),
  database           : process.env.DB_NAME,
  user               : process.env.DB_USER,
  password           : process.env.DB_PASS,
  waitForConnections : true,
  connectionLimit    : 10,
  queueLimit         : 0,
  charset            : 'utf8mb4',
  ssl                : { rejectUnauthorized: false }, // ← add this line
});
  }
  return pool;
}
