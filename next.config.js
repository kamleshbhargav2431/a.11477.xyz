/** @type {import('next').NextConfig} */
const nextConfig = {
  serverExternalPackages: ['mysql2', 'redis', 'cheerio'],
};
module.exports = nextConfig;
