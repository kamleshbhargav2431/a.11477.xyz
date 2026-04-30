/** @type {import('next').NextConfig} */
const nextConfig = {
  serverExternalPackages: ['mysql2', 'redis'],
};
module.exports = nextConfig;
