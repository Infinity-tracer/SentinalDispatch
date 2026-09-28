/** @type {import('next').NextConfig} */
const path = require('path');

// Next.js automatically loads .env files from the project root (parent directory)
// Variables prefixed with NEXT_PUBLIC_ are automatically exposed to the browser
//
// Note: Next.js uses dotenv which supports ${VAR} variable expansion syntax
// You can reference variables like: NEXT_PUBLIC_GOOGLE_MAPS_API_KEY="${GOOGLE_MAPS_API_KEY}"

const nextConfig = {
  reactStrictMode: true,
}

module.exports = nextConfig

