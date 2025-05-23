/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  webpack: (config) => {
    config.resolve.fallback = {
      ...config.resolve.fallback,
      fs: false,
      net: false,
      tls: false,
      bufferutil: false,
      "utf-8-validate": false,
      path: false,
    };
    return config;
  },
};

module.exports = nextConfig;