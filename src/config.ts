/**
 * File: src/config.ts
 * Author: Claude
 * Last Updated: 2025-10-07
 * Description: Configuration loader for MySQL MCP server
 */

import { config as dotenvConfig } from 'dotenv';
import { readFileSync } from 'fs';
import { resolve } from 'path';
import { log } from './logger.js';

dotenvConfig();

export interface MySQLConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database?: string;
  connectionLimit: number;
}

export interface TelemetryConfig {
  enabled: boolean;
  endpoint: string;
  serviceName: string;
}

export interface LoggingConfig {
  enabled: boolean;
  logPath?: string;
}

export interface Config {
  mysql: MySQLConfig;
  telemetry: TelemetryConfig;
  logging: LoggingConfig;
}

function loadConfig(): Config {
  // Try to load from config file if specified
  const configPath = process.env.CONFIG_PATH;
  if (configPath) {
    try {
      const configFile = readFileSync(resolve(configPath), 'utf-8');
      return JSON.parse(configFile);
    } catch (error) {
      console.error(`Failed to load config from ${configPath}:`, error);
      process.exit(1);
    }
  }

  // Fall back to environment variables
  const rawPassword = process.env.MYSQL_PASSWORD || '';

  // URL-decode password if it appears to be encoded
  let decodedPassword = rawPassword;
  if (rawPassword.includes('%')) {
    try {
      decodedPassword = decodeURIComponent(rawPassword);
    } catch (error) {
      // If decoding fails, use raw password
      decodedPassword = rawPassword;
    }
  }

  // Build OpenTelemetry endpoint
  // Priority: OTEL_ENDPOINT (full URL) > OTEL_HOST + OTEL_PORT (construct URL)
  let otelEndpoint: string;
  if (process.env.OTEL_ENDPOINT) {
    otelEndpoint = process.env.OTEL_ENDPOINT;
  } else {
    const otelHost = process.env.OTEL_HOST || 'localhost';
    const otelPort = process.env.OTEL_PORT || '4318';
    const otelProtocol = process.env.OTEL_PROTOCOL || 'http';
    otelEndpoint = `${otelProtocol}://${otelHost}:${otelPort}`;
  }

  const config = {
    mysql: {
      host: process.env.MYSQL_HOST || 'localhost',
      port: parseInt(process.env.MYSQL_PORT || '3306', 10),
      user: process.env.MYSQL_USER || 'root',
      password: decodedPassword,
      database: process.env.MYSQL_DATABASE,
      connectionLimit: parseInt(process.env.MYSQL_CONNECTION_LIMIT || '10', 10),
    },
    telemetry: {
      enabled: process.env.OTEL_ENABLED === 'true',
      endpoint: otelEndpoint,
      serviceName: process.env.OTEL_SERVICE_NAME || 'cc-mysql',
    },
    logging: {
      enabled: process.env.LOG_ENABLED === 'true',
      logPath: process.env.LOG_PATH,
    },
  };

  // Log configuration (mask password)
  log('Configuration loaded:');
  log(`  MySQL Host: ${config.mysql.host}:${config.mysql.port}`);
  log(`  MySQL User: ${config.mysql.user}`);
  log(`  MySQL Password: ${config.mysql.password ? '[SET]' : '[NOT SET]'}`);
  log(`  MySQL Database: ${config.mysql.database || '[NOT SET]'}`);
  log(`  Connection Limit: ${config.mysql.connectionLimit}`);
  log(`  Telemetry Enabled: ${config.telemetry.enabled}`);
  if (config.telemetry.enabled) {
    log(`  Telemetry Endpoint: ${config.telemetry.endpoint}`);
    log(`  Telemetry Service: ${config.telemetry.serviceName}`);
  }
  log('');

  return config;
}

export const config = loadConfig();