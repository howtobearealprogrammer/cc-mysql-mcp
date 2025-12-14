/**
 * File: src/logger.ts
 * Author: Claude
 * Last Updated: 2025-10-07
 * Description: File-based logging utility for MCP server diagnostics
 */

import { appendFileSync, mkdirSync } from 'fs';
import { join, dirname } from 'path';
import { homedir, tmpdir } from 'os';

// Check if logging is enabled via environment variable
const LOGGING_ENABLED = process.env.LOG_ENABLED === 'true';

// Try multiple log file locations for reliability
const LOG_LOCATIONS = [
  process.env.LOG_PATH,
  join(process.cwd(), 'cc-mysql-debug.log'),
  join(homedir(), 'cc-mysql-debug.log'),
  join(tmpdir(), 'cc-mysql-debug.log'),
].filter(Boolean) as string[];

let activeLogFile: string | null = null;

// Only initialize log file if logging is enabled
if (LOGGING_ENABLED) {
  // Find first writable location
  for (const logPath of LOG_LOCATIONS) {
    try {
      const dir = dirname(logPath);
      mkdirSync(dir, { recursive: true });
      const testMessage = `[${new Date().toISOString()}] === MCP Server Starting - Log file: ${logPath} ===\n`;
      appendFileSync(logPath, testMessage);
      activeLogFile = logPath;
      console.error(`Logging enabled - writing to: ${logPath}`);
      break;
    } catch (error) {
      console.error(`Cannot write to ${logPath}:`, error);
    }
  }

  if (!activeLogFile) {
    console.error('WARNING: Logging enabled but could not create log file in any location!');
  }
} else {
  console.error('Logging disabled (set LOG_ENABLED=true to enable)');
}

export function log(message: string): void {
  if (!LOGGING_ENABLED) return;

  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}\n`;

  console.error(message); // Log to stderr when logging is enabled

  if (activeLogFile) {
    try {
      appendFileSync(activeLogFile, logMessage);
    } catch (error) {
      console.error('Failed to write to log file:', error);
    }
  }
}

export function logObject(label: string, obj: any): void {
  if (!LOGGING_ENABLED) return;
  log(`${label}: ${JSON.stringify(obj, null, 2)}`);
}

// Log initial startup info if logging is enabled
if (LOGGING_ENABLED) {
  log('=== MCP Server Process Starting ===');
  log(`Process ID: ${process.pid}`);
  log(`Node version: ${process.version}`);
  log(`CWD: ${process.cwd()}`);
  log(`Platform: ${process.platform}`);
  if (activeLogFile) {
    log(`Active log file: ${activeLogFile}`);
  }
  log('');
}
