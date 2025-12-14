/**
 * File: src/telemetry.ts
 * Author: Claude
 * Last Updated: 2025-10-07
 * Description: OpenTelemetry configuration and instrumentation
 */

import { NodeSDK } from '@opentelemetry/sdk-node';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { Resource } from '@opentelemetry/resources';
import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions';
import { trace, metrics, Span, Counter, Histogram } from '@opentelemetry/api';
import { config } from './config.js';

let sdk: NodeSDK | null = null;
let tracer: ReturnType<typeof trace.getTracer> | null = null;
let meter: ReturnType<typeof metrics.getMeter> | null = null;

// Metrics
let toolCallCounter: Counter | null = null;
let queryDurationHistogram: Histogram | null = null;
let queryErrorCounter: Counter | null = null;
let queryRowsHistogram: Histogram | null = null;
let queryBytesHistogram: Histogram | null = null;

export function initTelemetry(): void {
  if (!config.telemetry.enabled) {
    console.log('OpenTelemetry disabled');
    return;
  }

  const resource = Resource.default().merge(
    new Resource({
      [ATTR_SERVICE_NAME]: config.telemetry.serviceName,
    })
  );

  const traceExporter = new OTLPTraceExporter({
    url: `${config.telemetry.endpoint}/v1/traces`,
  });

  const metricExporter = new OTLPMetricExporter({
    url: `${config.telemetry.endpoint}/v1/metrics`,
  });

  sdk = new NodeSDK({
    resource,
    traceExporter,
    metricReader: new PeriodicExportingMetricReader({
      exporter: metricExporter,
      exportIntervalMillis: 10000,
    }),
    instrumentations: [getNodeAutoInstrumentations()],
  });

  sdk.start();

  tracer = trace.getTracer(config.telemetry.serviceName);
  meter = metrics.getMeter(config.telemetry.serviceName);

  // Initialize metrics
  toolCallCounter = meter.createCounter('ccmysql.tool.calls', {
    description: 'Number of tool calls',
  });

  queryDurationHistogram = meter.createHistogram('ccmysql.query.duration', {
    description: 'Query execution duration in milliseconds',
    unit: 'ms',
  });

  queryErrorCounter = meter.createCounter('ccmysql.query.errors', {
    description: 'Number of query errors',
  });

  queryRowsHistogram = meter.createHistogram('ccmysql.query.rows', {
    description: 'Number of rows returned or affected by query',
    unit: 'rows',
  });

  queryBytesHistogram = meter.createHistogram('ccmysql.query.bytes', {
    description: 'Size of query result payload in bytes',
    unit: 'bytes',
  });

  console.log(`OpenTelemetry initialized, exporting to ${config.telemetry.endpoint}`);
}

export function shutdownTelemetry(): Promise<void> {
  if (sdk) {
    return sdk.shutdown();
  }
  return Promise.resolve();
}

export function startSpan(name: string, attributes?: Record<string, string | number>): Span | null {
  if (!tracer) return null;
  return tracer.startSpan(name, { attributes });
}

export function recordToolCall(toolName: string): void {
  if (toolCallCounter) {
    toolCallCounter.add(1, { tool: toolName });
  }
}

export function recordQueryDuration(duration: number, operation: string): void {
  if (queryDurationHistogram) {
    queryDurationHistogram.record(duration, { operation });
  }
}

export function recordQueryError(operation: string, error: string): void {
  if (queryErrorCounter) {
    queryErrorCounter.add(1, { operation, error });
  }
}

export function recordQueryRows(rows: number, queryType: string, operation: string): void {
  if (queryRowsHistogram) {
    queryRowsHistogram.record(rows, { queryType, operation });
  }
}

export function recordQueryBytes(bytes: number, queryType: string, operation: string): void {
  if (queryBytesHistogram) {
    queryBytesHistogram.record(bytes, { queryType, operation });
  }
}