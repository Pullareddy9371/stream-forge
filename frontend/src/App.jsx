import { useCallback, useEffect, useState } from "react";
import {
  ReactFlow,
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  addEdge,
} from "@xyflow/react";

import "@xyflow/react/dist/style.css";
import "./App.css";

const initialNodes = [
  {
    id: "truck",
    position: { x: 30, y: 220 },
    data: {
      label: "Truck IoT\nTelemetry",
    },
  },
  {
    id: "kafka",
    position: { x: 280, y: 220 },
    data: {
      label: "Apache Kafka\ntruck-telemetry",
    },
  },
  {
    id: "bytewax",
    position: { x: 540, y: 220 },
    data: {
      label: "Bytewax\nStream Processor",
    },
  },
  {
    id: "metrics",
    position: { x: 820, y: 220 },
    data: {
      label: "Prometheus Metrics\n:8000",
    },
  },
  {
    id: "prometheus",
    position: { x: 1080, y: 220 },
    data: {
      label: "Prometheus\n:9090",
    },
  },
];

const initialEdges = [
  {
    id: "truck-kafka",
    source: "truck",
    target: "kafka",
    animated: true,
  },
  {
    id: "kafka-bytewax",
    source: "kafka",
    target: "bytewax",
    animated: true,
  },
  {
    id: "bytewax-metrics",
    source: "bytewax",
    target: "metrics",
    animated: true,
  },
  {
    id: "metrics-prometheus",
    source: "metrics",
    target: "prometheus",
    animated: true,
  },
];

function getMetricValue(metricsText, metricName) {
  const escapedName = metricName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

  const regex = new RegExp(
    `^${escapedName}(?:\\{[^\\n]*\\})?\\s+([0-9.eE+-]+)$`,
    "m"
  );

  const match = metricsText.match(regex);

  return match ? Number(match[1]) : 0;
}

function App() {
  const [nodes, setNodes, onNodesChange] =
    useNodesState(initialNodes);

  const [edges, setEdges, onEdgesChange] =
    useEdgesState(initialEdges);

  const [metrics, setMetrics] = useState({
    eventsProcessed: 0,
    invalidEvents: 0,
    processingErrors: 0,
    activeWorkers: 0,
    latencyCount: 0,
    latencySum: 0,
  });

  const [metricsStatus, setMetricsStatus] = useState("Connecting...");
  const [lastUpdated, setLastUpdated] = useState("-");

  const onConnect = useCallback(
    (connection) =>
      setEdges((currentEdges) =>
        addEdge(
          {
            ...connection,
            animated: true,
          },
          currentEdges
        )
      ),
    [setEdges]
  );

  useEffect(() => {
    let isMounted = true;

    const fetchMetrics = async () => {
      try {
        const response = await fetch("/metrics");

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const text = await response.text();

        if (!isMounted) {
          return;
        }

        setMetrics({
          eventsProcessed: getMetricValue(
            text,
            "streamforge_events_processed_total"
          ),
          invalidEvents: getMetricValue(
            text,
            "streamforge_invalid_events_total"
          ),
          processingErrors: getMetricValue(
            text,
            "streamforge_processing_errors_total"
          ),
          activeWorkers: getMetricValue(
            text,
            "streamforge_active_workers"
          ),
          latencyCount: getMetricValue(
            text,
            "streamforge_processing_latency_seconds_count"
          ),
          latencySum: getMetricValue(
            text,
            "streamforge_processing_latency_seconds_sum"
          ),
        });

        setMetricsStatus("Connected");
        setLastUpdated(new Date().toLocaleTimeString());
      } catch (error) {
        if (!isMounted) {
          return;
        }

        console.error("Failed to fetch Prometheus metrics:", error);
        setMetricsStatus("Disconnected");
      }
    };

    fetchMetrics();

    const interval = setInterval(fetchMetrics, 5000);

    return () => {
      isMounted = false;
      clearInterval(interval);
    };
  }, []);

  const averageLatency =
    metrics.latencyCount > 0
      ? metrics.latencySum / metrics.latencyCount
      : 0;

  return (
    <div className="app">
      <header className="header">
        <div>
          <h1>StreamForge</h1>
          <p>Distributed Python Event Processor</p>
        </div>

        <div className="system-status">
          <span
            className={`status-dot ${
              metricsStatus === "Connected" ? "connected" : "disconnected"
            }`}
          ></span>

          <span>{metricsStatus}</span>
        </div>
      </header>

      <main className="dashboard">
        <section className="metrics-grid">
          <div className="metric-card">
            <span className="metric-title">Events Processed</span>
            <strong>{metrics.eventsProcessed.toLocaleString()}</strong>
          </div>

          <div className="metric-card">
            <span className="metric-title">Invalid Events</span>
            <strong>{metrics.invalidEvents.toLocaleString()}</strong>
          </div>

          <div className="metric-card">
            <span className="metric-title">Processing Errors</span>
            <strong>{metrics.processingErrors.toLocaleString()}</strong>
          </div>

          <div className="metric-card">
            <span className="metric-title">Active Workers</span>
            <strong>{metrics.activeWorkers.toLocaleString()}</strong>
          </div>

          <div className="metric-card">
            <span className="metric-title">Average Latency</span>
            <strong>
              {(averageLatency * 1000).toFixed(2)} ms
            </strong>
          </div>
        </section>

        <section className="dashboard-card">
          <div className="card-header">
            <div>
              <h2>Streaming Architecture</h2>
              <p>
                Kafka → Bytewax → Prometheus Metrics → Prometheus
              </p>
            </div>

            <div className="metrics-info">
              Last updated: {lastUpdated}
            </div>
          </div>

          <div className="topology-container">
            <ReactFlow
              nodes={nodes}
              edges={edges}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onConnect={onConnect}
              fitView
            >
              <Background />
              <Controls />
              <MiniMap />
            </ReactFlow>
          </div>
        </section>
      </main>
    </div>
  );
}

export default App;