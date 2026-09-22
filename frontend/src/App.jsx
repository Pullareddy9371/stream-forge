import { useCallback } from "react";
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

function App() {
  const [nodes, setNodes, onNodesChange] =
    useNodesState(initialNodes);

  const [edges, setEdges, onEdgesChange] =
    useEdgesState(initialEdges);

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

  return (
    <div className="app">
      <header className="header">
        <div>
          <h1>StreamForge</h1>
          <p>Distributed Python Event Processor</p>
        </div>

        <div className="system-status">
          <span className="status-dot"></span>
          System Dashboard
        </div>
      </header>

      <main className="dashboard">
        <section className="dashboard-card">
          <div className="card-header">
            <h2>Streaming Architecture</h2>
            <p>
              Kafka → Bytewax → Prometheus Metrics → Prometheus
            </p>
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