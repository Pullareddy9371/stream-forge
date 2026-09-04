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
    position: { x: 50, y: 180 },
    data: {
      label: "🚚 Truck IoT\nTelemetry",
    },
  },
  {
    id: "kafka",
    position: { x: 300, y: 180 },
    data: {
      label: "⚡ Apache Kafka\ntruck-telemetry",
    },
  },
  {
    id: "consumer",
    position: { x: 570, y: 180 },
    data: {
      label: "📥 Stream\nConsumer",
    },
  },
  {
    id: "processor",
    position: { x: 830, y: 180 },
    data: {
      label: "⚙️ Telemetry\nProcessor",
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
    id: "kafka-consumer",
    source: "kafka",
    target: "consumer",
    animated: true,
  },
  {
    id: "consumer-processor",
    source: "consumer",
    target: "processor",
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
        <h1>StreamForge</h1>
        <p>Real-Time IoT Streaming Topology</p>
      </header>

      <main className="topology-container">
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
      </main>
    </div>
  );
}

export default App;