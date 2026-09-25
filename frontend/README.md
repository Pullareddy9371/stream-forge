\# StreamForge Frontend



React-based monitoring dashboard for the StreamForge distributed Python event processor.



\## Overview



The StreamForge frontend provides a visual dashboard for monitoring the real-time telemetry processing pipeline.



The dashboard is built using:



\- React

\- Vite

\- React Flow

\- Prometheus metrics



\## Architecture



```text

Truck IoT Telemetry

&#x20;       |

&#x20;       v

Apache Kafka

&#x20;       |

&#x20;       v

Bytewax Stream Processor

&#x20;       |

&#x20;       v

Prometheus Metrics :8000

&#x20;       |

&#x20;       v

Prometheus :9090

&#x20;       |

&#x20;       v

React Dashboard :5173

