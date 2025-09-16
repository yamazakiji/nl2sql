import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";

const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8001";

export default function RunLogs() {
  const { id } = useParams();
  const [lines, setLines] = useState<string[]>([]);

  useEffect(() => {
    if (!id) return;
    const eventSource = new EventSource(`${API_BASE}/runs/${id}/logs/stream`);
    eventSource.onmessage = (event) => {
      setLines((prev) => [...prev.slice(-200), event.data]);
    };
    eventSource.onerror = () => {
      eventSource.close();
    };
    return () => eventSource.close();
  }, [id]);

  return (
    <div className="content">
      <header>
        <h1>Run Logs</h1>
        <Link to="/">Back</Link>
      </header>
      <section className="logs">
        {lines.map((line, index) => (
          <div key={`${line}-${index}`}>{line}</div>
        ))}
      </section>
    </div>
  );
}
