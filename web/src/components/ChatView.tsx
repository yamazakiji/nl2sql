import { FormEvent, useMemo, useState } from "react";

interface Message {
  role: "user" | "assistant";
  content: string;
}

interface PlanCandidate {
  sql: string;
  rationale: string;
  explain_summary: string;
  est_cost: number;
}

interface ExecuteResult {
  rows: Array<Record<string, unknown>>;
  row_count: number;
  result_ref: string;
}

const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8001";

export default function ChatView() {
  const [deployment, setDeployment] = useState("dev");
  const [connector, setConnector] = useState("");
  const [question, setQuestion] = useState("");
  const [messages, setMessages] = useState<Message[]>([]);
  const [runId, setRunId] = useState<string | null>(null);
  const [candidates, setCandidates] = useState<PlanCandidate[]>([]);
  const [clarifications, setClarifications] = useState<string[]>([]);
  const [result, setResult] = useState<ExecuteResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  const sidebarPlaceholders = useMemo(
    () => ({
      schemas: ["orders", "users", "events"],
      savedQueries: ["Daily active users", "Order totals", "Sessions by browser"],
    }),
    []
  );

  async function handlePlan(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!question || !connector) {
      setError("Connector and question are required.");
      return;
    }
    setIsLoading(true);
    setError(null);
    setCandidates([]);
    setClarifications([]);
    setResult(null);

    try {
      const response = await fetch(`${API_BASE}/inference/plan`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question, deployment, connector }),
      });
      if (!response.ok) {
        const detail = await response.json().catch(() => ({ detail: response.statusText }));
        throw new Error(detail.detail ?? "Planning failed");
      }
      const data = await response.json();
      setRunId(data.run_id);
      setCandidates(data.candidates ?? []);
      setClarifications(data.clarifications ?? []);
      setMessages((prev) => [
        ...prev,
        { role: "user", content: question },
        { role: "assistant", content: `Proposed ${data.candidates?.length ?? 0} candidate SQL statements.` },
      ]);
      setQuestion("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setIsLoading(false);
    }
  }

  async function handleExecute(candidate: PlanCandidate) {
    if (!runId) {
      setError("No plan is associated with this conversation.");
      return;
    }
    setIsLoading(true);
    setError(null);

    try {
      const response = await fetch(`${API_BASE}/inference/execute`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          run_id: runId,
          connector,
          approved_sql: candidate.sql,
          limit: 100,
        }),
      });
      if (!response.ok) {
        const detail = await response.json().catch(() => ({ detail: response.statusText }));
        throw new Error(detail.detail ?? "Execution failed");
      }
      const data = await response.json();
      setResult(data);
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: `Executed plan with ${data.row_count} row(s). Result ref ${data.result_ref}` },
      ]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div>
          <h2>Schemas</h2>
          <ul>
            {sidebarPlaceholders.schemas.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </div>
        <div>
          <h2>Saved Queries</h2>
          <ul>
            {sidebarPlaceholders.savedQueries.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </div>
      </aside>
      <main className="content">
        <section className="chat-box">
          <header>
            <h1>NL → SQL Chat</h1>
          </header>
          <div className="chat-messages">
            {messages.map((message, index) => (
              <div key={index} className={`message ${message.role}`}>
                {message.content}
              </div>
            ))}
          </div>
          {error && <div className="message assistant">{error}</div>}
          <form className="chat-input" onSubmit={handlePlan}>
            <input
              type="text"
              placeholder="Ask a question about your data"
              value={question}
              onChange={(event) => setQuestion(event.target.value)}
            />
            <input
              type="text"
              placeholder="Deployment label"
              value={deployment}
              onChange={(event) => setDeployment(event.target.value)}
            />
            <input
              type="text"
              placeholder="Connector ID"
              value={connector}
              onChange={(event) => setConnector(event.target.value)}
            />
            <button type="submit" disabled={isLoading}>
              {isLoading ? "Working..." : "Plan"}
            </button>
          </form>
        </section>

        {clarifications.length > 0 && (
          <section className="chat-box">
            <h2>Clarifications</h2>
            <ul>
              {clarifications.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          </section>
        )}

        {candidates.length > 0 && (
          <section className="chat-box">
            <h2>SQL Candidates</h2>
            <div className="candidates">
              {candidates.map((candidate) => (
                <div key={candidate.sql} className="candidate">
                  <pre>{candidate.sql}</pre>
                  <p>{candidate.rationale}</p>
                  <small>{candidate.explain_summary}</small>
                  <p>Estimated cost: {candidate.est_cost}</p>
                  <button onClick={() => handleExecute(candidate)} disabled={isLoading}>
                    Approve &amp; Execute
                  </button>
                </div>
              ))}
            </div>
          </section>
        )}

        {result && (
          <section className="chat-box">
            <h2>Execution Result</h2>
            <p>
              Returned {result.row_count} row(s). Artifact: <code>{result.result_ref}</code>
            </p>
            <DataTable rows={result.rows} />
          </section>
        )}
      </main>
    </div>
  );
}

function DataTable({ rows }: { rows: Array<Record<string, unknown>> }) {
  if (!rows.length) {
    return <p>No rows were returned.</p>;
  }
  const columns = Object.keys(rows[0]);
  return (
    <table className="result-table">
      <thead>
        <tr>
          {columns.map((column) => (
            <th key={column}>{column}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, index) => (
          <tr key={index}>
            {columns.map((column) => (
              <td key={column}>{String(row[column])}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
