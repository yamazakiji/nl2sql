import { Routes, Route, Link } from "react-router-dom";

import ChatView from "./components/ChatView";
import RunLogs from "./components/RunLogs";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<ChatView />} />
      <Route path="/runs/:id" element={<RunLogs />} />
      <Route
        path="*"
        element={
          <div className="content">
            <h1>Not found</h1>
            <Link to="/">Back to home</Link>
          </div>
        }
      />
    </Routes>
  );
}
