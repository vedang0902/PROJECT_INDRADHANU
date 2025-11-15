import { BrowserRouter as Router, Routes, Route, Link } from "react-router-dom";
import Chat from "./pages/Chat";
import Dashboard from "./pages/Dashboard";

export default function App() {
  return (
    <Router>
      <nav className="p-4 bg-gray-800 text-white flex justify-between">
        <h1 className="text-xl font-bold">Agent Control Center</h1>
        <div>
          <Link className="mr-4" to="/">Chat</Link>
          <Link to="/dashboard">Dashboard</Link>
        </div>
      </nav>

      <Routes>
        <Route path="/" element={<Chat />} />
        <Route path="/dashboard" element={<Dashboard />} />
      </Routes>
    </Router>
  );
}
