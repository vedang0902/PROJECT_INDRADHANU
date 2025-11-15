import { useState, useEffect } from "react";
import AgentCard from "../components/AgentCard";

export default function Dashboard() {
  const [agents, setAgents] = useState({});
  const [alerts, setAlerts] = useState([]);

  useEffect(() => {
    fetch("http://localhost:8000/agents/list")
      .then(res => res.json())
      .then(data => setAgents(data.agents));

    const ws = new WebSocket("ws://localhost:8000/alerts");
    ws.onmessage = (event) => {
      const alert = JSON.parse(event.data);
      setAlerts(a => [...a, alert]);
    };
  }, []);

  return (
    <div className="p-6 grid grid-cols-2 gap-4">
      <div>
        <h2 className="text-xl font-bold mb-2">Registered Agents</h2>
        {Object.entries(agents).map(([name, info]) => (
          <AgentCard key={name} name={name} info={info} />
        ))}
      </div>

      <div>
        <h2 className="text-xl font-bold mb-2">Live Alerts</h2>
        <div className="border p-3 h-96 overflow-y-auto bg-gray-100 rounded-lg">
          {alerts.map((a, i) => (
            <p key={i}>⚠️ {a.alert_message || JSON.stringify(a)}</p>
          ))}
        </div>
      </div>
    </div>
  );
}
