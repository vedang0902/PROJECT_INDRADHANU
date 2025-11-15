import { useState } from "react";

export default function Chat() {
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState([
    { from: "bot", text: "Hi! I’m Indradhanush. How can I help you?" },
  ]);

  const sendMessage = async () => {
    setMessages([...messages, { from: "user", text: input }]);
    setInput("");

    if (input.toLowerCase().includes("register agent")) {
      setMessages((msgs) => [
        ...msgs,
        { from: "bot", text: "Use the Dashboard to register a new agent." },
      ]);
    } else {
      setMessages((msgs) => [
        ...msgs,
        { from: "bot", text: "I'm here to help you monitor or register agents." },
      ]);
    }
  };

  return (
    <div className="p-6">
      <div className="border rounded-lg p-4 h-96 overflow-y-auto bg-gray-100">
        {messages.map((m, i) => (
          <div key={i} className={`my-2 ${m.from === "user" ? "text-right" : ""}`}>
            <span className={`inline-block px-3 py-2 rounded-lg ${m.from === "user" ? "bg-blue-400 text-white" : "bg-gray-300"}`}>
              {m.text}
            </span>
          </div>
        ))}
      </div>
      <div className="mt-4 flex">
        <input
          className="border flex-1 px-3 py-2 rounded-l-lg"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Type a message..."
        />
        <button
          onClick={sendMessage}
          className="bg-blue-600 text-white px-4 rounded-r-lg"
        >
          Send
        </button>
      </div>
    </div>
  );
}
