export default function AgentCard({ name, info }) {
    return (
      <div className="p-3 border rounded-lg shadow-sm bg-white mb-2">
        <h3 className="font-bold">{name}</h3>
        <p>{info.description}</p>
        <p className="text-sm text-gray-500">Topic: {info.topic}</p>
      </div>
    );
  }
  