// Borg's capabilities as async functions, for code run through `exec`.
//
//   import borg from "borg"            // Bun; Node: const borg = require("borg")
//   await borg.send_message({ target: "/root/worker", message: report })
//   const plan = await borg.get_plan()
//   await borg.call("create_goal", { objective: "..." })
//
// Each call goes to the running session over its tool socket, exactly like
// `borg call NAME JSON`, and shows in the transcript as a step of the command
// that made it. Results are decoded JSON; failures reject with `BorgError`.
import net from "node:net";

export class BorgError extends Error {
  name = "BorgError";
}

function request(name, args) {
  const env = process.env;
  const body = {
    name,
    arguments: args ?? {},
    workflow_approved: env.BORG_AGENT_TOOL_APPROVED === "1",
  };
  if (env.BORG_TOOL_CALL_ID) body.parent = env.BORG_TOOL_CALL_ID;
  let address;
  if (env.BORG_AGENT_TOOL_SOCKET) {
    address = { path: env.BORG_AGENT_TOOL_SOCKET };
  } else if (env.BORG_AGENT_TOOL_TCP) {
    const split = env.BORG_AGENT_TOOL_TCP.lastIndexOf(":");
    address = { host: env.BORG_AGENT_TOOL_TCP.slice(0, split), port: Number(env.BORG_AGENT_TOOL_TCP.slice(split + 1)) };
    body.token = env.BORG_AGENT_TOOL_TOKEN ?? "";
  } else {
    return Promise.reject(new BorgError("not running inside a Borg session: BORG_AGENT_TOOL_SOCKET is unset"));
  }
  return new Promise((resolve, reject) => {
    const socket = net.connect(address, () => socket.write(JSON.stringify(body) + "\n"));
    let buffer = "";
    socket.setEncoding("utf8");
    socket.on("data", (chunk) => {
      buffer += chunk;
      const end = buffer.indexOf("\n");
      if (end < 0) return;
      socket.end();
      const response = JSON.parse(buffer.slice(0, end));
      if ("error" in response) reject(new BorgError(response.error));
      else resolve(response.result);
    });
    socket.on("error", (error) => reject(new BorgError(error.message)));
    socket.on("end", () => {
      if (!buffer.includes("\n")) reject(new BorgError(`Borg closed the connection without answering ${name}`));
    });
  });
}

/** Call the Borg capability `name` with an arguments object. */
export const call = (name, args) => request(name, args);

/** Every capability this session offers, with its input schema. */
export const tools = () => request("__borg_tools", {});

/** Capabilities ranked for `query`, each with a compact signature. */
export const search = (query, limit = 10) => request("__borg_tools", { query, limit });

const borg = new Proxy(
  { call, search, tools, BorgError },
  // `then` stays undefined so the object is never mistaken for a promise.
  { get: (target, name) => (name in target || typeof name !== "string" || name === "then" ? target[name] : (args) => call(name, args)) },
);
export default borg;
