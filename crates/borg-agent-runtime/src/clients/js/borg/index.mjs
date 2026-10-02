// Borg's catalog/native/MCP tools as async functions, for `exec` commands.
// In runtime_exec, use the preloaded borg object instead of importing.
// Calls retain host permission, approval, and human-confirmation checks.
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

function toolArguments(args = {}) {
  if (args === null) return {};
  if (typeof args !== "object" || Array.isArray(args)) {
    throw new TypeError("Borg tool arguments must be an object");
  }
  return args;
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
  let line;
  try { line = JSON.stringify(body) + "\n"; }
  catch (error) { return Promise.reject(new BorgError(`Borg tool ${name} failed: ${error.message}`)); }
  return new Promise((resolve, reject) => {
    let socket;
    try { socket = net.connect(address, () => socket.write(line)); }
    catch (error) { reject(new BorgError(error.message)); return; }
    let buffer = "";
    socket.setEncoding("utf8");
    socket.on("data", (chunk) => {
      buffer += chunk;
      const end = buffer.indexOf("\n");
      if (end < 0) return;
      socket.end();
      try {
        const response = JSON.parse(buffer.slice(0, end));
        if (response === null || typeof response !== "object" || Array.isArray(response)) {
          throw new Error("Borg returned an invalid response object");
        }
        if ("error" in response) reject(new BorgError(response.error));
        else resolve(response.result);
      } catch (error) {
        reject(new BorgError(`Borg tool ${name} failed: ${error.message}`));
      }
    });
    socket.on("error", (error) => reject(new BorgError(error.message)));
    socket.on("end", () => {
      if (!buffer.includes("\n")) reject(new BorgError(`Borg closed the connection without answering ${name}`));
    });
  });
}

/** Call any host tool with its arguments object. */
export const call = (name, args) => request(name, toolArguments(args));
export const tool = call;

/** All available catalog/native/MCP tools, or ranked matches with schemas. */
export const tools = (query, limit = 10) => request("__borg_tools", {
  workspace_tools: true, ...(query === undefined || query === null ? {} : { query, limit }),
});

const borg = new Proxy(
  { call, tool, tools, BorgError },
  // `then` stays undefined so the object is never mistaken for a promise.
  { get: (target, name) => (Object.hasOwn(target, name) || typeof name !== "string" || name === "then" ? target[name] : (args) => call(name, args)) },
);
export default borg;
