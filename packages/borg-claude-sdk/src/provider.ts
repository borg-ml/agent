import {
  query,
  type CanUseTool,
  type ElicitationResult,
  type OnElicitation,
  type Options,
  type PermissionResult,
  type PermissionUpdate,
} from "@anthropic-ai/claude-agent-sdk";
import { createInterface } from "node:readline";

import { TurnMessageBoundary } from "./turn_messages.js";

type ProviderConfig = {
  prompt: string;
  attachments?: string[];
  workspace_dir?: string;
  model?: string;
  effort?: Options["effort"];
  fast?: boolean;
  permission_mode?: Options["permissionMode"];
  system_prompt?: string;
  output_schema?: Record<string, unknown>;
  mcp_servers?: Options["mcpServers"];
  allowed_tools?: string;
  resume?: string;
  persist_session?: boolean;
};

type Control =
  | { type: "steer"; text: string; attachments?: string[] }
  | { type: "interrupt" }
  | {
      type: "approval";
      approval_id: string;
      decision: "approve_once" | "approve_session" | "reject";
    }
  | {
      type: "provider_interaction_response";
      interaction_id: string;
      response: ElicitationResult;
    }
  | { type: "start"; config: ProviderConfig };

type UserMessage = {
  type: "user";
  message: {
    role: "user";
    content: Array<{ type: "text"; text: string }>;
  };
  parent_tool_use_id: null;
  session_id: string;
  uuid: `${string}-${string}-${string}-${string}-${string}`;
};

class AsyncQueue<T> implements AsyncIterable<T> {
  private values: T[] = [];
  private waiters: Array<(value: IteratorResult<T>) => void> = [];
  private closed = false;

  push(value: T): void {
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter({ value, done: false });
    } else if (!this.closed) {
      this.values.push(value);
    }
  }

  close(): void {
    this.closed = true;
    for (const waiter of this.waiters.splice(0)) {
      waiter({ value: undefined, done: true });
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: async () => {
        const value = this.values.shift();
        if (value) return { value, done: false };
        if (this.closed) return { value: undefined, done: true };
        return new Promise((resolve) => this.waiters.push(resolve));
      },
    };
  }
}

function optionsFrom(
  config: ProviderConfig,
  canUseTool: CanUseTool,
  onElicitation: OnElicitation,
): Options {
  const options: Options = {
    cwd: config.workspace_dir,
    model: config.model,
    effort: config.effort,
    permissionMode: config.permission_mode ?? "default",
    allowDangerouslySkipPermissions:
      config.permission_mode === "bypassPermissions",
    systemPrompt: config.system_prompt,
    outputFormat: config.output_schema
      ? { type: "json_schema", schema: config.output_schema }
      : undefined,
    mcpServers: config.mcp_servers,
    allowedTools: config.allowed_tools
      ?.split(",")
      .map((tool) => tool.trim())
      .filter(Boolean),
    resume: config.resume,
    persistSession: config.persist_session,
    includePartialMessages: true,
    canUseTool,
    onElicitation,
  };

  if (config.fast) {
    options.settings = { fastMode: true, fastModePerSessionOptIn: true };
  }
  return options;
}

function promptText(
  prompt: string,
  configuredAttachments: string[] = [],
): string {
  const attachments = configuredAttachments.filter(Boolean);
  if (attachments.length === 0) {
    return prompt;
  }
  return `${prompt}\n\nAttached files:\n${attachments
    .map((path) => `- ${path}`)
    .join("\n")}`;
}

function userMessage(text: string): UserMessage {
  return {
    type: "user",
    message: { role: "user", content: [{ type: "text", text }] },
    parent_tool_use_id: null,
    session_id: "",
    uuid: globalThis.crypto.randomUUID(),
  };
}

function lifecycleKey(config: ProviderConfig): string {
  const { prompt: _prompt, attachments: _attachments, resume: _resume, ...stable } =
    config;
  return JSON.stringify(stable);
}

async function main(): Promise<void> {
  const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
  const iterator = lines[Symbol.asyncIterator]();
  const first = await iterator.next();
  if (first.done) throw new Error("Borg Claude adapter config was not provided");
  let config: ProviderConfig | undefined = JSON.parse(first.value);
  const turns = new AsyncQueue<ProviderConfig>();
  let activeInput: AsyncQueue<UserMessage> | undefined;
  let activeInputIds: Set<string> | undefined;
  let activeBoundary: TurnMessageBoundary | undefined;
  let activeStream: ReturnType<typeof query> | undefined;
  const pendingApprovals = new Map<
    string,
    {
      resolve: (result: PermissionResult) => void;
      suggestions?: PermissionUpdate[];
    }
  >();
  const pendingInteractions = new Map<
    string,
    (result: ElicitationResult) => void
  >();
  const canUseTool: CanUseTool = async (toolName, toolInput, context) => {
    process.stdout.write(
      `${JSON.stringify({
        type: "borg_permission_request",
        approval_id: context.requestId,
        tool_use_id: context.toolUseID,
        tool_name: toolName,
        input: toolInput,
        title: context.title ?? context.displayName ?? `Use ${toolName}`,
        detail:
          context.description ??
          context.decisionReason ??
          `Claude requested permission to use ${toolName}.`,
        command:
          toolName === "Bash" && typeof toolInput.command === "string"
            ? toolInput.command
            : null,
      })}\n`,
    );
    return new Promise<PermissionResult>((resolve) => {
      const abort = () => {
        pendingApprovals.delete(context.requestId);
        resolve({
          behavior: "deny",
          message: "Permission request was cancelled.",
          interrupt: true,
        });
      };
      if (context.signal.aborted) {
        abort();
        return;
      }
      context.signal.addEventListener("abort", abort, { once: true });
      pendingApprovals.set(context.requestId, {
        suggestions: context.suggestions,
        resolve: (result) => {
          context.signal.removeEventListener("abort", abort);
          resolve(result);
        },
      });
    });
  };
  const onElicitation: OnElicitation = async (request, { signal }) => {
    const interactionId =
      request.elicitationId ?? globalThis.crypto.randomUUID();
    process.stdout.write(
      `${JSON.stringify({
        type: "borg_provider_interaction",
        interaction_id: interactionId,
        kind: "mcp_elicitation",
        title:
          request.title ??
          `${request.displayName ?? request.serverName} requests input`,
        detail: request.description ?? request.message,
        payload: request,
      })}\n`,
    );
    return new Promise<ElicitationResult>((resolve) => {
      const abort = () => {
        pendingInteractions.delete(interactionId);
        resolve({ action: "cancel" });
      };
      if (signal.aborted) {
        abort();
        return;
      }
      signal.addEventListener("abort", abort, { once: true });
      pendingInteractions.set(interactionId, (result) => {
        signal.removeEventListener("abort", abort);
        resolve(result);
      });
    });
  };
  const controls = (async () => {
    try {
      for await (const line of { [Symbol.asyncIterator]: () => iterator }) {
        const control = JSON.parse(line) as Control;
        if (control.type === "start") {
          turns.push(control.config);
        } else if (control.type === "steer") {
          const message = userMessage(
            promptText(control.text, control.attachments),
          );
          activeInputIds?.add(message.uuid);
          activeInput?.push(message);
        } else if (control.type === "interrupt") {
          const stream = activeStream;
          if (stream) {
            await Promise.allSettled(
              [
                stream.interrupt(),
                ...(activeBoundary?.backgroundTaskIds() ?? []).map((taskId) =>
                  stream.stopTask(taskId),
                ),
              ],
            );
          }
        } else if (control.type === "approval") {
          const pending = pendingApprovals.get(control.approval_id);
          if (!pending) continue;
          pendingApprovals.delete(control.approval_id);
          if (control.decision === "reject") {
            pending.resolve({
              behavior: "deny",
              message: "User denied this action.",
            });
          } else {
            pending.resolve({
              behavior: "allow",
              updatedPermissions:
                control.decision === "approve_session"
                  ? pending.suggestions
                  : undefined,
            });
          }
        } else if (control.type === "provider_interaction_response") {
          const resolve = pendingInteractions.get(control.interaction_id);
          if (!resolve) continue;
          pendingInteractions.delete(control.interaction_id);
          resolve(control.response);
        }
      }
    } finally {
      turns.close();
    }
  })();

  while (config) {
    const input = new AsyncQueue<UserMessage>();
    const stream = query({
      prompt: input,
      options: optionsFrom(config, canUseTool, onElicitation),
    });
    const messages = stream[Symbol.asyncIterator]();
    activeInput = input;
    activeStream = stream;
    while (config) {
      const currentLifecycle = lifecycleKey(config);
      const initialMessage = userMessage(
        promptText(config.prompt, config.attachments),
      );
      const inputIds = new Set([initialMessage.uuid]);
      const boundary = new TurnMessageBoundary();
      activeInputIds = inputIds;
      activeBoundary = boundary;
      input.push(initialMessage);
      while (true) {
        const nextMessage = await messages.next();
        if (nextMessage.done) {
          config = undefined;
          break;
        }
        const message = nextMessage.value;
        const action = boundary.classify(message, { inputIds });
        if (action === "suppress") {
          continue;
        }
        process.stdout.write(`${JSON.stringify(message)}\n`);
        if (message.type === "assistant") {
          try {
            const usage = await stream.getContextUsage();
            process.stdout.write(
              `${JSON.stringify({
                type: "borg_context_usage",
                total_tokens: usage.totalTokens,
                context_window_tokens: usage.maxTokens,
                raw_context_window_tokens: usage.rawMaxTokens,
                model: usage.model,
                categories: usage.categories,
              })}\n`,
            );
          } catch {
            // Older runtimes may not expose context usage. Final usage remains
            // authoritative in that case.
          }
        }
        if (action === "terminal") break;
      }
      if (!config) break;
      const next = await turns[Symbol.asyncIterator]().next();
      const nextConfig = next.done ? undefined : next.value;
      if (!nextConfig || lifecycleKey(nextConfig) !== currentLifecycle) {
        config = nextConfig;
        break;
      }
      config = nextConfig;
    }
    activeInput = undefined;
    activeInputIds = undefined;
    activeBoundary = undefined;
    activeStream = undefined;
    input.close();
    stream.close();
  }
  for (const pending of pendingApprovals.values()) {
    pending.resolve({
      behavior: "deny",
      message: "Claude session ended before permission was decided.",
    });
  }
  pendingApprovals.clear();
  for (const resolve of pendingInteractions.values()) {
    resolve({ action: "cancel" });
  }
  pendingInteractions.clear();
  lines.close();
  await controls;
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  process.exitCode = 1;
});
