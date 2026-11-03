import { CommandId, MessageId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import { forkParked } from "../serverActivation.ts";
import * as Orchestrator from "./Orchestrator.ts";

export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* forkParked(
      Stream.runForEach(orchestrator.streamDomainEvents, (event) => {
        if (event.type !== "run.updated" || event.payload.status !== "failed") return Effect.void;
        const identity = `codex-account-recovery:${event.threadId}:${event.payload.id}`;
        return orchestrator
          .dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(identity),
            messageId: MessageId.make(identity),
            threadId: event.threadId,
            codexAccountContinuationOfRunId: event.payload.id,
            text: "Continue where you left off.",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "server",
          })
          .pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("orchestration-v2.codex-account-recovery.failed", {
                threadId: event.threadId,
                cause,
              }),
            ),
          );
      }),
    );
  }),
);
