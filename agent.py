"""
Core triage agent loop.

The agent calls Databricks tools in a loop until Claude reaches end_turn.
Multiple tool calls in a single response (e.g. fix1 + fix2 + fix3) are all
executed and their results returned together in one user message, enabling
the agent to apply multiple fixes in a single iteration.
"""
from __future__ import annotations

import anthropic

from prompts import get_system_prompt
from tools import ALL_TOOLS, DatabricksToolExecutor


class TriageAgent:
    MODEL = "claude-sonnet-4-6"
    MAX_ITERATIONS = 20

    def __init__(
        self,
        tool_executor: DatabricksToolExecutor,
        verbose: bool = False,
    ):
        self.client = anthropic.Anthropic()
        self.executor = tool_executor
        self.verbose = verbose
        self.messages: list[dict] = []
        self.tools_called: list[str] = []
        self.iteration_count = 0

    def run(self, job_id: int | None = None, run_id: int | None = None) -> str:
        """
        Entry point. Provide at least one of job_id or run_id.
        Returns the final triage report as a string.
        """
        if not job_id and not run_id:
            raise ValueError("At least one of job_id or run_id must be provided.")

        initial_message = self._build_initial_message(job_id, run_id)
        self.messages = [{"role": "user", "content": initial_message}]
        return self._agent_loop()

    # ------------------------------------------------------------------
    # Agent loop
    # ------------------------------------------------------------------

    def _agent_loop(self) -> str:
        system = [
            {
                "type": "text",
                "text": get_system_prompt(),
                "cache_control": {"type": "ephemeral"},
            }
        ]

        for iteration in range(1, self.MAX_ITERATIONS + 1):
            self.iteration_count = iteration
            if self.verbose:
                print(f"\n[AGENT] Iteration {iteration}")

            response = self.client.messages.create(
                model=self.MODEL,
                max_tokens=8096,
                system=system,
                tools=ALL_TOOLS,
                messages=self.messages,
            )

            # Append full assistant response (text + tool_use blocks) to history.
            # Must include ALL blocks — stripping text blocks causes API errors.
            self.messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason == "end_turn":
                return self._extract_final_text(response.content)

            # Collect every tool_use block from this response
            tool_use_blocks = [b for b in response.content if b.type == "tool_use"]

            if not tool_use_blocks:
                # stop_reason was tool_use but no tool blocks — safety exit
                return "Agent stopped unexpectedly without tool calls or final text."

            if self.verbose:
                names = [b.name for b in tool_use_blocks]
                print(f"[AGENT] Calling {len(tool_use_blocks)} tool(s): {', '.join(names)}")

            # Execute ALL tool calls and batch results into one user message.
            # This is what allows multiple fixes to be applied per loop iteration.
            tool_results = self._execute_tool_batch(tool_use_blocks)
            self.messages.append({"role": "user", "content": tool_results})

        return "Maximum iterations reached. Partial triage report may be available above."

    def _execute_tool_batch(
        self, tool_use_blocks: list
    ) -> list[anthropic.types.ToolResultBlockParam]:
        """
        Execute every tool_use block from a single assistant response and return
        all results as a flat list of ToolResultBlockParam.

        All results are returned in one user message so Claude receives them
        simultaneously — enabling it to apply fix1 + fix2 + fix3 in one turn.
        """
        results: list[anthropic.types.ToolResultBlockParam] = []

        for block in tool_use_blocks:
            self.tools_called.append(block.name)

            if self.verbose:
                import json
                print(f"  [TOOL] {block.name}({json.dumps(block.input, default=str)})")

            content = self.executor.execute(
                tool_name=block.name,
                tool_input=block.input,
            )

            if self.verbose:
                # Show a trimmed preview of the result
                preview = content[:300] + "..." if len(content) > 300 else content
                print(f"  [RESULT] {preview}")

            results.append(
                anthropic.types.ToolResultBlockParam(
                    type="tool_result",
                    tool_use_id=block.id,
                    content=content,
                )
            )

        return results

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_initial_message(self, job_id: int | None, run_id: int | None) -> str:
        if run_id and job_id:
            return (
                f"Please triage Databricks job run {run_id} (job ID {job_id}). "
                f"Start with get_job_run_details for run {run_id}. "
                f"Also use list_recent_job_runs for job {job_id} to check for recurring failure patterns."
            )
        if run_id:
            return (
                f"Please triage Databricks job run {run_id}. "
                f"Start with get_job_run_details to identify the failure, cluster, and error message."
            )
        # job_id only — find the most recent failed run first
        return (
            f"Please triage Databricks job {job_id}. "
            f"Start with list_recent_job_runs to find the most recent failed run, "
            f"then diagnose it with get_job_run_details."
        )

    @staticmethod
    def _extract_final_text(content_blocks: list) -> str:
        text_parts = [b.text for b in content_blocks if b.type == "text"]
        return "\n".join(text_parts) if text_parts else "Triage complete (no final text produced)."
