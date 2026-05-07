"""
cli.py — Interactive command-line interface for the credit-risk agent.

Usage (inside the Docker scheduler container or a local Python env):
  python agent/cli.py

Try asking:
  "What is the status of eligibility_analytics_dag?"
  "Show me the high risk customers"
  "Summarise the latest eligibility output"

LEARNING NOTE — What to observe:
  The LLM will answer from its training knowledge about Airflow in general.
  It cannot query your running system — it will either make up an answer or
  admit it doesn't know. This is exactly why tools are added in Phase 4.
"""

import logging
import sys
import os

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s %(name)s - %(message)s",
)

sys.path.insert(0, os.path.dirname(__file__))
import agent_core


def main() -> None:
    print("Credit-Risk Agent CLI  [Phase 7 -- approval gate active]")
    print("Read tools: list_dag_runs, get_task_status, read_eligibility_output, get_high_risk_customers")
    print("Action tools: trigger_dag_run, retry_task, unpause_dag (low risk - auto)")
    print("              clear_dag_run (HIGH risk - requires Gmail approval)")
    print("Type a question and press Enter. Ctrl+C to exit.\n")

    while True:
        try:
            user_input = input("You: ").strip()
            if not user_input:
                continue
            response = agent_core.run(user_input)
            print(f"\nAgent: {response}\n")
        except KeyboardInterrupt:
            print("\nGoodbye.")
            break


if __name__ == "__main__":
    main()