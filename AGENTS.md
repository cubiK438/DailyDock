# DailyDock — Agent Instructions

## 1. Project Overview

DailyDock is a personal infrastructure and automation platform designed to run primarily on a Raspberry Pi 4B with 2 GB RAM.

Its purpose is to centralize infrastructure monitoring, personal data management, automation, notifications, and system interaction.

The project is being developed incrementally. Requirements and architecture will evolve as implementation progresses.

## 2. Technology Direction

- Backend: Python-first hybrid architecture.
- Frontend: React.
- Architecture: Modular Monolith.
- Backend testing: Pytest.
- Future performance components: Rust, only when justified by measured requirements.
- Primary deployment target: Raspberry Pi 4B, 2 GB RAM.
- Development machine: MacBook Air M4.
- Version control: Git and GitHub.

Do not introduce additional technologies or frameworks without discussing their purpose and trade-offs.

## 3. Architectural Principles

- Maintain clear module responsibilities.
- Avoid unnecessary coupling between modules.
- Prefer simple solutions over speculative abstractions.
- Introduce new modules when actual requirements justify them.
- Keep the application modular without prematurely introducing microservices.
- Separate frontend and backend responsibilities.
- Keep core functionality independent of optional AI services and the Mac compute worker.
- Design for limited memory and processing resources.

## 4. Development Rules

- Understand the existing code before modifying it.
- Do not assume the project structure is permanent.
- Let requirements drive directory and module organization.
- Avoid unnecessary dependencies.
- Do not duplicate existing functionality.
- Keep changes focused on the requested task.
- Explain significant architectural decisions.
- Do not silently change established requirements or interfaces.
- Do not generate application code unless explicitly requested.

## 5. Hardware Constraints

The Raspberry Pi has 2 GB RAM.

Therefore:

- Avoid unnecessarily heavyweight services.
- Consider memory consumption when selecting dependencies.
- Avoid excessive background processes.
- Measure actual resource consumption.
- Do not assume the MacBook will always be available.
- Keep core functionality operational on the Raspberry Pi independently.

The MacBook is an optional on-demand compute worker.

## 6. Testing Requirements

- Use Pytest for Python tests.
- Write tests for meaningful functionality.
- Include error and failure scenarios.
- Test module interactions where applicable.
- Do not claim a feature works without appropriate verification.
- Distinguish tests executed locally from tests executed on the Raspberry Pi.

## 7. Security Rules

- Never expose passwords, API keys, tokens, or private credentials.
- Never commit secrets or local environment files.
- Do not disable security protections for convenience.
- Do not expose administrative interfaces publicly without explicit approval.
- Require explicit approval for destructive operations.

## 8. Agent Permissions

The agent may inspect and modify project files within the authorized workspace when instructed.

Explicit approval is required before:

- Installing system-wide packages.
- Modifying Raspberry Pi system configuration.
- Changing network or SSH configuration.
- Formatting, partitioning, or deleting storage.
- Deleting user data.
- Deploying or restarting production services.
- Executing destructive commands.
- Pushing changes to GitHub unless explicitly authorized.

Never assume permission for an operation simply because it appears technically necessary.

## 9. Communication

Before implementing a substantial feature:

1. Understand the requirement.
2. Identify relevant existing components.
3. Explain the proposed approach.
4. Identify important trade-offs.
5. Ask for clarification when requirements are ambiguous.

After implementation:

1. Summarize changes.
2. List files modified.
3. Report tests executed and their results.
4. Identify known limitations.
5. Mention any additional actions requiring approval.

## 10. Guiding Principle

DailyDock should be built incrementally.

Prioritize correctness, reliability, maintainability, security, and resource efficiency over unnecessary complexity.

Do not build hypothetical future functionality without an actual requirement.