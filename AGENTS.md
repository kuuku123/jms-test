# Workspace Isolation Rules

- **Strict Directory Boundary**: Do NOT access, search, inspect, or reference any files, directories, or paths outside the workspace root (`/home/tony/workspace/study/EE/jms-test`).
- **Command Confinement**: All terminal commands, grep searches, file listings, and scripts MUST operate strictly within this workspace directory (`/home/tony/workspace/study/EE/jms-test`).
- Never traverse parent directories (`..`) or use absolute paths targeting anything outside this project.
- Respect user privacy and environment boundaries at all times.
