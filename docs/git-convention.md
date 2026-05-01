```markdown
# Git Commit Standards

This project follows a strict commit message convention to maintain a readable and searchable history.

## 1. Commit Message Format
Each commit message consists of a **header**, a **body**, and an **optional footer**.
```text
<type>(<scope>): <subject>

[optional body]

[optional footer(s)]
```

### The Header (Required)
*   **Type:** Describes the *intent* of the change (see tags below).
*   **Scope:** A noun describing the section of the codebase (e.g., `auth`, `api`, `ui`).
*   **Subject:** A short, **present-tense** summary.
    *   Use: "fix bug" instead of "fixed bug" or "fixes bug."
    *   No period at the end.

---

## 2. Standard Tags (Types)

| Tag | Purpose |
| :--- | :--- |
| **feat** | A new feature or functional improvement. |
| **fix** | A bug fix. |
| **chore** | Routine tasks, maintenance, or tooling changes (e.g., updating dependencies). |
| **docs** | Documentation-only changes (README, inline comments). |
| **refactor** | Code changes that neither fix a bug nor add a feature. |
| **perf** | Code changes that improve performance. |
| **test** | Adding missing tests or correcting existing tests. |
| **style** | Changes that do not affect the meaning of the code (white-space, formatting). |
| **ci** | Changes to CI configuration files and scripts. |

---

## 3. Detailed Description (Body)
The body is used to explain the **what** and **why** of the change, especially for complex logic.

*   Use the **present tense**.
*   Separate the body from the header with a single blank line.
*   Explain the motivation for the change and contrast it with previous behavior.

---

## 4. Examples

### Simple Feature
```text
feat(api): add endpoint for smart tagging

Implement keyphrase extraction to automatically tag notes based on content.
```

### Bug Fix
```text
fix(auth): secure cookie flags for production

Update cookie configuration to include Secure and HttpOnly flags to prevent
token exposure in transit.
```

### Maintenance
```text
chore(deps): update aws-sdk to v3

Refresh cloud provider libraries to resolve minor security vulnerabilities.
```

### Breaking Change
```text
feat(db)!: migrate reporting views to PostgreSQL

Move all LARS Core Service views from Oracle to PostgreSQL.

BREAKING CHANGE: SQL queries using Oracle-specific syntax will no longer
function.
```

```

---

### Pro-Tip: The "Git Alias"
To make it easier to view this clean history, you can add this alias to your `.gitconfig` to see the tags in your terminal:
`git config --global alias.lg "log --graph --pretty=format:'%C(yellow)%h%Creset -%C(auto)%d%Creset %s %Cgreen(%cr) %C(bold blue)<%an>%Creset' --abbrev-commit"`