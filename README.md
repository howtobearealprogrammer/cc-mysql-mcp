# cc-mysql

**A reliable, Windows-first MySQL MCP server for Claude Code.**

`cc-mysql` lets Claude Code interact directly with MySQL or MariaDB databases on your local Windows machine — without Docker, without bash assumptions, and without fragile tooling.

If you work primarily on **Windows**, use **PowerShell**, and want a MySQL MCP server that stays connected and predictable, this is for you.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Node.js](https://img.shields.io/badge/node-%3E%3D18-brightgreen)](https://nodejs.org/)

---

## Why this exists

Many MCP database servers assume:

- macOS or Linux
- bash-first workflows
- Docker as a baseline dependency
- clean, opinionated environments

That works well — until it doesn’t.

`cc-mysql` exists for developers who:

- Work primarily on **Windows**
- Use **PowerShell**
- Prefer **direct Node.js execution**
- Want **predictable behaviour**
- Need tooling that remains stable in real-world dev environments
- Have very messy environments

If other tools work for you, that’s great.  
This exists for when they don’t.

---

## What it does

`cc-mysql` is a **Model Context Protocol (MCP) server** that exposes MySQL access to Claude Code.

It provides Claude with structured tools to:

| Tool               | Purpose                                    |
| ------------------ | ------------------------------------------ |
| `onboarding`       | Teaches Claude safe and effective DB usage |
| `list_tables`      | Lists all tables                           |
| `get_table_schema` | Returns complete schema information        |
| `execute_query`    | Executes arbitrary SQL                     |

Everything runs **locally**, using your MySQL credentials, under your control.

---

## ⚠️ Important: No safety rails

This server executes **exactly** the SQL it is given.

There are:

- No confirmation prompts
- No validation
- No undo
- No protection from destructive operations

If a query runs in MySQL, it will run here.

**Use a restricted database user.**  
**Do not point this at production unless you understand the risk.**

This is deliberate.

---

## Quick Start

### Requirements

- Windows 10 / 11 (Mac/Linux should also work)
- Node.js 18+
- MySQL 5.7+ or MariaDB
- Claude Code

---

### 1. Clone and build

```powershell
git clone https://github.com//howtobearealprogrammer/cc-mysql-mcp.git
cd cc-mysql\src
npm install
npm run build
```

The compiled server will be located at:

```
src\dist\index.js
```

---

### 2. Configure Claude Code MCP

Add the following to your MCP configuration file (`.claude/mcp.json` or local `.mcp.json`):

```json
{
  "mcpServers": {
    "cc-mysql": {
      "command": "node",
      "args": ["C:\\path\\to\\cc-mysql\\src\\dist\\index.js"],
      "env": {
        "MYSQL_HOST": "localhost",
        "MYSQL_PORT": "3306",
        "MYSQL_USER": "db_user",
        "MYSQL_PASSWORD": "your_password",
        "MYSQL_DATABASE": "your_database",
        "LOG_ENABLED": "false",
        "OTEL_ENABLED": "false"
      }
    }
  }
}
```

> **Mac/Linux paths:** use forward slashes instead.

---

### 3. Verify installation

Restart Claude Code and ask:

```
Run the onboarding tool from cc-mysql
```

If Claude responds with guidance about your database, you’re connected.

---

## Configuration Reference

### Required

| Variable         | Description       |
| ---------------- | ----------------- |
| `MYSQL_HOST`     | MySQL hostname    |
| `MYSQL_PORT`     | MySQL port        |
| `MYSQL_USER`     | Database user     |
| `MYSQL_PASSWORD` | Database password |

### Optional

| Variable                 | Description          | Default |
| ------------------------ | -------------------- | ------- |
| `MYSQL_DATABASE`         | Default database     | —       |
| `MYSQL_CONNECTION_LIMIT` | Pool size            | 10      |
| `LOG_ENABLED`            | Enable debug logging | false   |
| `LOG_PATH`               | Custom log path      | auto    |
| `OTEL_ENABLED`           | Enable telemetry     | false   |

---

## OpenTelemetry (optional)

For production or long-running use, OpenTelemetry can be enabled:

```json
{
  "OTEL_ENABLED": "true",
  "OTEL_ENDPOINT": "http://localhost:4318",
  "OTEL_SERVICE_NAME": "cc-mysql"
}
```

<details>
<summary>Full OpenTelemetry options</summary>

| Variable            | Description        | Default   |
| ------------------- | ------------------ | --------- |
| `OTEL_ENABLED`      | Enable telemetry   | false     |
| `OTEL_ENDPOINT`     | Collector URL      | —         |
| `OTEL_HOST`         | Collector hostname | localhost |
| `OTEL_PORT`         | Collector port     | 4318      |
| `OTEL_PROTOCOL`     | http / https       | http      |
| `OTEL_SERVICE_NAME` | Trace service name | cc-mysql  |

</details>

---

## Multiple databases

You can configure multiple MySQL connections by adding additional server entries:

```json
{
  "mcpServers": {
    "mysql-app": {
      "command": "node",
      "args": ["C:\\path\\to\\cc-mysql\\src\\dist\\index.js"],
      "env": {
        "MYSQL_HOST": "localhost",
        "MYSQL_DATABASE": "app_db",
        "MYSQL_USER": "app_user",
        "MYSQL_PASSWORD": "password",
        "MYSQL_PORT": "3306"
      }
    },
    "mysql-analytics": {
      "command": "node",
      "args": ["C:\\path\\to\\cc-mysql\\src\\dist\\index.js"],
      "env": {
        "MYSQL_HOST": "localhost",
        "MYSQL_DATABASE": "analytics_db",
        "MYSQL_USER": "analytics_user",
        "MYSQL_PASSWORD": "password",
        "MYSQL_PORT": "3306"
      }
    }
  }
}
```

---

## Troubleshooting

Enable logging to diagnose issues:

```json
{
  "LOG_ENABLED": "true"
}
```

Logs are written to `cc-mysql-debug.log` in the current directory, home directory, or temp folder.

<details>
<summary>Common issues</summary>

**Access denied**

- Check credentials
- Verify user permissions

**Cannot find module**

- Run `npm run build` in `src`
- Verify path points to `src\dist\index.js`

**Claude can’t see the server**

- Check MCP JSON syntax
- Restart Claude Code completely

**Connection refused**

- Verify MySQL is running
- Confirm host and port

</details>

---

## Running as a Windows service

<details>
<summary>Using NSSM</summary>

```cmd
nssm install CCMySQL "C:\Program Files\nodejs\node.exe"
nssm set CCMySQL AppParameters "C:\path\to\cc-mysql\src\dist\index.js"
nssm set CCMySQL AppEnvironmentExtra MYSQL_HOST=localhost MYSQL_PORT=3306 MYSQL_USER=db_user MYSQL_PASSWORD=password MYSQL_DATABASE=mydb
nssm start CCMySQL
```

</details>

---

## Development

```powershell
cd src
npm install
npm run build
npm run watch
npm start
```

---

## License

MIT

---

Built with Claude Code.  
Not maintained at all.
