# DailyDock

DailyDock is a personal infrastructure and automation platform designed to centralize infrastructure monitoring, personal data management, automation, notifications, and system interaction.

This repository contains the application foundation and core architecture:
- **Backend:** Python-first modular monolith built with FastAPI.
- **Frontend:** React application built with Vite and JavaScript (JSX).

---

## Prerequisites

- **Python:** 3.10 or newer (tested on Python 3.13)
- **Node.js:** 18 or newer (tested on Node v26)
- **npm:** 9 or newer

---

## Project Structure

```
DailyDock/
├── backend/
│   ├── app/
│   │   ├── api/
│   │   │   └── health.py              # Health check route handler
│   │   ├── core/
│   │   │   ├── errors.py              # Standardized exception hierarchy & API handlers
│   │   │   ├── events.py              # Bounded EventBus & selective retry records
│   │   │   ├── logging.py             # Lightweight logging & secret redaction
│   │   │   ├── module.py              # BaseModule & ModuleStatus
│   │   │   ├── registry.py            # ModuleRegistry, topological sort, startup rollback
│   │   │   └── tasks.py               # Bounded TaskManager & pending queue limits
│   │   ├── config.py                  # Centralized configuration & built-in .env parser
│   │   └── main.py                    # FastAPI entry point & lifespan management
│   ├── tests/
│   │   ├── test_config.py             # Config defaults, env loading, validation, sanitization
│   │   ├── test_errors.py             # Exception hierarchy, context capture, API responses
│   │   ├── test_events.py             # Event dispatch, isolation, selective retries, shutdown
│   │   ├── test_health.py             # Health check endpoint tests
│   │   ├── test_logging.py            # Logging format, namespacing, secret sanitization
│   │   ├── test_module_dependencies.py# Cycle detection, missing dependencies, sort order
│   │   ├── test_module_lifecycle.py   # Module startup, reverse shutdown, startup rollback
│   │   └── test_tasks.py              # Task execution, queue limits, cancellation, shutdown
│   ├── requirements.txt               # Production dependencies
│   ├── requirements-dev.txt           # Development & test dependencies
│   └── .env.example                   # Backend environment example
├── frontend/
│   ├── src/
│   │   ├── App.jsx                    # Minimal landing & health status UI
│   │   ├── App.css                    # Component styling
│   │   ├── main.jsx                   # React root entry point
│   │   └── index.css                  # Global layout styles
│   ├── index.html                     # HTML entry point
│   ├── package.json                   # Frontend dependencies & scripts
│   ├── vite.config.js                 # Vite config & API dev proxy
│   └── .env.example                   # Frontend environment example
└── README.md
```

---

## Backend Setup & Execution

### 1. Create and Activate Virtual Environment

From the project root:

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install Dependencies

```bash
pip install -r requirements-dev.txt
```

### 3. Environment Configuration (Optional)

Copy `.env.example` to `.env` if custom configuration is needed:

```bash
cp .env.example .env
```

Available environment variables:
- `DAILYDOCK_ENV`: Runtime environment (`development`, `production`, `test`). Default: `development`
- `DAILYDOCK_HOST`: Host interface to bind. Default: `0.0.0.0`
- `DAILYDOCK_PORT`: Port number to bind. Default: `8000`
- `DAILYDOCK_CORS_ORIGINS`: Comma-separated list of allowed CORS origins. Default: `http://localhost:5173,http://127.0.0.1:5173`
- `DAILYDOCK_LOG_LEVEL`: Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`). Default: `INFO`
- `DAILYDOCK_SHUTDOWN_GRACE_PERIOD`: Seconds to allow active tasks and events to finish before shutdown timeout. Default: `5.0`
- `DAILYDOCK_TASK_MAX_CONCURRENCY`: Maximum concurrent background tasks executing simultaneously. Default: `5`
- `DAILYDOCK_TASK_MAX_PENDING`: Maximum tasks waiting in queue before new submissions are rejected. Default: `20`
- `DAILYDOCK_EVENT_MAX_CONCURRENCY`: Maximum concurrent event handler executions. Default: `10`
- `DAILYDOCK_DATA_DIR`: Directory for application data. Relative paths are resolved from the repository root; default: `data`
- `DAILYDOCK_DATABASE_URL`: SQLAlchemy database URL. Default: SQLite database at `<DAILYDOCK_DATA_DIR>/dailydock.db`
- `DAILYDOCK_DATABASE_ECHO`: Enable SQL statement logging (`true`/`false`). Default: `false`
- `DAILYDOCK_DATABASE_BUSY_TIMEOUT`: SQLite busy timeout in milliseconds. Default: `5000`
- `DAILYDOCK_DATABASE_WAL_ENABLED`: Enable SQLite write-ahead logging (`true`/`false`). Default: `true`

### 4. Apply Database Migrations

Apply the schema before the first backend start and after upgrading to a release with new migrations. Migrations are not run automatically during application startup.

From the repository root:

```bash
backend/.venv/bin/alembic -c backend/alembic.ini upgrade head
```

Or, from the `backend` directory after activating its virtual environment:

```bash
alembic upgrade head
```

Alembic uses `DAILYDOCK_DATABASE_URL` and `DAILYDOCK_DATA_DIR` from the backend configuration. The migration directory is resolved relative to `backend/alembic.ini`, so either invocation works independently of the current directory.

### 5. Run the Backend Server

Using uvicorn with hot reload:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Or execute directly via Python:

```bash
python app/main.py
```

The API will be available at `http://localhost:8000`.  
The health check endpoint is at `http://localhost:8000/api/health`.

### 6. Run Backend Tests

```bash
PYTHONPATH=backend pytest backend/tests -v
```

---

## Frontend Setup & Execution

### 1. Install Dependencies

From the project root:

```bash
cd frontend
npm install
```

### 2. Environment Configuration (Optional)

Copy `.env.example` to `.env` if pointing to a non-default backend address:

```bash
cp .env.example .env
```

Available environment variables:
- `VITE_API_BASE_URL`: Base URL of the backend (e.g. `http://localhost:8000`). If left empty, the Vite dev server proxies `/api` directly to `http://localhost:8000`.

### 3. Run Development Server

```bash
npm run dev
```

The frontend will be accessible at `http://localhost:5173`.  
It automatically connects to the backend and displays the connection status.

### 4. Build for Production

```bash
npm run build
```

The production output is placed into `frontend/dist`.

---

## Verification

1. Start the backend: `cd backend && source .venv/bin/activate && uvicorn app.main:app --reload`
2. Start the frontend: `cd frontend && npm run dev`
3. Open `http://localhost:5173` in your browser.
4. You should see "Backend Status: Connected" along with the service health, version, and environment.
