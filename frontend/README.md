# BizAgent Frontend

Next.js 15 dashboard for the BizAgent SaaS revenue analytics backend.

## Stack

| Layer | Choice |
|-------|--------|
| Framework | Next.js 15 (App Router) |
| Language | TypeScript (strict) |
| Styling | Tailwind CSS v3 |
| UI primitives | Hand-rolled shadcn-style (Button, Card, Badge) |
| Charts | Recharts 2 |
| Icons | lucide-react |

## Prerequisites

- Node.js ≥ 18
- BizAgent backend running at `http://localhost:8000`

## Quick start

```bash
cd frontend

# 1. Install dependencies
npm install

# 2. Configure environment
cp .env.local.example .env.local
# Edit .env.local if your backend runs on a different port

# 3. Start dev server
npm run dev
```

Open [http://localhost:3000](http://localhost:3000).

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NEXT_PUBLIC_API_BASE_URL` | `http://localhost:8000` | Base URL of the FastAPI backend. No trailing slash. |

## Project structure

```
frontend/
├── app/
│   ├── layout.tsx        # Root layout (Inter font, metadata)
│   ├── page.tsx          # Main dashboard (central state, two-column layout)
│   └── globals.css       # Tailwind directives + CSS custom properties
├── components/
│   ├── ui/               # Primitive components (Button, Card, Badge)
│   ├── StatusPill.tsx    # UIState machine indicator
│   ├── ErrorBanner.tsx   # Dismissible error display
│   ├── DatasetUploader.tsx
│   ├── KpiRunPanel.tsx
│   ├── HeadlineCards.tsx
│   ├── MrrChart.tsx
│   ├── RetentionChart.tsx
│   └── InsightsPanel.tsx
├── lib/
│   ├── utils.ts          # cn() helper
│   ├── types.ts          # TypeScript interfaces mirroring backend schemas
│   ├── api.ts            # Typed fetch wrappers for all backend endpoints
│   └── date.ts           # Formatting helpers (currency, percent, month)
└── .env.local.example
```

## UI state machine

The `StatusPill` in the top bar reflects the current UI state:

| State | When |
|-------|------|
| `idle` | Default — no operation in progress |
| `thinking` | Fetching KPI data or waiting for AI insights |
| `listening` | (Placeholder) Voice input active |
| `transcribing` | (Placeholder) Converting speech to text |
| `speaking` | (Placeholder) Text-to-speech output |

## Dashboard flow

1. **Upload** a revenue events CSV via the sidebar uploader
2. **Run KPIs** to compute MRR components, churn rates, segments, and cohorts
3. Charts and headline metrics populate automatically
4. **Generate Insights** for any available month — the backend calls OpenAI (or returns a stub if no key is set), caches the result, and returns a structured analysis
5. Re-generating the same month with identical KPI data returns the cached response instantly (`cached: true` badge)

## Building for production

```bash
npm run build
npm start
```
