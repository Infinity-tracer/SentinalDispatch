# Sentinel Dispatch Frontend

React/Next.js frontend for the Sentinel Dispatch emergency triage system.

## Features

- **Interactive Google Maps**: Real-time visualization of emergency calls (color-coded by risk score), fires, and alerts
- **WebSocket Integration**: Real-time updates via WebSocket connection
- **Call Management**: Sortable and filterable call list with risk scores
- **Responsive Design**: Modern UI with Tailwind CSS

## Setup

### Prerequisites

- Node.js 18+ and npm/yarn/pnpm
- Google Maps API key (Maps JavaScript API)

### Installation

```bash
cd frontend
npm install
# or
yarn install
# or
pnpm install
```

### Environment Variables

The frontend uses environment variables from the **root `.env` file** (not a separate frontend file).

Add these variables to your root `.env` file:

```bash
# Backend API keys
GOOGLE_MAPS_API_KEY=your_google_maps_api_key_here
GEMINI_API_KEY=your_gemini_api_key_here

# Frontend-specific variables (must be prefixed with NEXT_PUBLIC_)
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_WS_URL=ws://localhost:8000/ws
# Reference GOOGLE_MAPS_API_KEY to avoid duplication (variable expansion)
NEXT_PUBLIC_GOOGLE_MAPS_API_KEY="${GOOGLE_MAPS_API_KEY}"
```

**Note**:
- Next.js requires `NEXT_PUBLIC_` prefix for variables exposed to the browser
- The frontend automatically loads from the root `.env` file
- No need to create a separate `frontend/.env.local` file
- Variable expansion (`${VAR}`) is supported - you can reference other variables to avoid duplication

### Development

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

### Build

```bash
npm run build
npm start
```

## Project Structure

```
frontend/
├── app/              # Next.js app directory
│   ├── layout.tsx   # Root layout
│   ├── page.tsx     # Main dashboard page
│   └── globals.css  # Global styles
├── components/       # React components
│   ├── Map.tsx      # Google Maps component
│   └── CallList.tsx # Call list component
├── hooks/           # React hooks
│   └── useWebSocket.ts # WebSocket hook
├── lib/             # Utility libraries
│   ├── api.ts       # REST API client
│   └── websocket.ts # WebSocket client
├── types/           # TypeScript type definitions
│   └── index.ts     # All types
└── public/          # Static assets
    └── icons/       # Map marker icons
```

## Components

### Map Component

Displays emergency calls (color-coded by risk score), fires, and weather alerts on Google Maps for spatial triage decision-making.

**Props:**
- `calls`: Array of emergency calls
- `fires`: Array of fire data
- `riskScores`: Map of call ID to risk score (for color-coding markers)
- `selectedCallId`: Currently selected call ID
- `onCallClick`: Callback when a call marker is clicked

### CallList Component

Displays a prioritized list of emergency calls sorted by risk score for triage decision-making.

**Props:**
- `calls`: Array of emergency calls
- `riskScores`: Map of call ID to risk score
- `alerts`: Array of alerts (for highlighting high-priority cases)
- `selectedCallId`: Currently selected call ID
- `onCallSelect`: Callback when a call is selected

## WebSocket Integration

The frontend uses WebSocket for real-time updates. The `useWebSocket` hook manages the connection and provides:

- Automatic reconnection with exponential backoff
- Message queue for handling updates during reconnection
- Connection state monitoring

**Message Types:**
- `risk_score_update`: Risk score changes
- `new_call`: New emergency calls
- `fire_update`: Fire data updates
- `weather_update`: Weather data updates
- `alert`: Alert notifications (high-priority cases, fire weather warnings)

## API Integration

The frontend communicates with the FastAPI backend via REST endpoints:

- `GET /api/calls` - List all emergency calls
- `GET /api/calls/{call_id}` - Get call details
- `GET /api/risk-scores` - Get risk scores
- `GET /api/prioritized-cases` - Get prioritized cases for triage
- `GET /api/alerts` - Get active alerts
- `POST /api/alerts/{alert_id}/acknowledge` - Acknowledge an alert

## Development Notes

- The backend API must be running on the port specified in `NEXT_PUBLIC_API_URL`
- WebSocket connection is established automatically when the dashboard loads
- Map markers require icon files in `/public/icons/` directory
- Tailwind CSS is used for styling

