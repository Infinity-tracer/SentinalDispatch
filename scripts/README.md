# 911 Call Generator Script

This script generates realistic 911 emergency call transcripts for the Maui wildfire scenario (including Lahaina and other areas) using Google's Gemini API (gemini-2.5-flash model).

## Setup

1. Install dependencies (from project root):
```bash
uv sync
```

This will install all project dependencies including `google-generativeai`.

2. Set your Gemini API key:
```bash
export GEMINI_API_KEY="your-api-key-here"
```

Or pass it via command line argument.

## Usage

### Basic Usage
```bash
# Using uv (recommended)
uv run python scripts/generate_911_calls.py --num-calls 50

# Or if virtual environment is activated
python scripts/generate_911_calls.py --num-calls 50
```

### Custom Time Range
```bash
uv run python scripts/generate_911_calls.py \
  --num-calls 100 \
  --start-time "2023-08-08T14:00:00" \
  --end-time "2023-08-08T18:00:00"
```

### Custom Output File
```bash
uv run python scripts/generate_911_calls.py \
  --num-calls 50 \
  --output data/my_calls.json
```

## Options

- `--num-calls`: Number of calls to generate (default: 50)
- `--start-time`: Start time in ISO format (default: 2023-08-08T06:00:00)
- `--end-time`: End time in ISO format (default: 2023-08-08T22:00:00)
- `--output`: Output file path (default: data/generated_calls/maui_calls_TIMESTAMP.json)

**Note**: The script uses the `gemini-2.5-flash` model (hardcoded).

## Output Format

The script generates JSON files with the following structure:

```json
[
  {
    "callId": "CALL-20230808-0001",
    "timestamp": "2023-08-08T14:23:45Z",
    "callerLocation": {
      "lat": 20.8783,
      "lon": -156.6825
    },
    "transcript": "Dispatcher: 911, what's your emergency?\nCaller: ...",
    "metadata": {
      "phoneNumber": "+1-808-555-0123",
      "callDuration": 127,
      "scenarioType": "medical_emergency",
      "phase": "critical",
      "callerAge": 65,
      "knownConditions": ["asthma"],
      "mobility": "limited"
    }
  }
]
```

## Features

- **Maui-Wide Coverage**: Generates calls across Maui locations (60% Lahaina area, 40% other areas including Kihei, Kahului, Wailuku, Upcountry)
- **Gemini 2.5 Flash Model**: Uses gemini-2.5-flash model for fast, high-quality transcript generation
- **Realistic Scenarios**: Generates calls based on actual Maui locations and event timeline
- **Variety**: Multiple scenario types (medical, evacuation, fire proximity, etc.)
- **Vulnerability Factors**: Includes age and medical conditions for some calls
- **Time Distribution**: More calls during peak emergency hours
- **Location Accuracy**: Uses real Lahaina coordinates with slight variations

## Cost Considerations

Gemini API pricing applies. Check current pricing at:
https://ai.google.dev/pricing

Typical usage: ~1000-2000 tokens per call generation.

