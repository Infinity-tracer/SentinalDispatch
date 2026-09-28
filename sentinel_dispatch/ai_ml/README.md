# AI/ML Modules

This directory contains reusable AI/ML logic modules that are used in both:
- **FastAPI services** (for initial development and testing)
- **Streaming Agents** (via External Service Pattern in production)

## Modules

### `call_classifier.py`

Call classification using Gemini API to extract structured information from emergency call transcripts.

**Features**:
- Extracts urgency level (1-5)
- Classifies incident type (medical, fire, evacuation, other)
- Extracts entities (locations, symptoms, hazards, people)
- Assesses sentiment/panic level
- Identifies key phrases and people affected

**Usage**:
```python
from sentinel_dispatch.ai_ml.call_classifier import classify_call

result = classify_call("I need help! The fire is getting close to my house...")
print(result.urgency)  # 5
print(result.incident_type)  # "fire"
print(result.entities["locations"])  # ["my house", "Front Street"]
```

**Configuration**:
- Requires `GEMINI_API_KEY` environment variable
- Uses `gemini-2.5-flash` model by default
- Can be configured via `CallClassifier` class

## Design Principles

1. **Reusable**: Modules can be used in multiple contexts (services, agents)
2. **Testable**: Pure functions with clear inputs/outputs
3. **Structured Output**: Pydantic models for type safety and validation
4. **Error Handling**: Comprehensive error handling and logging
5. **Caching Ready**: Designed to support caching (to be implemented)
6. **Hybrid Approach**: Supports both rule-based and ML-based scoring with automatic fallback

## Future Modules

- Additional alert types and escalation logic

