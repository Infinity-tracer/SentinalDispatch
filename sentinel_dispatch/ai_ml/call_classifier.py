"""
Call Classification Module using Gemini API.

This module provides call classification functionality that extracts structured
information from emergency call transcripts using Google's Gemini API.

The module is designed to be reusable in both:
- FastAPI services (direct function calls)
- Streaming Agents (via External Service Pattern)
"""

import json
import os
from typing import Optional
import google.genai as genai

from sentinel_dispatch.ai_ml.models import CallClassificationResult
from sentinel_dispatch.utils.logging_config import get_logger

logger = get_logger(__name__)


class CallClassifier:
    """Classifies emergency call transcripts using Gemini API."""
    
    def __init__(self, api_key: Optional[str] = None, model_name: str = "gemini-2.5-flash"):
        """
        Initialize the call classifier.
        
        Args:
            api_key: Gemini API key (defaults to GEMINI_API_KEY env var)
            model_name: Gemini model to use (default: gemini-2.5-flash)
        """
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError(
                "Gemini API key is required. Set GEMINI_API_KEY environment variable "
                "or pass api_key parameter."
            )
        
        self.model_name = model_name
        self.client = genai.Client(api_key=self.api_key)
        self.model_name = model_name
        
        logger.info(
            "sentinel_dispatch.ai_ml.call_classifier.CallClassifier.__init__.initialized",
            model=model_name
        )
    
    def _build_classification_prompt(self, transcript: str) -> str:
        """
        Build the structured prompt for call classification.
        
        Args:
            transcript: The emergency call transcript
            
        Returns:
            Formatted prompt string
        """
        prompt = f"""You are an emergency dispatch AI assistant analyzing a 911 call transcript during a wildfire emergency. Extract structured information from the following call transcript.

Call Transcript:
{transcript}

Analyze this transcript and provide a JSON response with the following structure:
{{
  "urgency": <integer 1-5, where 1=low priority, 5=critical/immediate>,
  "incident_type": "<one of: medical, fire, evacuation, other>",
  "entities": {{
    "locations": [
      {{
        "name": "<location name>",
        "lat": <latitude if mentioned or null>,
        "lon": <longitude if mentioned or null>,
        "context": "<context: 'near', 'at', 'between', etc. or null>"
      }}
    ],
    "symptoms": [
      {{
        "symptom": "<symptom or condition>",
        "severity": "<'mild', 'severe', 'critical', or null>"
      }}
    ],
    "hazards": [
      {{
        "hazard": "<hazard type: fire, smoke, structural damage, etc.>",
        "proximity": "<'close', 'approaching', 'immediate', or null>"
      }}
    ],
    "people_mentioned": [
      {{
        "role": "<'caller', 'neighbor', 'family member', 'trapped person', etc.>",
        "status": "<'safe', 'in danger', 'trapped', 'injured', or null>",
        "location": "<where they are mentioned to be, or null>"
      }}
    ]
  }},
  "sentiment": <float 0.0-1.0, where 0.0=calm, 1.0=extreme panic>,
  "key_phrases": [<list of critical phrases indicating urgency>],
  "people_affected": <integer or null, count of people affected - estimate from context if not explicitly stated>,
  "confidence": <float 0.0-1.0, confidence in the classification>,
  "operator_questions": [
    "<suggested questions for the operator to ask the caller if critical information is missing>"
  ]
}}

Guidelines:
- Urgency 5: Immediate life-threatening situation, active fire, trapped people, medical emergency
- Urgency 4: High priority, fire approaching, evacuation needed, serious medical issue
- Urgency 3: Moderate priority, fire nearby, smoke inhalation, minor injuries
- Urgency 2: Low-moderate priority, monitoring situation, precautionary
- Urgency 1: Low priority, informational, false alarm, resolved

- Incident type "medical": Health emergencies, injuries, medical conditions
- Incident type "fire": Active fires, fire-related emergencies
- Incident type "evacuation": Evacuation requests, people needing to leave
- Incident type "other": Other emergencies not fitting above categories

Entity Extraction:
- Locations: Extract all location names (streets, landmarks, buildings). Include coordinates only if explicitly mentioned in the call. Context should indicate relationship (e.g., "near Front Street", "at the house").
- Symptoms: Extract medical symptoms or conditions. Include severity if indicated by language (e.g., "severe difficulty breathing" → severity: "severe").
- Hazards: Extract hazards mentioned (fire, smoke, structural damage, falling debris, etc.). Include proximity if indicated (e.g., "fire is close" → proximity: "close").
- People mentioned: Identify all people referenced. Include their role (caller, neighbor, family member, etc.), status if mentioned (safe, in danger, trapped, injured), and location if specified.

- Sentiment: Assess panic/stress level from language, tone indicators, urgency words
- Key phrases: Extract phrases like "can't breathe", "fire is close", "trapped", "need help now"

People Affected (CRITICAL - Always try to estimate):
- Count ALL people mentioned or implied in the transcript (caller, family members, neighbors, trapped individuals, etc.)
- If explicit numbers are mentioned (e.g., "3 people", "my family of 4"), use that number
- If people are mentioned individually (e.g., "me and my neighbor", "my wife and kids"), count them
- If plural references are used (e.g., "we", "people are", "others"), estimate based on context (minimum 2 for "we", estimate from context for "people")
- If only the caller is mentioned with no others, use 1
- Only use null if absolutely no people are mentioned or implied (very rare)
- Examples:
  * "We're trapped" → people_affected: 2 (caller + at least one other)
  * "People are jumping in the water" → people_affected: 3+ (estimate from context, use 3 as minimum)
  * "My family is here" → people_affected: 3 (caller + family, estimate 2+ family members)
  * "Just me" → people_affected: 1

Operator Questions (IMPORTANT - Include when information is missing):
- If the caller's age is not mentioned, add: "What is your age?" or "How old are you?"
- If medical conditions are not mentioned, add: "Do you have any medical conditions we should know about?" or "Are you taking any medications?"
- If the caller sounds elderly (based on voice description, context, or name), prioritize asking about age and medical conditions
- If multiple people are affected but their ages/conditions aren't mentioned, add: "What are the ages of the people with you?" and "Do any of them have medical conditions?"
- Only include questions for information that is truly missing and would be useful for risk assessment
- Format as clear, direct questions the operator can ask verbatim

Respond with ONLY valid JSON, no additional text or explanation."""
        
        return prompt
    
    def classify(self, transcript: str, cache_key: Optional[str] = None) -> CallClassificationResult:
        """
        Classify an emergency call transcript.
        
        Args:
            transcript: The emergency call transcript text
            cache_key: Optional cache key for similar calls (for future caching implementation)
            
        Returns:
            CallClassificationResult with structured classification data
            
        Raises:
            ValueError: If API call fails or response is invalid
        """
        logger.debug(
            "sentinel_dispatch.ai_ml.call_classifier.CallClassifier.classify.starting",
            transcript_length=len(transcript),
            cache_key=cache_key
        )
        
        try:
            prompt = self._build_classification_prompt(transcript)
            
            # Call Gemini API
            response = self.client.models.generate_content(model=self.model_name, contents=prompt)
            
            # Extract text from response
            response_text = response.text.strip()
            
            # Remove Markdown code blocks if present
            if response_text.startswith("```json"):
                response_text = response_text[7:]
            if response_text.startswith("```"):
                response_text = response_text[3:]
            if response_text.endswith("```"):
                response_text = response_text[:-3]
            response_text = response_text.strip()
            
            # Parse JSON response
            try:
                result_dict = json.loads(response_text)
            except json.JSONDecodeError as e:
                logger.error(
                    "sentinel_dispatch.ai_ml.call_classifier.CallClassifier.classify.json_parse_error",
                    error=str(e),
                    response_preview=response_text[:200]
                )
                raise ValueError(f"Failed to parse Gemini response as JSON: {e}")
            
            # Validate and create result object
            result = CallClassificationResult(**result_dict)
            
            logger.info(
                "sentinel_dispatch.ai_ml.call_classifier.CallClassifier.classify.completed",
                urgency=result.urgency,
                incident_type=result.incident_type,
                confidence=result.confidence
            )
            
            return result
            
        except Exception as e:
            logger.error(
                "sentinel_dispatch.ai_ml.call_classifier.CallClassifier.classify.error",
                error=str(e),
                error_type=type(e).__name__
            )
            raise
    
    def classify_batch(self, transcripts: list[str]) -> list[CallClassificationResult]:
        """
        Classify multiple call transcripts.
        
        Args:
            transcripts: List of call transcript texts
            
        Returns:
            List of CallClassificationResult objects
        """
        results = []
        for i, transcript in enumerate(transcripts):
            try:
                result = self.classify(transcript)
                results.append(result)
            except Exception as e:
                logger.error(
                    "sentinel_dispatch.ai_ml.call_classifier.CallClassifier.classify_batch.item_error",
                    index=i,
                    error=str(e)
                )
                # Continue with other transcripts even if one fails
                # Could return a default result or re-raise depending on requirements
                raise
        
        return results


# Convenience function for direct use
def classify_call(transcript: str, api_key: Optional[str] = None) -> CallClassificationResult:
    """
    Convenience function to classify a call transcript.
    
    Args:
        transcript: The emergency call transcript text
        api_key: Optional Gemini API key (defaults to GEMINI_API_KEY env var)
        
    Returns:
        CallClassificationResult with structured classification data
    """
    classifier = CallClassifier(api_key=api_key)
    return classifier.classify(transcript)

