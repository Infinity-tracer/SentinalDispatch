    #!/usr/bin/env python3
"""
911 Call Transcript Generator for Maui Wildfire Scenario
Uses Google Gemini API to generate realistic emergency call transcripts
based on the August 8-9, 2023 Maui wildfire event, including Lahaina and other areas.
"""

import json
import os
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import google.genai as genai
from pathlib import Path

# Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "generated_calls"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Maui locations for realistic scenarios (including Lahaina and other areas)
MAUI_LOCATIONS = [
    # Lahaina area (heavily affected)
    {"name": "Front Street, Lahaina", "lat": 20.8783, "lon": -156.6825, "description": "Historic downtown Lahaina, densely populated, heavily damaged"},
    {"name": "Lahainaluna Road, Lahaina", "lat": 20.8844, "lon": -156.6800, "description": "Residential area, hillside, fire spread rapidly"},
    {"name": "Kaanapali Beach", "lat": 20.9289, "lon": -156.6944, "description": "Resort area, tourist zone, evacuation point"},
    {"name": "Puunoa Point, Lahaina", "lat": 20.8700, "lon": -156.6800, "description": "Residential neighborhood near waterfront"},
    {"name": "Lahaina Harbor", "lat": 20.8700, "lon": -156.6825, "description": "Waterfront, evacuation point, boats used for rescue"},
    {"name": "Lahaina Civic Center", "lat": 20.8900, "lon": -156.6850, "description": "Public facility, used as shelter"},
    {"name": "Lahaina Intermediate School", "lat": 20.8800, "lon": -156.6800, "description": "School, potential shelter, evacuation center"},
    {"name": "Maui Preparatory Academy", "lat": 20.9200, "lon": -156.6900, "description": "School campus, evacuation point"},
    {"name": "Lahaina Bypass Road", "lat": 20.9000, "lon": -156.6800, "description": "Main evacuation route, traffic congestion"},
    {"name": "Lahaina Jodo Mission", "lat": 20.8750, "lon": -156.6820, "description": "Historic site, near fire zone"},

    # Kihei area (south Maui)
    {"name": "Kihei Town", "lat": 20.7750, "lon": -156.4600, "description": "South Maui town, tourist area, monitoring fire spread"},
    {"name": "Maalaea Harbor", "lat": 20.7900, "lon": -156.5100, "description": "Harbor, potential evacuation point"},
    {"name": "Wailea", "lat": 20.6900, "lon": -156.4400, "description": "Resort area, monitoring conditions"},

    # Kahului area (central Maui)
    {"name": "Kahului Airport", "lat": 20.8986, "lon": -156.4306, "description": "Main airport, evacuation hub, flights disrupted"},
    {"name": "Kahului Town", "lat": 20.8900, "lon": -156.4700, "description": "Central Maui commercial area, shelter location"},
    {"name": "Maui Memorial Medical Center", "lat": 20.9000, "lon": -156.4800, "description": "Main hospital, receiving patients"},

    # Wailuku area (central Maui)
    {"name": "Wailuku Town", "lat": 20.8900, "lon": -156.5000, "description": "County seat, government center, coordination point"},
    {"name": "Iao Valley", "lat": 20.8800, "lon": -156.5500, "description": "Valley area, monitoring for fire spread"},

    # Upcountry areas
    {"name": "Makawao", "lat": 20.8500, "lon": -156.3100, "description": "Upcountry town, monitoring fire conditions"},
    {"name": "Pukalani", "lat": 20.8400, "lon": -156.3400, "description": "Upcountry residential area"},
    {"name": "Kula", "lat": 20.7900, "lon": -156.3300, "description": "Upcountry agricultural area"},

    # Other areas
    {"name": "Paia", "lat": 20.9100, "lon": -156.3700, "description": "North shore town, monitoring conditions"},
    {"name": "Hana", "lat": 20.7600, "lon": -155.9900, "description": "Remote east Maui, isolated area"},
    {"name": "Napili", "lat": 20.9800, "lon": -156.6800, "description": "Resort area north of Lahaina, evacuation zone"},
]

# Scenario types for variety
SCENARIO_TYPES = [
    "medical_emergency",
    "fire_proximity",
    "evacuation_assistance",
    "trapped_person",
    "smoke_inhalation",
    "elderly_resident",
    "child_separation",
    "pet_rescue",
    "structure_fire",
    "road_blocked",
]

# Time progression during the event (August 8, 2023)
EVENT_TIMELINE = [
    {"time": "2023-08-08T06:00:00", "phase": "early", "description": "Fire starts, initial reports"},
    {"time": "2023-08-08T10:00:00", "phase": "developing", "description": "Fire spreading, evacuations begin"},
    {"time": "2023-08-08T14:00:00", "phase": "critical", "description": "Rapid spread, multiple calls"},
    {"time": "2023-08-08T18:00:00", "phase": "peak", "description": "Peak emergency, widespread destruction"},
    {"time": "2023-08-08T22:00:00", "phase": "aftermath", "description": "Search and rescue, recovery"},
]

# Vulnerability factors
VULNERABILITY_FACTORS = [
    {"age": 75, "conditions": ["COPD", "heart_disease"], "mobility": "limited"},
    {"age": 45, "conditions": ["asthma"], "mobility": "normal"},
    {"age": 8, "conditions": [], "mobility": "normal"},
    {"age": 32, "conditions": [], "mobility": "normal"},
    {"age": 68, "conditions": ["diabetes"], "mobility": "limited"},
    {"age": None, "conditions": [], "mobility": "normal"},  # No age/condition info
]


class CallGenerator:
    """Generates realistic 911 call transcripts using Gemini API"""
    
    def __init__(self):
        """Initialize Gemini client with gemini-2.5-flash model"""
        self.client = genai.Client(api_key=GEMINI_API_KEY)
        self.model_name = "gemini-2.5-flash"
    
    def generate_call_transcript(self, scenario: Dict) -> str:
        """Generate a single call transcript using Gemini"""
        location_name = scenario['location']['name']
        location_desc = scenario['location']['description']

        # Determine if location is in Lahaina or elsewhere in Maui
        is_lahaina = 'Lahaina' in location_name or 'Kaanapali' in location_name or 'Napili' in location_name

        prompt = f"""Generate a realistic 911 emergency call transcript for the Maui wildfire on August 8, 2023.

Context:
- Location: {location_name} ({location_desc})
- Coordinates: {scenario['location']['lat']}, {scenario['location']['lon']}
- Time: {scenario['timestamp']}
- Scenario Type: {scenario['scenario_type']}
- Caller Age: {scenario.get('age', 'unknown')}
- Medical Conditions: {', '.join(scenario.get('conditions', [])) if scenario.get('conditions') else 'none'}
- Phase: {scenario['phase']}
- Area: {'Lahaina (heavily affected area)' if is_lahaina else 'Other Maui area (monitoring/evacuation)'}

Requirements:
1. Make it sound like a real, panicked emergency call
2. Include natural speech patterns, interruptions, background noise descriptions
3. Mention specific landmarks, streets, or locations in {location_name.split(',')[0] if ',' in location_name else location_name}
4. Include urgency and emotion appropriate to the situation
5. Reference fire, smoke, evacuation concerns, or monitoring needs
6. Include location details that would help dispatchers
7. Keep it between 30-90 seconds of conversation (roughly 100-300 words)
8. Format as a dialogue between caller and dispatcher
9. If location is outside Lahaina, caller may be reporting conditions, asking about evacuation, or requesting assistance for relatives in affected areas

Generate ONLY the transcript text, no additional commentary."""

        try:
            response = self.client.models.generate_content(model=self.model_name, contents=prompt)
            return response.text.strip()
        except Exception as e:
            print(f"Error generating call: {e}")
            return None

    def create_scenario(self, call_id: str, timestamp: datetime) -> Dict:
        """Create a scenario configuration for a call"""
        # Weight locations: 60% Lahaina (heavily affected), 40% other Maui areas
        if random.random() < 0.6:
            # Lahaina area locations
            lahaina_locations = [loc for loc in MAUI_LOCATIONS if 'Lahaina' in loc['name'] or 'Kaanapali' in loc['name'] or 'Napili' in loc['name']]
            location = random.choice(lahaina_locations)
        else:
            # Other Maui locations
            other_locations = [loc for loc in MAUI_LOCATIONS if 'Lahaina' not in loc['name'] and 'Kaanapali' not in loc['name'] and 'Napili' not in loc['name']]
            location = random.choice(other_locations)
        scenario_type = random.choice(SCENARIO_TYPES)

        # Determine phase based on timestamp
        if timestamp.hour >= 18:
            phase = "peak"
        elif timestamp.hour >= 14:
            phase = "critical"
        elif timestamp.hour >= 10:
            phase = "developing"
        else:
            phase = "early"

        # Add vulnerability factors (optional, for some calls)
        vulnerability = None
        if random.random() < 0.4:  # 40% of calls include vulnerability info
            vulnerability = random.choice(VULNERABILITY_FACTORS)

        scenario = {
            "callId": call_id,
            "timestamp": timestamp.isoformat() + "Z",
            "callerLocation": {
                "lat": location["lat"] + random.uniform(-0.01, 0.01),  # Add slight variation
                "lon": location["lon"] + random.uniform(-0.01, 0.01)
            },
            "location": location,
            "scenario_type": scenario_type,
            "phase": phase,
        }

        if vulnerability:
            scenario["age"] = vulnerability["age"]
            scenario["conditions"] = vulnerability["conditions"]
            scenario["mobility"] = vulnerability["mobility"]
        
        return scenario

    def generate_call_data(self, scenario: Dict) -> Optional[Dict]:
        """Generate complete call data including transcript"""
        print(f"Generating call {scenario['callId']}...")
        
        transcript = self.generate_call_transcript(scenario)
        if not transcript:
            return None

        # Estimate call duration (rough approximation: 150 words per minute)
        word_count = len(transcript.split())
        estimated_duration = max(30, min(180, int(word_count / 2.5)))  # 30-180 seconds

        call_data = {
            "callId": scenario["callId"],
            "timestamp": scenario["timestamp"],
            "callerLocation": scenario["callerLocation"],
            "transcript": transcript,
            "metadata": {
                "phoneNumber": f"+1-808-555-{random.randint(1000, 9999)}",
                "callDuration": estimated_duration,
                "scenarioType": scenario["scenario_type"],
                "phase": scenario["phase"],
            }
        }

        # Add optional vulnerability metadata
        if "age" in scenario:
            call_data["metadata"]["callerAge"] = scenario["age"]
        if "conditions" in scenario and scenario["conditions"]:
            call_data["metadata"]["knownConditions"] = scenario["conditions"]
        if "mobility" in scenario:
            call_data["metadata"]["mobility"] = scenario["mobility"]

        return call_data
    
    def generate_batch(self, num_calls: int, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Generate a batch of calls over a time period"""
        calls = []
        time_range = (end_time - start_time).total_seconds()

        for i in range(num_calls):
            # Distribute calls across time period (more calls during peak hours)
            hour = start_time.hour + (i / num_calls) * ((end_time - start_time).total_seconds() / 3600)
            if hour >= 14 and hour <= 20:  # Peak hours: more calls
                time_offset = random.uniform(0, time_range * 0.3)  # Concentrate in peak
            else:
                time_offset = random.uniform(0, time_range)

            call_time = start_time + timedelta(seconds=time_offset)
            call_id = f"CALL-{call_time.strftime('%Y%m%d')}-{i+1:04d}"

            scenario = self.create_scenario(call_id, call_time)
            call_data = self.generate_call_data(scenario)

            if call_data:
                calls.append(call_data)
            
            # Rate limiting: be respectful of API limits
            time.sleep(1)  # 1 second between calls
        
        return calls


def main():
    """Main function to generate calls"""
    import argparse

    parser = argparse.ArgumentParser(description="Generate 911 call transcripts for Maui wildfire (including Lahaina)")
    parser.add_argument("--num-calls", type=int, default=50, help="Number of calls to generate")
    parser.add_argument("--start-time", type=str, default="2023-08-08T06:00:00", 
                       help="Start time (ISO format)")
    parser.add_argument("--end-time", type=str, default="2023-08-08T23:59:00",
                       help="End time (ISO format)")
    parser.add_argument("--output", type=str, help="Output file path")

    args = parser.parse_args()

    # Parse times
    start_time = datetime.fromisoformat(args.start_time.replace("Z", "+00:00"))
    end_time = datetime.fromisoformat(args.end_time.replace("Z", "+00:00"))

    # Generate calls
    generator = CallGenerator()
    print("Using model: gemini-2.5-flash")
    print(f"Generating {args.num_calls} calls from {start_time} to {end_time}...")
    print("Locations: 60% Lahaina area, 40% other Maui areas")
    calls = generator.generate_batch(args.num_calls, start_time, end_time)
    # Save output
    output_file = args.output or OUTPUT_DIR / f"maui_calls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(calls, f, indent=2)

    print(f"\nGenerated {len(calls)} calls")
    print(f"Saved to: {output_file}")

    # Print sample
    if calls:
        print("\n--- Sample Call ---")
        sample = calls[0]
        print(f"Call ID: {sample['callId']}")
        print(f"Time: {sample['timestamp']}")
        print(f"Location: {sample['callerLocation']}")
        print(f"Transcript (first 200 chars): {sample['transcript'][:200]}...")


if __name__ == "__main__":
    main()

