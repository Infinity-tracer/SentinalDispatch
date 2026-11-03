#!/usr/bin/env python3
"""
Emergency Call Transcript Generator - Multi-Region Support
Supports both US 911 calls and Indian 112 emergency calls for various calamities.
Uses Groq API to generate realistic emergency call transcripts.
"""

import json
import os
import random
import time
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from groq import Groq
from pathlib import Path

# Configuration
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "generated_calls"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ==================== INDIAN LOCATIONS ====================

INDIA_LOCATIONS = {
    # Mumbai - Floods
    "mumbai_floods": [
        {"name": "Dharavi, Mumbai", "lat": 19.0420, "lon": 72.8540, "description": "Densely populated slum area, severe waterlogging"},
        {"name": "Kurla East, Mumbai", "lat": 19.0728, "lon": 72.8826, "description": "Low-lying area, flood-prone"},
        {"name": "Sion, Mumbai", "lat": 19.0405, "lon": 72.8615, "description": "Railway station area, waterlogging"},
        {"name": "Andheri Subway, Mumbai", "lat": 19.1197, "lon": 72.8464, "description": "Major underpass, water accumulation"},
        {"name": "Bandra-Kurla Complex, Mumbai", "lat": 19.0596, "lon": 72.8656, "description": "Business district, drainage issues"},
        {"name": "Mahalaxmi, Mumbai", "lat": 18.9830, "lon": 72.8130, "description": "Low-lying area near sea"},
        {"name": "Chembur, Mumbai", "lat": 19.0522, "lon": 72.8947, "description": "Residential area, flood-affected"},
        {"name": "Thane Creek, Mumbai", "lat": 19.1866, "lon": 72.9756, "description": "Waterfront, high tide flooding"},
    ],

    # Odisha - Cyclone
    "odisha_cyclone": [
        {"name": "Puri Beach, Odisha", "lat": 19.7983, "lon": 85.8245, "description": "Coastal town, direct cyclone impact"},
        {"name": "Bhubaneswar, Odisha", "lat": 20.2961, "lon": 85.8245, "description": "State capital, wind damage"},
        {"name": "Cuttack, Odisha", "lat": 20.4625, "lon": 85.8830, "description": "Twin city, flooding from Mahanadi"},
        {"name": "Paradip Port, Odisha", "lat": 20.3165, "lon": 86.6113, "description": "Major port, storm surge"},
        {"name": "Konark, Odisha", "lat": 19.8876, "lon": 86.0945, "description": "Coastal village, evacuation zone"},
        {"name": "Balasore, Odisha", "lat": 21.4934, "lon": 86.9135, "description": "Northern coast, cyclone landfall area"},
        {"name": "Ganjam District, Odisha", "lat": 19.3867, "lon": 85.0501, "description": "Coastal district, heavy damage"},
    ],

    # Gujarat - Earthquake
    "gujarat_earthquake": [
        {"name": "Bhuj, Gujarat", "lat": 23.2420, "lon": 69.6669, "description": "Epicenter zone, severe damage"},
        {"name": "Gandhidham, Gujarat", "lat": 23.0753, "lon": 70.1337, "description": "Industrial town, building collapse"},
        {"name": "Anjar, Gujarat", "lat": 23.1127, "lon": 70.0267, "description": "Historic town, structural damage"},
        {"name": "Rapar, Gujarat", "lat": 23.5738, "lon": 70.6385, "description": "Rural area, mud house collapse"},
        {"name": "Bachau, Gujarat", "lat": 23.2961, "lon": 70.3503, "description": "Town near epicenter, casualties"},
        {"name": "Ahmedabad, Gujarat", "lat": 23.0225, "lon": 72.5714, "description": "Major city, high-rise damage"},
        {"name": "Rajkot, Gujarat", "lat": 22.3039, "lon": 70.8022, "description": "City, moderate damage"},
    ],

    # Kerala - Floods/Landslides
    "kerala_floods": [
        {"name": "Wayanad, Kerala", "lat": 11.6854, "lon": 76.1320, "description": "Hilly district, severe landslides"},
        {"name": "Idukki, Kerala", "lat": 9.8494, "lon": 76.9710, "description": "Dam area, heavy rains, flooding"},
        {"name": "Kochi, Kerala", "lat": 9.9312, "lon": 76.2673, "description": "Coastal city, urban flooding"},
        {"name": "Alappuzha (Alleppey), Kerala", "lat": 9.4981, "lon": 76.3388, "description": "Backwater area, flooding"},
        {"name": "Munnar, Kerala", "lat": 10.0889, "lon": 77.0595, "description": "Hill station, landslide risk"},
        {"name": "Thrissur, Kerala", "lat": 10.5276, "lon": 76.2144, "description": "Central Kerala, waterlogging"},
        {"name": "Pathanamthitta, Kerala", "lat": 9.2648, "lon": 76.7870, "description": "District, severe flooding"},
    ],

    # West Bengal - Cyclone Amphan type
    "bengal_cyclone": [
        {"name": "Kolkata, West Bengal", "lat": 22.5726, "lon": 88.3639, "description": "Metro city, wind and rain damage"},
        {"name": "Sundarbans, West Bengal", "lat": 21.9497, "lon": 89.1833, "description": "Mangrove delta, storm surge"},
        {"name": "Digha Beach, West Bengal", "lat": 21.6274, "lon": 87.5098, "description": "Coastal resort, evacuation zone"},
        {"name": "Kakdwip, West Bengal", "lat": 21.8742, "lon": 88.1881, "description": "Fishing village, high vulnerability"},
        {"name": "Howrah, West Bengal", "lat": 22.5958, "lon": 88.2636, "description": "Industrial area, structural damage"},
        {"name": "North 24 Parganas, West Bengal", "lat": 22.6173, "lon": 88.4234, "description": "District, flooding"},
    ],

    # Uttarakhand - Cloudburst/Flash Floods
    "uttarakhand_cloudburst": [
        {"name": "Kedarnath, Uttarakhand", "lat": 30.7346, "lon": 79.0669, "description": "Pilgrimage site, flash flood risk"},
        {"name": "Chamoli District, Uttarakhand", "lat": 30.4167, "lon": 79.5000, "description": "Glacier area, avalanche risk"},
        {"name": "Rudraprayag, Uttarakhand", "lat": 30.2833, "lon": 78.9833, "description": "Confluence town, flood damage"},
        {"name": "Pithoragarh, Uttarakhand", "lat": 29.5829, "lon": 80.2182, "description": "Border district, landslides"},
        {"name": "Joshimath, Uttarakhand", "lat": 30.5567, "lon": 79.5667, "description": "Hill town, land subsidence"},
        {"name": "Nainital, Uttarakhand", "lat": 29.3803, "lon": 79.4636, "description": "Lake town, overflow risk"},
    ],

    # Assam - Floods
    "assam_floods": [
        {"name": "Guwahati, Assam", "lat": 26.1445, "lon": 91.7362, "description": "Capital city, Brahmaputra flooding"},
        {"name": "Majuli Island, Assam", "lat": 26.9500, "lon": 94.1667, "description": "River island, erosion and floods"},
        {"name": "Dibrugarh, Assam", "lat": 27.4728, "lon": 94.9120, "description": "Town, riverbank erosion"},
        {"name": "Silchar, Assam", "lat": 24.8333, "lon": 92.7789, "description": "Barak Valley, severe flooding"},
        {"name": "Kaziranga, Assam", "lat": 26.5775, "lon": 93.1711, "description": "Wildlife sanctuary, animals stranded"},
        {"name": "Jorhat, Assam", "lat": 26.7509, "lon": 94.2037, "description": "District, embankment breach"},
    ],

    # Tamil Nadu - Cyclone
    "tamil_nadu_cyclone": [
        {"name": "Chennai, Tamil Nadu", "lat": 13.0827, "lon": 80.2707, "description": "Metro city, coastal flooding"},
        {"name": "Nagapattinam, Tamil Nadu", "lat": 10.7672, "lon": 79.8449, "description": "Coastal district, direct impact"},
        {"name": "Cuddalore, Tamil Nadu", "lat": 11.7480, "lon": 79.7714, "description": "Port town, storm surge"},
        {"name": "Pondicherry", "lat": 11.9416, "lon": 79.8083, "description": "Union territory, coastal damage"},
        {"name": "Rameswaram, Tamil Nadu", "lat": 9.2876, "lon": 79.3129, "description": "Island town, high winds"},
    ],
}

# Indian scenario types for different calamities
INDIA_SCENARIO_TYPES = {
    "flood": [
        "house_submerged", "stranded_on_roof", "boat_rescue_needed", "medical_emergency_in_flood",
        "electrocution_risk", "snake_bite", "drinking_water_shortage", "elderly_stranded",
        "child_missing", "livestock_rescue", "road_blocked_by_water"
    ],
    "cyclone": [
        "roof_blown_off", "tree_fallen_on_house", "power_line_down", "flying_debris_injury",
        "storm_surge_flooding", "evacuation_assistance", "shelter_needed", "medical_emergency",
        "communication_lost", "fishing_boat_missing", "building_collapse"
    ],
    "earthquake": [
        "building_collapse", "trapped_under_debris", "gas_leak", "fire_after_earthquake",
        "structural_damage", "injured_person", "school_collapse", "hospital_damage",
        "aftershock_fear", "rescue_needed", "bridge_collapse"
    ],
    "landslide": [
        "house_buried", "road_blocked", "vehicle_swept_away", "missing_person",
        "village_cut_off", "rescue_team_needed", "injured_in_slide", "evacuation_needed"
    ],
    "cloudburst": [
        "flash_flood", "bridge_washed_away", "pilgrims_stranded", "vehicle_swept",
        "village_submerged", "rescue_urgent", "multiple_casualties", "communication_down"
    ],
}

# Indian vulnerability factors
INDIA_VULNERABILITY_FACTORS = [
    {"age": 78, "conditions": ["diabetes", "heart_disease"], "mobility": "limited", "language": "Hindi"},
    {"age": 65, "conditions": ["asthma"], "mobility": "limited", "language": "Bengali"},
    {"age": 42, "conditions": [], "mobility": "normal", "language": "Hindi"},
    {"age": 8, "conditions": [], "mobility": "normal", "language": "Tamil"},
    {"age": 55, "conditions": ["hypertension"], "mobility": "normal", "language": "Odia"},
    {"age": 35, "conditions": [], "mobility": "normal", "language": "Malayalam"},
    {"age": 70, "conditions": ["arthritis"], "mobility": "limited", "language": "Gujarati"},
    {"age": None, "conditions": [], "mobility": "normal", "language": "Hindi"},
]

# ==================== US LOCATIONS (MAUI) ====================

MAUI_LOCATIONS = [
    {"name": "Front Street, Lahaina", "lat": 20.8783, "lon": -156.6825, "description": "Historic downtown Lahaina"},
    {"name": "Lahainaluna Road, Lahaina", "lat": 20.8844, "lon": -156.6800, "description": "Residential area, hillside"},
    {"name": "Kaanapali Beach", "lat": 20.9289, "lon": -156.6944, "description": "Resort area, tourist zone"},
    {"name": "Lahaina Harbor", "lat": 20.8700, "lon": -156.6825, "description": "Waterfront, evacuation point"},
    {"name": "Kahului Airport", "lat": 20.8986, "lon": -156.4306, "description": "Main airport, evacuation hub"},
    {"name": "Maui Memorial Medical Center", "lat": 20.9000, "lon": -156.4800, "description": "Main hospital"},
    {"name": "Wailuku Town", "lat": 20.8900, "lon": -156.5000, "description": "County seat, government center"},
]

US_SCENARIO_TYPES = [
    "medical_emergency", "fire_proximity", "evacuation_assistance", "trapped_person",
    "smoke_inhalation", "elderly_resident", "child_separation", "pet_rescue",
    "structure_fire", "road_blocked",
]


class CallGenerator:
    """Generates realistic emergency call transcripts using Groq API"""

    def __init__(self):
        """Initialize Groq client"""
        self.client = Groq(api_key=GROQ_API_KEY)
        self.model_name = "llama-3.1-8b-instant"  # Free tier compatible, fast model

    def generate_india_call_transcript(self, scenario: Dict, calamity_type: str) -> str:
        """Generate a single Indian emergency call transcript"""
        location_name = scenario['location']['name']
        location_desc = scenario['location']['description']
        language = scenario.get('language', 'Hindi')

        calamity_descriptions = {
            "flood": "severe flooding due to heavy monsoon rains",
            "cyclone": "devastating cyclone with high winds and storm surge",
            "earthquake": "powerful earthquake causing widespread damage",
            "landslide": "massive landslide due to heavy rains",
            "cloudburst": "sudden cloudburst causing flash floods",
        }

        prompt = f"""Generate a realistic Indian emergency call (112) transcript for a {calamity_descriptions.get(calamity_type, calamity_type)}.

Context:
- Location: {location_name} ({location_desc})
- Coordinates: {scenario['location']['lat']}, {scenario['location']['lon']}
- Time: {scenario['timestamp']}
- Emergency Type: {scenario['scenario_type']}
- Caller Age: {scenario.get('age', 'unknown')}
- Medical Conditions: {', '.join(scenario.get('conditions', [])) if scenario.get('conditions') else 'none'}
- Primary Language: {language}
- Severity Phase: {scenario['phase']}

Requirements:
1. Make it sound like a real, panicked Indian emergency call
2. Include natural speech patterns with some Hindi/regional language phrases mixed with English (Hinglish style)
3. Use typical Indian expressions of distress ("Baap re baap!", "Bhagwan bachao!", "Please jaldi aao!")
4. Mention specific local landmarks, areas, or neighborhoods
5. Include urgency appropriate to the {calamity_type} situation
6. Reference specific dangers: {'water level rising, electricity poles submerged, snakes' if calamity_type == 'flood' else 'wind speed, flying objects, roof damage' if calamity_type == 'cyclone' else 'building cracks, aftershocks, trapped people' if calamity_type == 'earthquake' else 'mudslide, road blocked, houses buried'}
7. Include family members (mentioning "amma", "babuji", "bachche", "dadi", etc.)
8. Keep it between 100-300 words
9. Format as dialogue between caller and emergency dispatcher (112)
10. Dispatcher should ask relevant questions and provide assurance

Generate ONLY the transcript text, no additional commentary."""

        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": "You are an expert at creating realistic emergency call transcripts for disaster simulations and training purposes."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.8,
                max_tokens=1000
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"Error generating call: {e}")
            return None

    def generate_us_call_transcript(self, scenario: Dict) -> str:
        """Generate a single US 911 call transcript (Maui wildfire)"""
        location_name = scenario['location']['name']
        location_desc = scenario['location']['description']

        prompt = f"""Generate a realistic 911 emergency call transcript for the Maui wildfire on August 8, 2023.

Context:
- Location: {location_name} ({location_desc})
- Coordinates: {scenario['location']['lat']}, {scenario['location']['lon']}
- Time: {scenario['timestamp']}
- Scenario Type: {scenario['scenario_type']}
- Caller Age: {scenario.get('age', 'unknown')}
- Medical Conditions: {', '.join(scenario.get('conditions', [])) if scenario.get('conditions') else 'none'}
- Phase: {scenario['phase']}

Requirements:
1. Make it sound like a real, panicked emergency call
2. Include natural speech patterns, interruptions, background noise
3. Mention specific landmarks in {location_name.split(',')[0]}
4. Include urgency and emotion
5. Reference fire, smoke, evacuation
6. Keep it between 100-300 words
7. Format as dialogue between caller and dispatcher

Generate ONLY the transcript text."""

        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": "You are an expert at creating realistic emergency call transcripts for disaster simulations and training purposes."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.8,
                max_tokens=1000
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"Error generating call: {e}")
            return None

    def create_india_scenario(self, call_id: str, timestamp: datetime, region: str, calamity_type: str) -> Dict:
        """Create a scenario for Indian emergency call"""
        locations = INDIA_LOCATIONS.get(region, INDIA_LOCATIONS["mumbai_floods"])
        location = random.choice(locations)

        scenario_types = INDIA_SCENARIO_TYPES.get(calamity_type, INDIA_SCENARIO_TYPES["flood"])
        scenario_type = random.choice(scenario_types)

        # Determine phase
        if timestamp.hour >= 18:
            phase = "critical"
        elif timestamp.hour >= 12:
            phase = "peak"
        elif timestamp.hour >= 6:
            phase = "developing"
        else:
            phase = "early"

        vulnerability = None
        if random.random() < 0.5:
            vulnerability = random.choice(INDIA_VULNERABILITY_FACTORS)

        scenario = {
            "callId": call_id,
            "timestamp": timestamp.isoformat() + "Z",
            "callerLocation": {
                "lat": location["lat"] + random.uniform(-0.005, 0.005),
                "lon": location["lon"] + random.uniform(-0.005, 0.005)
            },
            "location": location,
            "scenario_type": scenario_type,
            "phase": phase,
            "calamity_type": calamity_type,
            "region": region,
        }

        if vulnerability:
            scenario["age"] = vulnerability["age"]
            scenario["conditions"] = vulnerability["conditions"]
            scenario["mobility"] = vulnerability["mobility"]
            scenario["language"] = vulnerability["language"]

        return scenario

    def create_us_scenario(self, call_id: str, timestamp: datetime) -> Dict:
        """Create a scenario for US 911 call"""
        location = random.choice(MAUI_LOCATIONS)
        scenario_type = random.choice(US_SCENARIO_TYPES)

        if timestamp.hour >= 18:
            phase = "peak"
        elif timestamp.hour >= 14:
            phase = "critical"
        elif timestamp.hour >= 10:
            phase = "developing"
        else:
            phase = "early"

        return {
            "callId": call_id,
            "timestamp": timestamp.isoformat() + "Z",
            "callerLocation": {
                "lat": location["lat"] + random.uniform(-0.01, 0.01),
                "lon": location["lon"] + random.uniform(-0.01, 0.01)
            },
            "location": location,
            "scenario_type": scenario_type,
            "phase": phase,
        }

    def generate_call_data(self, scenario: Dict, country: str = "india") -> Optional[Dict]:
        """Generate complete call data"""
        print(f"Generating call {scenario['callId']} ({country.upper()})...")

        if country == "india":
            transcript = self.generate_india_call_transcript(scenario, scenario.get('calamity_type', 'flood'))
            phone_prefix = "+91"
            phone_number = f"{phone_prefix}-{random.choice(['98', '99', '70', '80', '90'])}{random.randint(10000000, 99999999)}"
        else:
            transcript = self.generate_us_call_transcript(scenario)
            phone_prefix = "+1"
            phone_number = f"{phone_prefix}-808-555-{random.randint(1000, 9999)}"

        if not transcript:
            return None

        word_count = len(transcript.split())
        estimated_duration = max(30, min(180, int(word_count / 2.5)))

        call_data = {
            "callId": scenario["callId"],
            "timestamp": scenario["timestamp"],
            "callerLocation": scenario["callerLocation"],
            "transcript": transcript,
            "metadata": {
                "phoneNumber": phone_number,
                "callDuration": estimated_duration,
                "scenarioType": scenario["scenario_type"],
                "phase": scenario["phase"],
                "country": country,
            }
        }

        if country == "india":
            call_data["metadata"]["calamityType"] = scenario.get("calamity_type")
            call_data["metadata"]["region"] = scenario.get("region")
            if "language" in scenario:
                call_data["metadata"]["language"] = scenario["language"]

        if "age" in scenario:
            call_data["metadata"]["callerAge"] = scenario["age"]
        if "conditions" in scenario and scenario["conditions"]:
            call_data["metadata"]["knownConditions"] = scenario["conditions"]

        return call_data

    def generate_batch(self, num_calls: int, country: str, region: str = None,
                      calamity_type: str = None, start_time: datetime = None,
                      end_time: datetime = None) -> List[Dict]:
        """Generate a batch of calls"""
        calls = []

        if start_time is None:
            start_time = datetime.now().replace(hour=6, minute=0, second=0)
        if end_time is None:
            end_time = start_time + timedelta(hours=18)

        time_range = (end_time - start_time).total_seconds()

        for i in range(num_calls):
            time_offset = random.uniform(0, time_range)
            call_time = start_time + timedelta(seconds=time_offset)
            call_id = f"CALL-{country.upper()[:2]}-{call_time.strftime('%Y%m%d')}-{i+1:04d}"

            if country == "india":
                scenario = self.create_india_scenario(call_id, call_time, region, calamity_type)
            else:
                scenario = self.create_us_scenario(call_id, call_time)

            call_data = self.generate_call_data(scenario, country)

            if call_data:
                calls.append(call_data)

            time.sleep(1)  # Rate limiting

        return calls


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Generate emergency call transcripts (India 112 / US 911)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 10 Mumbai flood calls
  python generate_emergency_calls.py --country india --region mumbai_floods --calamity flood --num-calls 10

  # Generate 10 Odisha cyclone calls
  python generate_emergency_calls.py --country india --region odisha_cyclone --calamity cyclone --num-calls 10

  # Generate 10 Gujarat earthquake calls
  python generate_emergency_calls.py --country india --region gujarat_earthquake --calamity earthquake --num-calls 10

  # Generate 10 Kerala flood/landslide calls
  python generate_emergency_calls.py --country india --region kerala_floods --calamity flood --num-calls 10

  # Generate 10 US Maui wildfire calls
  python generate_emergency_calls.py --country us --num-calls 10

Available Indian regions:
  - mumbai_floods (Maharashtra floods)
  - odisha_cyclone (Odisha coastal cyclone)
  - gujarat_earthquake (Gujarat earthquake)
  - kerala_floods (Kerala floods/landslides)
  - bengal_cyclone (West Bengal cyclone)
  - uttarakhand_cloudburst (Uttarakhand flash floods)
  - assam_floods (Assam Brahmaputra floods)
  - tamil_nadu_cyclone (Tamil Nadu coastal cyclone)

Calamity types: flood, cyclone, earthquake, landslide, cloudburst
        """
    )

    parser.add_argument("--country", type=str, choices=["india", "us"], default="india",
                       help="Country (india/us)")
    parser.add_argument("--region", type=str, default="mumbai_floods",
                       help="Region for Indian calls (e.g., mumbai_floods, odisha_cyclone)")
    parser.add_argument("--calamity", type=str, default="flood",
                       choices=["flood", "cyclone", "earthquake", "landslide", "cloudburst"],
                       help="Type of calamity for Indian calls")
    parser.add_argument("--num-calls", type=int, default=10, help="Number of calls to generate")
    parser.add_argument("--start-time", type=str, help="Start time (ISO format)")
    parser.add_argument("--end-time", type=str, help="End time (ISO format)")
    parser.add_argument("--output", type=str, help="Output file path")

    args = parser.parse_args()

    # Parse times
    if args.start_time:
        start_time = datetime.fromisoformat(args.start_time.replace("Z", "+00:00"))
    else:
        start_time = datetime.now().replace(hour=6, minute=0, second=0, microsecond=0)

    if args.end_time:
        end_time = datetime.fromisoformat(args.end_time.replace("Z", "+00:00"))
    else:
        end_time = start_time + timedelta(hours=18)

    # Generate calls
    generator = CallGenerator()
    print(f"Using model: {generator.model_name} (OpenAI)")
    print(f"Country: {args.country.upper()}")

    if args.country == "india":
        print(f"Region: {args.region}")
        print(f"Calamity Type: {args.calamity}")
        calls = generator.generate_batch(
            args.num_calls, args.country, args.region, args.calamity, start_time, end_time
        )
        filename_prefix = f"india_{args.region}_{args.calamity}"
    else:
        calls = generator.generate_batch(args.num_calls, args.country, start_time=start_time, end_time=end_time)
        filename_prefix = "maui_wildfire"

    print(f"Generating {args.num_calls} calls from {start_time} to {end_time}...")

    # Save output
    output_file = args.output or OUTPUT_DIR / f"{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(calls, f, indent=2, ensure_ascii=False)

    print(f"\nGenerated {len(calls)} calls")
    print(f"Saved to: {output_file}")

    # Print sample
    if calls:
        print("\n--- Sample Call ---")
        sample = calls[0]
        print(f"Call ID: {sample['callId']}")
        print(f"Time: {sample['timestamp']}")
        print(f"Location: {sample['callerLocation']}")
        print(f"Metadata: {sample['metadata']}")
        print(f"\nTranscript (first 300 chars):\n{sample['transcript'][:300]}...")


if __name__ == "__main__":
    main()
