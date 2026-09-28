/**
 * TypeScript types for Sentinel Dispatch frontend
 */

// Emergency Call Types
export interface CallerLocation {
  lat: number;
  lon: number;
}

export interface CallMetadata {
  phoneNumber: string;
  callDuration: number;
  callerAge?: number;
  knownConditions?: string[];
}

export interface EmergencyCall {
  callId: string;
  timestamp: string;
  callerLocation: CallerLocation;
  transcript: string;
  metadata: CallMetadata;
}

// Weather Data Types
export interface WeatherLocation {
  lat: number;
  lon: number;
}

export interface WeatherData {
  timestamp: string;
  location: WeatherLocation;
  temperature: number;
  windSpeed: number;
  windDirection: number;
  humidity: number;
  airQualityIndex: number;
  visibility: string;
}

// Fire Data Types
export interface FireLocation {
  lat: number;
  lon: number;
}

export interface FirePerimeter {
  lat: number;
  lon: number;
}

export interface FireData {
  timestamp: string;
  fireId: string;
  location: FireLocation;
  confidence: number;
  brightness: number;
  fireType: string;
  perimeter: FirePerimeter[];
  spreadRate: number;
  estimatedSize: number;
}

// Risk Score Types
export interface RiskScore {
  callId: string;
  timestamp: string;
  riskScore: number;
  confidence: number;
  factors: {
    callUrgency?: number;
    urgency?: number; // Legacy support
    fireProximity: number;
    fireSpreadRate?: number;
    weatherRisk?: number;
    weatherImpact?: number; // Legacy support
    vulnerability: number;
    timeSinceCall?: number;
  };
  recommendation?: string;
  priority?: number;
}

// Alert Types
export interface Alert {
  alertId: string;
  timestamp: string;
  severity: 'low' | 'medium' | 'high' | 'critical';
  message: string;
  alert_type?: string; // e.g., 'risk_threshold', 'fire_proximity', 'fire_spread', etc.
  callId?: string;
  event?: string;
  headline?: string;
  description?: string;
  urgency?: string;
  certainty?: string;
  effective?: string;
  expires?: string;
  status?: string;
  acknowledged?: boolean; // Whether alert has been acknowledged
}

// WebSocket Message Types
export type WebSocketMessageType =
  | 'risk_score_update'
  | 'new_call'
  | 'alert'
  | 'fire_update'
  | 'weather_update';

export interface WebSocketMessage {
  type: WebSocketMessageType;
  timestamp: string;
  data: RiskScore | EmergencyCall | FireData | WeatherData | Alert;
}

// API Response Types
export interface ApiResponse<T> {
  data: T;
  timestamp: string;
}

export interface CallsResponse {
  calls: EmergencyCall[];
  total: number;
}

export interface PrioritizedCasesResponse {
  cases: PrioritizedCase[];
  total: number;
}

// Prioritized Case (for triage decision-making)
export interface PrioritizedCase {
  callId: string;
  emergencyCall: EmergencyCall;
  riskScore: RiskScore;
  alerts?: Alert[];
  hasFireWeatherWarning?: boolean;
  priority: number;
}

