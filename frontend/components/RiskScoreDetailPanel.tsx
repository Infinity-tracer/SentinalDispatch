/**
 * Risk Score Detail Panel
 * Displays detailed breakdown of risk score including:
 * - Contributing factors with visual bars
 * - Confidence intervals
 * - Historical trends over time
 */

'use client';

import { useState, useEffect, useMemo } from 'react';
import { RiskScore, EmergencyCall } from '@/types';
import { apiClient } from '@/lib/api';

interface RiskScoreDetailPanelProps {
  callId: string | null;
  emergencyCall: EmergencyCall | null;
  riskScore: RiskScore | null;
  onClose?: () => void;
}

interface HistoricalDataPoint {
  timestamp: string;
  riskScore: number;
  confidence: number;
}

export default function RiskScoreDetailPanel({
  callId,
  emergencyCall,
  riskScore,
  onClose,
}: RiskScoreDetailPanelProps) {
  const [historicalData, setHistoricalData] = useState<HistoricalDataPoint[]>([]);
  const [isLoadingHistory, setIsLoadingHistory] = useState(false);

  // Load historical data (for now, we'll simulate it since we don't have persistent storage)
  useEffect(() => {
    if (!callId || !riskScore) {
      setHistoricalData([]);
      return;
    }

    const loadHistoricalData = async () => {
      setIsLoadingHistory(true);
      try {
        // TODO: Replace with actual API endpoint when persistent storage is implemented
        // For now, generate simulated historical data based on current score
        const simulated = generateSimulatedHistory(riskScore);
        setHistoricalData(simulated);
      } catch (error) {
        console.error('Error loading historical data:', error);
        setHistoricalData([]);
      } finally {
        setIsLoadingHistory(false);
      }
    };

    loadHistoricalData();
  }, [callId, riskScore]);

  if (!callId || !riskScore || !emergencyCall) {
    return (
      <div className="w-96 bg-white border-l border-gray-200 p-6 flex items-center justify-center">
        <div className="text-center text-gray-500">
          <p className="text-sm">Select a case to view risk score details</p>
        </div>
      </div>
    );
  }

  const factors = riskScore.factors || {};
  const urgency = factors.urgency || factors.callUrgency || 0;
  const fireProximity = factors.fireProximity || 0;
  const fireSpreadRate = factors.fireSpreadRate || 0;
  const weatherRisk = factors.weatherRisk || factors.weatherImpact || 0;
  const vulnerability = factors.vulnerability || 0;
  const timeSinceCall = factors.timeSinceCall || 0;

  // Calculate confidence interval
  const confidenceInterval = useMemo(() => {
    const confidence = riskScore.confidence || 0;
    const margin = (1 - confidence) * riskScore.riskScore;
    return {
      lower: Math.max(0, riskScore.riskScore - margin),
      upper: Math.min(100, riskScore.riskScore + margin),
      confidence: confidence * 100,
    };
  }, [riskScore]);

  // Factor contribution analysis
  const factorContributions = useMemo(() => {
    const total = urgency + fireProximity + fireSpreadRate + weatherRisk + vulnerability;
    if (total === 0) return [];

    return [
      { name: 'Call Urgency', value: urgency, percentage: (urgency / total) * 100, color: 'bg-red-500' },
      { name: 'Fire Proximity', value: fireProximity, percentage: (fireProximity / total) * 100, color: 'bg-orange-500' },
      { name: 'Fire Spread Rate', value: fireSpreadRate, percentage: (fireSpreadRate / total) * 100, color: 'bg-orange-600' },
      { name: 'Weather Risk', value: weatherRisk, percentage: (weatherRisk / total) * 100, color: 'bg-yellow-500' },
      { name: 'Vulnerability', value: vulnerability, percentage: (vulnerability / total) * 100, color: 'bg-purple-500' },
    ].filter((f) => f.value > 0).sort((a, b) => b.value - a.value);
  }, [urgency, fireProximity, fireSpreadRate, weatherRisk, vulnerability]);

  const getRiskColor = (score: number): string => {
    if (score >= 80) return 'text-red-600';
    if (score >= 60) return 'text-orange-600';
    if (score >= 40) return 'text-yellow-600';
    return 'text-green-600';
  };

  const getConfidenceColor = (confidence: number): string => {
    if (confidence >= 0.8) return 'text-green-600';
    if (confidence >= 0.6) return 'text-yellow-600';
    return 'text-orange-600';
  };

  const formatTimeSinceCall = (minutes: number): string => {
    if (minutes < 1) return 'Just now';
    if (minutes < 60) return `${Math.round(minutes)}m ago`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${hours}h ${Math.round(minutes % 60)}m ago`;
    const days = Math.floor(hours / 24);
    return `${days}d ${hours % 24}h ago`;
  };

  return (
    <div className="w-96 bg-white border-l border-gray-200 h-full overflow-y-auto">
      {/* Header */}
      <div className="sticky top-0 bg-white border-b border-gray-200 p-4 z-10">
        <div className="flex items-center justify-between mb-2">
          <h2 className="text-lg font-bold text-gray-900">Risk Score Details</h2>
          {onClose && (
            <button
              onClick={onClose}
              className="text-gray-400 hover:text-gray-600 transition-colors"
              aria-label="Close"
            >
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          )}
        </div>
        <div className="text-sm text-gray-600">
          Call {callId}
        </div>
      </div>

      <div className="p-4 space-y-6">
        {/* Overall Risk Score */}
        <div className="bg-gradient-to-br from-gray-50 to-gray-100 rounded-lg p-4 border border-gray-200">
          <div className="flex items-baseline justify-between mb-2">
            <span className="text-sm font-medium text-gray-700">Overall Risk Score</span>
            <span className={`text-3xl font-bold ${getRiskColor(riskScore.riskScore)}`}>
              {Math.round(riskScore.riskScore)}
            </span>
          </div>
          <div className="w-full bg-gray-200 rounded-full h-3 mb-2">
            <div
              className={`h-3 rounded-full transition-all ${
                riskScore.riskScore >= 80
                  ? 'bg-red-500'
                  : riskScore.riskScore >= 60
                  ? 'bg-orange-500'
                  : riskScore.riskScore >= 40
                  ? 'bg-yellow-500'
                  : 'bg-green-500'
              }`}
              style={{ width: `${Math.min(100, riskScore.riskScore)}%` }}
            />
          </div>
          <div className="flex items-center justify-between text-xs text-gray-600">
            <span>Priority: {riskScore.priority ? `P${riskScore.priority}` : 'N/A'}</span>
            <span>{new Date(riskScore.timestamp).toLocaleString()}</span>
          </div>
        </div>

        {/* Confidence Interval */}
        <div className="bg-blue-50 rounded-lg p-4 border border-blue-200">
          <div className="flex items-center justify-between mb-3">
            <span className="text-sm font-semibold text-gray-900">Confidence Level</span>
            <span className={`text-lg font-bold ${getConfidenceColor(riskScore.confidence)}`}>
              {Math.round(confidenceInterval.confidence)}%
            </span>
          </div>
          <div className="space-y-2">
            <div className="relative">
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div
                  className="bg-blue-500 h-2 rounded-full transition-all"
                  style={{ width: `${confidenceInterval.confidence}%` }}
                />
              </div>
            </div>
            <div className="text-xs text-gray-600 space-y-1">
              <div className="flex justify-between">
                <span>Confidence Interval:</span>
                <span className="font-medium">
                  {Math.round(confidenceInterval.lower)} - {Math.round(confidenceInterval.upper)}
                </span>
              </div>
              <div className="text-gray-500 italic">
                The risk score is {Math.round(confidenceInterval.confidence)}% confident
                within this range
              </div>
            </div>
          </div>
        </div>

        {/* Contributing Factors */}
        <div>
          <h3 className="text-sm font-semibold text-gray-900 mb-3">Contributing Factors</h3>
          <div className="space-y-3">
            {factorContributions.length > 0 ? (
              factorContributions.map((factor, index) => (
                <div key={index} className="space-y-1">
                  <div className="flex items-center justify-between text-xs">
                    <span className="font-medium text-gray-700">{factor.name}</span>
                    <span className="text-gray-600">
                      {factor.value.toFixed(1)} ({factor.percentage.toFixed(1)}%)
                    </span>
                  </div>
                  <div className="w-full bg-gray-200 rounded-full h-2">
                    <div
                      className={`${factor.color} h-2 rounded-full transition-all`}
                      style={{ width: `${Math.min(100, factor.percentage)}%` }}
                    />
                  </div>
                </div>
              ))
            ) : (
              <p className="text-sm text-gray-500 italic">No factor data available</p>
            )}
          </div>
        </div>

        {/* Factor Details */}
        <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-900 mb-3">Factor Breakdown</h3>
          <div className="space-y-2 text-xs">
            {urgency > 0 && (
              <div className="flex justify-between">
                <span className="text-gray-600">Call Urgency:</span>
                <span className="font-medium text-gray-900">{urgency.toFixed(1)}</span>
              </div>
            )}
            {fireProximity > 0 && (
              <div className="flex justify-between">
                <span className="text-gray-600">Fire Proximity:</span>
                <span className="font-medium text-gray-900">
                  {fireProximity.toFixed(1)} km
                </span>
              </div>
            )}
            {fireSpreadRate > 0 && (
              <div className="flex justify-between">
                <span className="text-gray-600">Fire Spread Rate:</span>
                <span className="font-medium text-gray-900">
                  {fireSpreadRate.toFixed(2)} km/h
                </span>
              </div>
            )}
            {weatherRisk > 0 && (
              <div className="flex justify-between">
                <span className="text-gray-600">Weather Risk:</span>
                <span className="font-medium text-gray-900">{weatherRisk.toFixed(1)}</span>
              </div>
            )}
            {vulnerability > 0 && (
              <div className="flex justify-between">
                <span className="text-gray-600">Vulnerability:</span>
                <span className="font-medium text-gray-900">{vulnerability.toFixed(1)}</span>
              </div>
            )}
            {timeSinceCall > 0 && (
              <div className="flex justify-between">
                <span className="text-gray-600">Time Since Call:</span>
                <span className="font-medium text-gray-900">
                  {formatTimeSinceCall(timeSinceCall)}
                </span>
              </div>
            )}
          </div>
        </div>

        {/* Recommendation */}
        {riskScore.recommendation && (
          <div className="bg-yellow-50 rounded-lg p-4 border border-yellow-200">
            <h3 className="text-sm font-semibold text-gray-900 mb-2">Recommendation</h3>
            <p className="text-sm text-gray-700">{riskScore.recommendation}</p>
          </div>
        )}

        {/* Historical Trends */}
        <div>
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-semibold text-gray-900">Historical Trends</h3>
            {isLoadingHistory && (
              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-500"></div>
            )}
          </div>
          {historicalData.length > 0 ? (
            <div className="bg-white rounded-lg p-4 border border-gray-200">
              <HistoricalChart data={historicalData} currentScore={riskScore.riskScore} />
            </div>
          ) : (
            <div className="bg-gray-50 rounded-lg p-4 border border-gray-200 text-center">
              <p className="text-xs text-gray-500">
                {isLoadingHistory
                  ? 'Loading historical data...'
                  : 'Historical data not available (requires persistent storage)'}
              </p>
            </div>
          )}
        </div>

        {/* Call Information */}
        <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-900 mb-2">Call Information</h3>
          <div className="space-y-1 text-xs text-gray-600">
            <div>
              <span className="font-medium">Timestamp:</span>{' '}
              {new Date(emergencyCall.timestamp).toLocaleString()}
            </div>
            <div>
              <span className="font-medium">Location:</span>{' '}
              {emergencyCall.callerLocation.lat.toFixed(4)}, {emergencyCall.callerLocation.lon.toFixed(4)}
            </div>
            {emergencyCall.metadata?.callDuration && (
              <div>
                <span className="font-medium">Call Duration:</span>{' '}
                {emergencyCall.metadata.callDuration}s
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

/**
 * Simple historical chart component
 * Shows risk score trend over time
 */
function HistoricalChart({
  data,
  currentScore,
}: {
  data: HistoricalDataPoint[];
  currentScore: number;
}) {
  if (data.length === 0) return null;

  const maxScore = Math.max(...data.map((d) => d.riskScore), currentScore, 100);
  const minScore = Math.min(...data.map((d) => d.riskScore), currentScore, 0);
  const range = maxScore - minScore || 1;

  // Simple line chart using SVG
  const width = 300;
  const height = 120;
  const padding = 20;
  const chartWidth = width - padding * 2;
  const chartHeight = height - padding * 2;

  const points = data.map((d, i) => {
    const x = padding + (i / (data.length - 1 || 1)) * chartWidth;
    const y = padding + chartHeight - ((d.riskScore - minScore) / range) * chartHeight;
    return { x, y, ...d };
  });

  // Add current score as the last point
  const currentPoint = {
    x: padding + chartWidth,
    y: padding + chartHeight - ((currentScore - minScore) / range) * chartHeight,
    riskScore: currentScore,
    timestamp: new Date().toISOString(),
    confidence: 1,
  };

  const allPoints = [...points, currentPoint];

  // Create path for line
  const pathData = allPoints
    .map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`)
    .join(' ');

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between text-xs text-gray-600 mb-2">
        <span>Risk Score Over Time</span>
        <span className="font-medium">
          {data.length} {data.length === 1 ? 'data point' : 'data points'}
        </span>
      </div>
      <svg width={width} height={height} className="w-full">
        {/* Grid lines */}
        {[0, 25, 50, 75, 100].map((score) => {
          const y = padding + chartHeight - ((score - minScore) / range) * chartHeight;
          return (
            <g key={score}>
              <line
                x1={padding}
                y1={y}
                x2={width - padding}
                y2={y}
                stroke="#e5e7eb"
                strokeWidth={1}
                strokeDasharray="2,2"
              />
              <text x={padding - 5} y={y + 4} fontSize="10" fill="#6b7280" textAnchor="end">
                {score}
              </text>
            </g>
          );
        })}

        {/* Area under curve */}
        <path
          d={`${pathData} L ${currentPoint.x} ${padding + chartHeight} L ${padding} ${padding + chartHeight} Z`}
          fill="rgba(59, 130, 246, 0.1)"
        />

        {/* Line */}
        <path
          d={pathData}
          fill="none"
          stroke="#3b82f6"
          strokeWidth={2}
          strokeLinecap="round"
          strokeLinejoin="round"
        />

        {/* Data points */}
        {points.map((p, i) => (
          <circle
            key={i}
            cx={p.x}
            cy={p.y}
            r={3}
            fill="#3b82f6"
            className="hover:r-4 transition-all"
          />
        ))}

        {/* Current score point */}
        <circle
          cx={currentPoint.x}
          cy={currentPoint.y}
          r={4}
          fill="#ef4444"
          className="animate-pulse"
        />
      </svg>

      {/* Legend */}
      <div className="flex items-center justify-between text-xs text-gray-500 mt-2">
        <div className="flex items-center gap-2">
          <div className="w-3 h-0.5 bg-blue-500"></div>
          <span>Historical</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-3 h-3 bg-red-500 rounded-full"></div>
          <span>Current</span>
        </div>
      </div>
    </div>
  );
}

/**
 * Generate simulated historical data for demonstration
 * TODO: Replace with actual API call when persistent storage is implemented
 */
function generateSimulatedHistory(currentScore: RiskScore): HistoricalDataPoint[] {
  const history: HistoricalDataPoint[] = [];
  const baseScore = currentScore.riskScore;
  const now = new Date(currentScore.timestamp);

  // Generate 10 data points over the last hour
  for (let i = 9; i >= 0; i--) {
    const timestamp = new Date(now.getTime() - i * 6 * 60 * 1000); // 6 minutes apart
    // Simulate some variation in the score
    const variation = (Math.random() - 0.5) * 10; // ±5 points
    const score = Math.max(0, Math.min(100, baseScore + variation));
    const confidence = Math.max(0.5, Math.min(1, currentScore.confidence + (Math.random() - 0.5) * 0.2));

    history.push({
      timestamp: timestamp.toISOString(),
      riskScore: score,
      confidence: confidence,
    });
  }

  return history;
}

