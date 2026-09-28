/**
 * Prioritized case list component for triage decision-making
 * Displays emergency calls sorted by risk score with alert indicators
 */

'use client';

import { useState, useMemo } from 'react';
import { EmergencyCall, RiskScore, Alert } from '@/types';

interface CallListProps {
  calls: EmergencyCall[];
  riskScores?: Map<string, RiskScore>;
  alerts?: Alert[];
  selectedCallId?: string;
  onCallSelect?: (callId: string) => void;
}

export default function CallList({
  calls,
  riskScores = new Map(),
  alerts = [],
  selectedCallId,
  onCallSelect,
}: CallListProps) {
  const [sortBy, setSortBy] = useState<'timestamp' | 'risk'>('risk');
  const [filter, setFilter] = useState<string>('');

  const sortedAndFilteredCalls = useMemo(() => {
    let filtered = calls;

    if (filter) {
      filtered = calls.filter(
        (call) =>
          call.callId.toLowerCase().includes(filter.toLowerCase()) ||
          call.transcript.toLowerCase().includes(filter.toLowerCase())
      );
    }

    return [...filtered].sort((a, b) => {
      if (sortBy === 'risk') {
        const scoreA = riskScores.get(a.callId)?.riskScore || 0;
        const scoreB = riskScores.get(b.callId)?.riskScore || 0;
        return scoreB - scoreA; // Descending
      } else {
        return new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime(); // Descending
      }
    });
  }, [calls, riskScores, sortBy, filter]);

  const getRiskColor = (riskScore: number): string => {
    if (riskScore >= 80) return 'bg-red-500';
    if (riskScore >= 60) return 'bg-orange-500';
    if (riskScore >= 40) return 'bg-yellow-500';
    return 'bg-green-500';
  };

  return (
    <div className="flex flex-col h-full bg-white">
      <div className="p-4 border-b">
        <h2 className="text-xl font-bold mb-2">Prioritized Cases</h2>
        <p className="text-sm text-gray-600 mb-4">Sorted by risk score for triage decision-making</p>
        
        {/* Filter */}
        <input
          type="text"
          placeholder="Search calls..."
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className="w-full px-3 py-2 border border-gray-300 rounded-md mb-3"
        />

        {/* Sort */}
        <div className="flex gap-2">
          <button
            onClick={() => setSortBy('risk')}
            className={`px-3 py-1 rounded ${
              sortBy === 'risk' ? 'bg-blue-500 text-white' : 'bg-gray-200'
            }`}
          >
            Sort by Risk
          </button>
          <button
            onClick={() => setSortBy('timestamp')}
            className={`px-3 py-1 rounded ${
              sortBy === 'timestamp' ? 'bg-blue-500 text-white' : 'bg-gray-200'
            }`}
          >
            Sort by Time
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto">
        {sortedAndFilteredCalls.length === 0 ? (
          <div className="p-4 text-center text-gray-500">No calls found</div>
        ) : (
          <ul className="divide-y">
            {sortedAndFilteredCalls.map((call) => {
              const riskScore = riskScores.get(call.callId);
              const score = riskScore?.riskScore || 0;
              const callAlerts = alerts.filter((a) => a.callId === call.callId);
              const hasHighPriorityAlert = callAlerts.some((a) => 
                a.severity === 'high' || a.severity === 'critical'
              );

              return (
                <li
                  key={call.callId}
                  onClick={() => onCallSelect?.(call.callId)}
                  className={`p-4 cursor-pointer hover:bg-gray-50 ${
                    selectedCallId === call.callId ? 'bg-blue-50 border-l-4 border-blue-500' : ''
                  } ${hasHighPriorityAlert ? 'border-l-4 border-red-500 bg-red-50' : ''}`}
                >
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <div className="flex items-center gap-2 mb-1">
                        <span className="font-semibold">Call {call.callId}</span>
                        <span className={`px-2 py-1 rounded text-white text-xs ${getRiskColor(score)}`}>
                          Risk: {score}
                        </span>
                        {hasHighPriorityAlert && (
                          <span className="px-2 py-1 rounded bg-red-600 text-white text-xs font-semibold">
                            ALERT
                          </span>
                        )}
                      </div>
                      <p className="text-sm text-gray-600 line-clamp-2">{call.transcript}</p>
                      {callAlerts.length > 0 && (
                        <div className="mt-2">
                          {callAlerts.map((alert) => (
                            <div key={alert.alertId} className="text-xs text-red-600 font-medium">
                              ⚠️ {alert.headline || alert.message}
                            </div>
                          ))}
                        </div>
                      )}
                      <p className="text-xs text-gray-400 mt-1">
                        {new Date(call.timestamp).toLocaleString()}
                      </p>
                    </div>
                  </div>
                </li>
              );
            })}
          </ul>
        )}
      </div>
    </div>
  );
}

