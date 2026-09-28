/**
 * Prioritized case list component for triage decision-making
 * Displays emergency calls sorted by priority and risk score with alert indicators
 */

'use client';

import { useState, useEffect, useMemo } from 'react';
import { PrioritizedCase, Alert } from '@/types';
import { apiClient } from '@/lib/api';
import { useWebSocket } from '@/hooks/useWebSocket';

interface PrioritizedCaseListProps {
  selectedCallId?: string;
  onCaseSelect?: (callId: string) => void;
}

export default function PrioritizedCaseList({
  selectedCallId,
  onCaseSelect,
}: PrioritizedCaseListProps) {
  const [cases, setCases] = useState<PrioritizedCase[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sortBy, setSortBy] = useState<'priority' | 'risk' | 'timestamp'>('priority');
  const [filter, setFilter] = useState<string>('');
  const [minPriority, setMinPriority] = useState<number | null>(null);

  const wsUrl = process.env.NEXT_PUBLIC_WS_URL || 'ws://localhost:8000/ws';
  const { isConnected, lastMessage } = useWebSocket(wsUrl);

  // Load initial data
  useEffect(() => {
    const loadCases = async () => {
      try {
        setIsLoading(true);
        setError(null);
        const response = await apiClient.getPrioritizedCases(
          undefined,
          minPriority === null ? undefined : minPriority
        );
        setCases(response.cases || []);
      } catch (err) {
        console.error('Error loading prioritized cases:', err);
        setError(err instanceof Error ? err.message : 'Failed to load cases');
      } finally {
        setIsLoading(false);
      }
    };

    loadCases();

    // Refresh every 5 seconds
    const interval = setInterval(loadCases, 5000);
    return () => clearInterval(interval);
  }, [minPriority]);

  // Handle WebSocket updates
  useEffect(() => {
    if (!lastMessage) return;

    // Refresh cases when we receive updates
    if (
      lastMessage.type === 'risk_score_update' ||
      lastMessage.type === 'new_call' ||
      lastMessage.type === 'alert'
    ) {
      // Reload cases to get updated priorities
      apiClient
        .getPrioritizedCases()
        .then((response) => {
          setCases(response.cases || []);
        })
        .catch((err) => {
          console.error('Error refreshing cases:', err);
        });
    }
  }, [lastMessage]);

  const sortedAndFilteredCases = useMemo(() => {
    let filtered = cases;

    // Filter by search term
    if (filter) {
      filtered = cases.filter(
        (c) =>
          c.callId.toLowerCase().includes(filter.toLowerCase()) ||
          c.emergencyCall.transcript.toLowerCase().includes(filter.toLowerCase()) ||
          c.riskScore.recommendation?.toLowerCase().includes(filter.toLowerCase())
      );
    }

    // Sort
    return [...filtered].sort((a, b) => {
      if (sortBy === 'priority') {
        // Priority: 5=critical, 1=low (higher is more urgent)
        if (b.priority !== a.priority) {
          return b.priority - a.priority;
        }
        // If same priority, sort by risk score
        return b.riskScore.riskScore - a.riskScore.riskScore;
      } else if (sortBy === 'risk') {
        return b.riskScore.riskScore - a.riskScore.riskScore;
      } else {
        // timestamp
        return (
          new Date(b.emergencyCall.timestamp).getTime() -
          new Date(a.emergencyCall.timestamp).getTime()
        );
      }
    });
  }, [cases, sortBy, filter]);

  const getPriorityColor = (priority: number): string => {
    if (priority >= 5) return 'bg-red-600 text-white';
    if (priority >= 4) return 'bg-orange-500 text-white';
    if (priority >= 3) return 'bg-yellow-500 text-white';
    if (priority >= 2) return 'bg-blue-500 text-white';
    return 'bg-gray-400 text-white';
  };

  const getPriorityLabel = (priority: number): string => {
    if (priority >= 5) return 'CRITICAL';
    if (priority >= 4) return 'HIGH';
    if (priority >= 3) return 'MEDIUM';
    if (priority >= 2) return 'LOW';
    return 'MINIMAL';
  };

  const getRiskColor = (riskScore: number): string => {
    if (riskScore >= 80) return 'bg-red-500';
    if (riskScore >= 60) return 'bg-orange-500';
    if (riskScore >= 40) return 'bg-yellow-500';
    return 'bg-green-500';
  };

  const formatTimestamp = (timestamp: string): string => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);

    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours}h ago`;
    return date.toLocaleString();
  };


  return (
    <div className="flex flex-col h-full bg-white">
      {/* Header */}
      <div className="p-4 border-b bg-gray-50">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-xl font-bold text-gray-900">Prioritized Cases</h2>
          <div className="flex items-center gap-2">
            <div
              className={`w-2 h-2 rounded-full ${
                isConnected ? 'bg-green-500' : 'bg-red-500'
              }`}
              title={isConnected ? 'WebSocket connected' : 'WebSocket disconnected'}
            />
          </div>
        </div>
        <p className="text-sm text-gray-600 mb-4">
          Sorted by priority for triage decision-making ({cases.length} total)
        </p>

        {/* Search Filter */}
        <input
          type="text"
          placeholder="Search by call ID, transcript, or recommendation..."
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className="w-full px-3 py-2 border border-gray-300 rounded-md mb-3 text-sm text-gray-900 bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
        />

        {/* Priority Filter */}
        <div className="mb-3">
          <label className="text-xs font-medium text-gray-700 mb-1 block">
            Minimum Priority:
          </label>
          <select
            value={minPriority === null ? '' : minPriority}
            onChange={(e) =>
              setMinPriority(e.target.value === '' ? null : parseInt(e.target.value))
            }
            className="w-full px-2 py-1 border border-gray-300 rounded text-sm text-gray-900 bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            <option value="">All Priorities</option>
            <option value="5">Critical (5)</option>
            <option value="4">High+ (4-5)</option>
            <option value="3">Medium+ (3-5)</option>
            <option value="2">Low+ (2-5)</option>
            <option value="1">All (1-5)</option>
          </select>
        </div>

      </div>

      {/* Cases List */}
      <div className="flex-1 overflow-y-auto">
        {isLoading && cases.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <div className="text-center">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500 mx-auto"></div>
              <p className="mt-2 text-gray-600 text-sm">Loading cases...</p>
            </div>
          </div>
        ) : error ? (
          <div className="flex items-center justify-center h-full">
            <div className="text-center text-red-600 p-4">
              <p className="font-semibold">Error loading cases</p>
              <p className="text-sm mt-1">{error}</p>
              <p className="text-xs mt-2 text-gray-500">
                Check browser console and API logs for details
              </p>
            </div>
          </div>
        ) : sortedAndFilteredCases.length === 0 ? (
          <div className="p-8 text-center text-gray-500">
            <p className="font-medium">No cases found</p>
            <p className="text-sm mt-1">
              {filter || minPriority !== null
                ? 'Try adjusting your filters'
                : cases.length === 0
                ? 'No prioritized cases available. Cases need to be enriched and have risk scores to appear here.'
                : 'No cases match your current filters'}
            </p>
            {cases.length === 0 && (
              <p className="text-xs mt-2 text-gray-400">
                Make sure calls have been processed through all agents (classification → enrichment → risk scoring).
              </p>
            )}
          </div>
        ) : (
          <ul className="divide-y divide-gray-200">
            {sortedAndFilteredCases.map((c) => {
              const hasAlerts = c.alerts && c.alerts.length > 0;
              const hasCriticalAlerts = c.alerts?.some(
                (a) => a.severity === 'critical' || a.severity === 'high'
              );
              const hasFireWeatherWarning = c.hasFireWeatherWarning;

              return (
                <li
                  key={c.callId}
                  onClick={() => onCaseSelect?.(c.callId)}
                  className={`p-4 cursor-pointer transition-colors hover:bg-gray-50 ${
                    selectedCallId === c.callId
                      ? 'bg-blue-50 border-l-4 border-blue-500'
                      : ''
                  } ${
                    hasCriticalAlerts
                      ? 'border-l-4 border-red-500 bg-red-50'
                      : 'border-l-4 border-transparent'
                  }`}
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="flex-1 min-w-0">
                      {/* Header with Priority and Risk */}
                      <div className="flex items-center gap-2 mb-2 flex-wrap">
                        <span
                          className={`px-2 py-1 rounded text-xs font-bold ${getPriorityColor(
                            c.priority
                          )}`}
                        >
                          P{c.priority} - {getPriorityLabel(c.priority)}
                        </span>
                        <span
                          className={`px-2 py-1 rounded text-white text-xs font-semibold ${getRiskColor(
                            c.riskScore.riskScore
                          )}`}
                        >
                          Risk: {Math.round(c.riskScore.riskScore)}
                        </span>
                        {hasCriticalAlerts && (
                          <span className="px-2 py-1 rounded bg-red-600 text-white text-xs font-semibold animate-pulse">
                            ⚠️ ALERT
                          </span>
                        )}
                        {hasFireWeatherWarning && (
                          <span className="px-2 py-1 rounded bg-orange-600 text-white text-xs font-semibold">
                            🔥 Fire Weather
                          </span>
                        )}
                      </div>

                      {/* Call ID */}
                      <div className="flex items-center gap-2 mb-1">
                        <span className="font-semibold text-gray-900">Call {c.callId}</span>
                        <span className="text-xs text-gray-500">
                          {formatTimestamp(c.emergencyCall.timestamp)}
                        </span>
                      </div>

                      {/* Transcript Preview */}
                      <p className="text-sm text-gray-700 line-clamp-2 mb-2">
                        {c.emergencyCall.transcript}
                      </p>

                      {/* Recommendation */}
                      {c.riskScore.recommendation && (
                        <p className="text-xs text-blue-700 font-medium mb-2">
                          💡 {c.riskScore.recommendation}
                        </p>
                      )}

                      {/* Alerts */}
                      {hasAlerts && (
                        <div className="mt-2 space-y-1">
                          {c.alerts!.map((alert: Alert) => (
                            <div
                              key={alert.alertId}
                              className={`text-xs font-medium px-2 py-1 rounded ${
                                alert.severity === 'critical'
                                  ? 'bg-red-100 text-red-800'
                                  : alert.severity === 'high'
                                  ? 'bg-orange-100 text-orange-800'
                                  : 'bg-yellow-100 text-yellow-800'
                              }`}
                            >
                              ⚠️ {alert.headline || alert.message}
                            </div>
                          ))}
                        </div>
                      )}

                      {/* Risk Factors Summary */}
                      {c.riskScore.factors && (
                        <div className="mt-2 flex flex-wrap gap-1">
                          {c.riskScore.factors.urgency && (
                            <span className="text-xs px-1.5 py-0.5 bg-gray-100 text-gray-700 rounded">
                              Urgency: {c.riskScore.factors.urgency}
                            </span>
                          )}
                          {c.riskScore.factors.fireProximity > 0 && (
                            <span className="text-xs px-1.5 py-0.5 bg-orange-100 text-orange-700 rounded">
                              Fire: {c.riskScore.factors.fireProximity.toFixed(1)}km
                            </span>
                          )}
                          {c.riskScore.factors.vulnerability > 0 && (
                            <span className="text-xs px-1.5 py-0.5 bg-purple-100 text-purple-700 rounded">
                              Vulnerability: {c.riskScore.factors.vulnerability.toFixed(1)}
                            </span>
                          )}
                        </div>
                      )}
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

