/**
 * Alerts Panel Component
 * Displays high priority alerts and notifications for dispatchers
 * Features:
 * - Real-time updates via WebSocket
 * - Filter by severity and acknowledgment status
 * - Acknowledge alerts directly from the panel
 * - Visual indicators for critical alerts
 */

'use client';

import { useState, useEffect, useMemo } from 'react';
import { Alert } from '@/types';
import { apiClient } from '@/lib/api';
import { useWebSocket } from '@/hooks/useWebSocket';

interface AlertsPanelProps {
  onAlertClick?: (alert: Alert) => void;
  maxHeight?: string;
}

export default function AlertsPanel({ onAlertClick, maxHeight = 'h-full' }: AlertsPanelProps) {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [severityFilter, setSeverityFilter] = useState<'all' | 'critical' | 'high' | 'medium' | 'low'>('all');
  const [showAcknowledged, setShowAcknowledged] = useState(false);
  const [acknowledging, setAcknowledging] = useState<Set<string>>(new Set());

  const wsUrl = process.env.NEXT_PUBLIC_WS_URL || 'ws://localhost:8000/ws';
  const { isConnected, lastMessage, sendMessage } = useWebSocket(wsUrl);

  // Load initial alerts
  useEffect(() => {
    const loadAlerts = async () => {
      try {
        setIsLoading(true);
        setError(null);
        const alertsData = await apiClient.getAlerts();
        setAlerts(alertsData || []);
      } catch (err) {
        console.error('Error loading alerts:', err);
        setError(err instanceof Error ? err.message : 'Failed to load alerts');
      } finally {
        setIsLoading(false);
      }
    };

    loadAlerts();

    // Refresh every 5 seconds
    const interval = setInterval(loadAlerts, 5000);
    return () => clearInterval(interval);
  }, []);

  // Handle WebSocket updates
  useEffect(() => {
    if (!lastMessage) return;

    if (lastMessage.type === 'alert') {
      const alert = lastMessage.data as Alert;
      setAlerts((prev) => {
        const index = prev.findIndex((a) => a.alertId === alert.alertId);
        if (index >= 0) {
          const updated = [...prev];
          updated[index] = alert;
          return updated;
        }
        return [alert, ...prev];
      });
    }
  }, [lastMessage]);

  // Filter and sort alerts
  const filteredAlerts = useMemo(() => {
    let filtered = alerts;

    // Filter by severity
    if (severityFilter !== 'all') {
      filtered = filtered.filter((a) => a.severity === severityFilter);
    }

    // Filter by acknowledgment status
    if (!showAcknowledged) {
      filtered = filtered.filter((a) => !a.acknowledged);
    }

    // Sort by severity (critical first), then timestamp (most recent first)
    const severityOrder = { critical: 4, high: 3, medium: 2, low: 1 };
    return [...filtered].sort((a, b) => {
      const severityDiff =
        (severityOrder[b.severity as keyof typeof severityOrder] || 0) -
        (severityOrder[a.severity as keyof typeof severityOrder] || 0);
      if (severityDiff !== 0) return severityDiff;
      return new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime();
    });
  }, [alerts, severityFilter, showAcknowledged]);

  const unacknowledgedCount = useMemo(
    () => alerts.filter((a) => !a.acknowledged).length,
    [alerts]
  );

  const criticalCount = useMemo(
    () => alerts.filter((a) => a.severity === 'critical' && !a.acknowledged).length,
    [alerts]
  );

  const highCount = useMemo(
    () => alerts.filter((a) => a.severity === 'high' && !a.acknowledged).length,
    [alerts]
  );

  const handleAcknowledge = async (alertId: string) => {
    if (acknowledging.has(alertId)) return;

    try {
      setAcknowledging((prev) => new Set(prev).add(alertId));
      
      // Send acknowledgment via WebSocket (handles database update and broadcasts to all clients)
      sendMessage({
        type: 'acknowledge_alert',
        alert_id: alertId,
        timestamp: new Date().toISOString(),
      });

      // Update local state optimistically
      setAlerts((prev) =>
        prev.map((a) => (a.alertId === alertId ? { ...a, acknowledged: true } : a))
      );
    } catch (error) {
      console.error('Error acknowledging alert:', error);
      // Reload alerts on error to get correct state
      const alertsData = await apiClient.getAlerts();
      setAlerts(alertsData || []);
    } finally {
      setAcknowledging((prev) => {
        const updated = new Set(prev);
        updated.delete(alertId);
        return updated;
      });
    }
  };

  const getSeverityColor = (severity: string): string => {
    switch (severity) {
      case 'critical':
        return 'bg-red-600 text-white border-red-700';
      case 'high':
        return 'bg-orange-500 text-white border-orange-600';
      case 'medium':
        return 'bg-yellow-500 text-white border-yellow-600';
      case 'low':
        return 'bg-blue-500 text-white border-blue-600';
      default:
        return 'bg-gray-500 text-white border-gray-600';
    }
  };

  const getSeverityIcon = (severity: string): string => {
    switch (severity) {
      case 'critical':
        return '🚨';
      case 'high':
        return '⚠️';
      case 'medium':
        return '⚡';
      case 'low':
        return 'ℹ️';
      default:
        return '📢';
    }
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
    <div className={`flex flex-col bg-white border-l border-gray-200 ${maxHeight} overflow-hidden`}>
      {/* Header */}
      <div className="sticky top-0 bg-white border-b border-gray-200 p-4 z-10">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <h2 className="text-lg font-bold text-gray-900">Alerts</h2>
            {unacknowledgedCount > 0 && (
              <span className="px-2 py-0.5 bg-red-500 text-white text-xs font-bold rounded-full">
                {unacknowledgedCount}
              </span>
            )}
            <div
              className={`w-2 h-2 rounded-full ${isConnected ? 'bg-green-500' : 'bg-red-500'}`}
              title={isConnected ? 'WebSocket connected' : 'WebSocket disconnected'}
            />
          </div>
        </div>

        {/* Summary Badges */}
        {(criticalCount > 0 || highCount > 0) && (
          <div className="flex gap-2 mb-3">
            {criticalCount > 0 && (
              <span className="px-2 py-1 bg-red-600 text-white text-xs font-semibold rounded">
                {criticalCount} Critical
              </span>
            )}
            {highCount > 0 && (
              <span className="px-2 py-1 bg-orange-500 text-white text-xs font-semibold rounded">
                {highCount} High
              </span>
            )}
          </div>
        )}

        {/* Filters */}
        <div className="space-y-2">
          <div>
            <label className="text-xs font-medium text-gray-700 mb-1 block">
              Severity:
            </label>
            <select
              value={severityFilter}
              onChange={(e) => setSeverityFilter(e.target.value as typeof severityFilter)}
              className="w-full px-2 py-1 border border-gray-300 rounded text-sm text-gray-900 bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="all">All Severities</option>
              <option value="critical">Critical</option>
              <option value="high">High</option>
              <option value="medium">Medium</option>
              <option value="low">Low</option>
            </select>
          </div>
          <label className="flex items-center gap-2 text-sm">
            <input
              type="checkbox"
              checked={showAcknowledged}
              onChange={(e) => setShowAcknowledged(e.target.checked)}
              className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
            />
            <span className="text-gray-700">Show acknowledged</span>
          </label>
        </div>
      </div>

      {/* Alerts List */}
      <div className="flex-1 overflow-y-auto">
        {isLoading && alerts.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <div className="text-center">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500 mx-auto"></div>
              <p className="mt-2 text-gray-600 text-sm">Loading alerts...</p>
            </div>
          </div>
        ) : error ? (
          <div className="flex items-center justify-center h-full p-4">
            <div className="text-center text-red-600">
              <p className="font-semibold">Error loading alerts</p>
              <p className="text-sm mt-1">{error}</p>
              <p className="text-xs mt-2 text-gray-500">
                Check browser console and API logs for details
              </p>
            </div>
          </div>
        ) : filteredAlerts.length === 0 ? (
          <div className="flex items-center justify-center h-full p-4">
            <div className="text-center text-gray-500">
              <p className="font-medium">No alerts found</p>
              <p className="text-sm mt-1">
                {showAcknowledged
                  ? 'Try adjusting your filters'
                  : 'All alerts have been acknowledged or no alerts exist'}
              </p>
              {!showAcknowledged && (
                <p className="text-xs mt-2 text-gray-400">
                  Toggle "Show acknowledged" to see all alerts
                </p>
              )}
            </div>
          </div>
        ) : (
          <ul className="divide-y divide-gray-200">
            {filteredAlerts.map((alert) => {
              const isAcknowledging = acknowledging.has(alert.alertId);
              const isCritical = alert.severity === 'critical' || alert.severity === 'high';

              return (
                <li
                  key={alert.alertId}
                  onClick={() => onAlertClick?.(alert)}
                  className={`p-4 transition-colors ${
                    onAlertClick ? 'cursor-pointer hover:bg-gray-50' : ''
                  } ${
                    !alert.acknowledged && isCritical
                      ? 'bg-red-50 border-l-4 border-red-500'
                      : !alert.acknowledged
                      ? 'bg-yellow-50 border-l-4 border-yellow-500'
                      : 'bg-gray-50 border-l-4 border-gray-300 opacity-75'
                  }`}
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="flex-1 min-w-0">
                      {/* Header with severity and timestamp */}
                      <div className="flex items-center gap-2 mb-2 flex-wrap">
                        <span
                          className={`px-2 py-1 rounded text-xs font-bold border ${getSeverityColor(
                            alert.severity
                          )}`}
                        >
                          {getSeverityIcon(alert.severity)} {alert.severity.toUpperCase()}
                        </span>
                        {!alert.acknowledged && (
                          <span className="px-2 py-0.5 bg-red-500 text-white text-xs font-semibold rounded animate-pulse">
                            NEW
                          </span>
                        )}
                        {alert.acknowledged && (
                          <span className="px-2 py-0.5 bg-gray-400 text-white text-xs font-semibold rounded">
                            ACKNOWLEDGED
                          </span>
                        )}
                        <span className="text-xs text-gray-500 ml-auto">
                          {formatTimestamp(alert.timestamp)}
                        </span>
                      </div>

                      {/* Alert Message */}
                      <div className="mb-2">
                        <h3 className="font-semibold text-gray-900 text-sm mb-1">
                          {alert.headline || alert.message}
                        </h3>
                        {alert.description && (
                          <p className="text-sm text-gray-700 line-clamp-2">{alert.description}</p>
                        )}
                        {!alert.description && alert.message && alert.message !== alert.headline && (
                          <p className="text-sm text-gray-700 line-clamp-2">{alert.message}</p>
                        )}
                      </div>

                      {/* Alert Metadata */}
                      <div className="flex flex-wrap gap-2 text-xs text-gray-600 mb-2">
                        {alert.callId && (
                          <span className="px-1.5 py-0.5 bg-blue-100 text-blue-700 rounded">
                            Call: {alert.callId}
                          </span>
                        )}
                        {alert.alert_type && (
                          <span className="px-1.5 py-0.5 bg-purple-100 text-purple-700 rounded">
                            {alert.alert_type}
                          </span>
                        )}
                        {alert.event && (
                          <span className="px-1.5 py-0.5 bg-green-100 text-green-700 rounded">
                            {alert.event}
                          </span>
                        )}
                      </div>

                      {/* Alert Details */}
                      {(alert.urgency || alert.certainty || alert.effective || alert.expires) && (
                        <div className="mt-2 pt-2 border-t border-gray-200 space-y-1 text-xs text-gray-600">
                          {alert.urgency && (
                            <div>
                              <span className="font-medium">Urgency:</span> {alert.urgency}
                            </div>
                          )}
                          {alert.certainty && (
                            <div>
                              <span className="font-medium">Certainty:</span> {alert.certainty}
                            </div>
                          )}
                          {alert.effective && (
                            <div>
                              <span className="font-medium">Effective:</span>{' '}
                              {new Date(alert.effective).toLocaleString()}
                            </div>
                          )}
                          {alert.expires && (
                            <div>
                              <span className="font-medium">Expires:</span>{' '}
                              {new Date(alert.expires).toLocaleString()}
                            </div>
                          )}
                        </div>
                      )}
                    </div>

                    {/* Acknowledge Button */}
                    {!alert.acknowledged && (
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          handleAcknowledge(alert.alertId);
                        }}
                        disabled={isAcknowledging}
                        className={`px-3 py-1.5 rounded text-xs font-medium transition-colors whitespace-nowrap ${
                          isAcknowledging
                            ? 'bg-gray-300 text-gray-500 cursor-not-allowed'
                            : 'bg-blue-600 text-white hover:bg-blue-700'
                        }`}
                      >
                        {isAcknowledging ? 'Acknowledging...' : 'Acknowledge'}
                      </button>
                    )}
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

