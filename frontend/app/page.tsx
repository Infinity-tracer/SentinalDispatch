/**
 * Main dashboard page
 */

'use client';

import { useState, useEffect } from 'react';
import { useWebSocket } from '@/hooks/useWebSocket';
import { apiClient } from '@/lib/api';
import Map from '@/components/Map';
import PrioritizedCaseList from '@/components/PrioritizedCaseList';
import RiskScoreDetailPanel from '@/components/RiskScoreDetailPanel';
import AlertsPanel from '@/components/AlertsPanel';
import {
  EmergencyCall,
  RiskScore,
  FireData,
  Alert,
  WebSocketMessage,
} from '@/types';

export default function Dashboard() {
  const [calls, setCalls] = useState<EmergencyCall[]>([]);
  const [riskScores, setRiskScores] = useState<Map<string, RiskScore>>(new globalThis.Map<string, RiskScore>());
  const [fires, setFires] = useState<FireData[]>([]);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [selectedCallId, setSelectedCallId] = useState<string | undefined>();
  const [isLoading, setIsLoading] = useState(true);

  const wsUrl = process.env.NEXT_PUBLIC_WS_URL || 'ws://localhost:8000/ws';
  const { isConnected, lastMessage, sendMessage } = useWebSocket(wsUrl);

  // Load initial data
  useEffect(() => {
    const loadInitialData = async () => {
      try {
        const [callsData, riskScoresData, prioritizedCasesData] = await Promise.all([
          apiClient.getCalls(),
          apiClient.getRiskScores(),
          apiClient.getPrioritizedCases().catch(() => ({ cases: [], total: 0 })), // Don't fail if this errors
        ]);

        // Start with calls from the calls endpoint
        const callsMap = new globalThis.Map<string, EmergencyCall>();
        (callsData.calls || []).forEach((call) => {
          callsMap.set(call.callId, call);
        });

        // Also add calls from prioritized cases (in case they're not in the main calls list)
        if (prioritizedCasesData.cases) {
          prioritizedCasesData.cases.forEach((case_) => {
            if (case_.emergencyCall && !callsMap.has(case_.emergencyCall.callId)) {
              callsMap.set(case_.emergencyCall.callId, case_.emergencyCall);
            }
          });
        }

        setCalls(Array.from(callsMap.values()));
        
        const scoresMap = new globalThis.Map<string, RiskScore>();
        riskScoresData.forEach((score) => {
          scoresMap.set(score.callId, score);
        });
        setRiskScores(scoresMap);
      } catch (error) {
        console.error('Error loading initial data:', error);
      } finally {
        setIsLoading(false);
      }
    };

    loadInitialData();
  }, []);

  // Handle WebSocket messages
  useEffect(() => {
    if (!lastMessage) return;

    const handleMessage = (message: WebSocketMessage) => {
      switch (message.type) {
        case 'new_call':
          setCalls((prev) => [message.data as EmergencyCall, ...prev]);
          break;

        case 'risk_score_update':
          const riskScore = message.data as RiskScore;
          setRiskScores((prev) => {
            const updated = new globalThis.Map<string, RiskScore>(prev);
            updated.set(riskScore.callId, riskScore);
            return updated;
          });
          break;

        case 'alert':
          setAlerts((prev) => {
            const alert = message.data as Alert;
            const index = prev.findIndex((a) => a.alertId === alert.alertId);
            if (index >= 0) {
              const updated = [...prev];
              updated[index] = alert;
              return updated;
            }
            return [...prev, alert];
          });
          break;

        case 'fire_update':
          setFires((prev) => {
            const fire = message.data as FireData;
            const index = prev.findIndex((f) => f.fireId === fire.fireId);
            if (index >= 0) {
              const updated = [...prev];
              updated[index] = fire;
              return updated;
            }
            return [...prev, fire];
          });
          break;

        default:
          console.log('Unhandled message type:', message.type);
      }
    };

    handleMessage(lastMessage);
  }, [lastMessage]);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto"></div>
          <p className="mt-4 text-gray-600">Loading dashboard...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="h-screen flex flex-col">
      {/* Header */}
      <header className="bg-blue-600 text-white p-4 shadow-md">
        <div className="flex items-center justify-between">
          <h1 className="text-2xl font-bold">Sentinel Dispatch</h1>
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-2">
              <div
                className={`w-3 h-3 rounded-full ${
                  isConnected ? 'bg-green-400' : 'bg-red-400'
                }`}
              />
              <span className="text-sm">
                {isConnected ? 'Connected' : 'Disconnected'}
              </span>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Prioritized Case List Sidebar */}
        <div className="w-96 border-r overflow-hidden">
          <PrioritizedCaseList
            selectedCallId={selectedCallId}
            onCaseSelect={setSelectedCallId}
          />
        </div>

        {/* Map */}
        <div className="flex-1 relative">
          <Map
            calls={calls}
            fires={fires}
            riskScores={riskScores}
            selectedCallId={selectedCallId}
            onCallClick={setSelectedCallId}
          />
        </div>

        {/* Right Sidebar - Risk Score Detail Panel or Alerts Panel */}
        <div className="w-96 border-l overflow-hidden flex">
          {/* Tabs for switching between Risk Score Details and Alerts */}
          <div className="flex-1 flex flex-col">
            {selectedCallId ? (
              <RiskScoreDetailPanel
                callId={selectedCallId}
                emergencyCall={
                  calls.find((c) => c.callId === selectedCallId) || null
                }
                riskScore={riskScores.get(selectedCallId) || null}
                onClose={() => setSelectedCallId(undefined)}
              />
            ) : (
              <AlertsPanel
                onAlertClick={(alert) => {
                  if (alert.callId) {
                    setSelectedCallId(alert.callId);
                  }
                }}
                key="alerts-panel"
              />
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

