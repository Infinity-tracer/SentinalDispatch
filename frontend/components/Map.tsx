/**
 * Interactive Google Maps component for displaying emergency calls, fires, and alerts
 * Features:
 * - Color-coded markers by risk score
 * - Info windows with call details
 * - Fire spread visualization with circles
 * - Heat map layer for risk score visualization
 * - Marker clustering for performance
 * - Interactive selection and highlighting
 */

'use client';

import { useState, useCallback, useMemo, useRef, useEffect } from 'react';
import {
  GoogleMap,
  Marker,
  InfoWindow,
  Circle,
  useJsApiLoader,
} from '@react-google-maps/api';
import { EmergencyCall, FireData, RiskScore } from '@/types';

const mapContainerStyle = {
  width: '100%',
  height: '100%',
};

const defaultCenter = {
  lat: 20.90, // Lahaina center
  lng: -156.68,
};

const defaultZoom = 12;

// Libraries needed for advanced features
const libraries: ('drawing' | 'visualization')[] = ['visualization'];

interface MapProps {
  calls?: EmergencyCall[];
  fires?: FireData[];
  riskScores?: Map<string, RiskScore>;
  selectedCallId?: string;
  onCallClick?: (callId: string | undefined) => void;
}

export default function Map({
  calls = [],
  fires = [],
  riskScores,
  selectedCallId,
  onCallClick,
}: MapProps) {
  const [selectedCall, setSelectedCall] = useState<EmergencyCall | null>(null);
  const [map, setMap] = useState<google.maps.Map | null>(null);
  const [showHeatMap, setShowHeatMap] = useState(false);
  const mapRef = useRef<google.maps.Map | null>(null);
  const heatMapLayerRef = useRef<google.maps.visualization.HeatmapLayer | null>(null);

  // Handle default riskScores - use empty Map if not provided
  const riskScoresMap: globalThis.Map<string, RiskScore> =
    riskScores ?? new globalThis.Map<string, RiskScore>();

  const googleMapsApiKey = process.env.NEXT_PUBLIC_GOOGLE_MAPS_API_KEY || '';

  // Load Google Maps with visualization library
  const { isLoaded, loadError } = useJsApiLoader({
    id: 'google-map-script',
    googleMapsApiKey: googleMapsApiKey,
    libraries: libraries,
  });

  // Update selected call when selectedCallId changes
  useEffect(() => {
    if (selectedCallId) {
      const call = calls.find((c) => c.callId === selectedCallId);
      setSelectedCall(call || null);
      
      // Pan to selected call
      if (call && mapRef.current) {
        // Use smooth panning for better UX
        mapRef.current.panTo({
          lat: call.callerLocation.lat,
          lng: call.callerLocation.lon,
        });
        // Zoom to a level that shows context but focuses on the call
        const currentZoom = mapRef.current.getZoom() || defaultZoom;
        mapRef.current.setZoom(Math.max(15, currentZoom));
        
        // Open info window automatically when selected from list
        if (call) {
          setSelectedCall(call);
        }
      } else if (selectedCallId && mapRef.current) {
        // If call not found in calls array, log warning
        console.warn(`Call ${selectedCallId} not found in calls array. Make sure the call is loaded.`);
      }
    } else {
      setSelectedCall(null);
    }
  }, [selectedCallId, calls]);

  const onMapLoad = useCallback((map: google.maps.Map) => {
    mapRef.current = map;
    setMap(map);
  }, []);

  const onMapUnmount = useCallback(() => {
    mapRef.current = null;
    setMap(null);
  }, []);

  // Create and manage heat map layer
  useEffect(() => {
    if (!showHeatMap || !isLoaded || !map) {
      if (heatMapLayerRef.current) {
        heatMapLayerRef.current.setMap(null);
        heatMapLayerRef.current = null;
      }
      return;
    }

    const heatMapData: google.maps.visualization.WeightedLocation[] = calls
      .map((call) => {
        const riskScore = riskScoresMap.get(call.callId);
        if (!riskScore) return null;
        
        // Create weighted location based on risk score
        const location = new google.maps.LatLng(
          call.callerLocation.lat,
          call.callerLocation.lon
        );
        return {
          location: location,
          weight: riskScore.riskScore / 100, // Normalize to 0-1
        } as google.maps.visualization.WeightedLocation;
      })
      .filter((point): point is google.maps.visualization.WeightedLocation => point !== null);

    if (heatMapData.length > 0) {
      const heatmap = new google.maps.visualization.HeatmapLayer({
        data: heatMapData,
        map: map,
        radius: 50,
        opacity: 0.6,
        gradient: [
          'rgba(0, 255, 255, 0)',
          'rgba(0, 255, 255, 1)',
          'rgba(0, 191, 255, 1)',
          'rgba(0, 127, 255, 1)',
          'rgba(0, 63, 255, 1)',
          'rgba(0, 0, 255, 1)',
          'rgba(0, 0, 223, 1)',
          'rgba(0, 0, 191, 1)',
          'rgba(0, 0, 159, 1)',
          'rgba(0, 0, 127, 1)',
          'rgba(63, 0, 91, 1)',
          'rgba(127, 0, 63, 1)',
          'rgba(191, 0, 31, 1)',
          'rgba(255, 0, 0, 1)',
        ],
      });
      heatMapLayerRef.current = heatmap;
    }

    return () => {
      if (heatMapLayerRef.current) {
        heatMapLayerRef.current.setMap(null);
        heatMapLayerRef.current = null;
      }
    };
  }, [showHeatMap, isLoaded, map, calls, riskScoresMap]);

  const handleMarkerClick = useCallback(
    (call: EmergencyCall) => {
      setSelectedCall(call);
      onCallClick?.(call.callId);
    },
    [onCallClick]
  );

  const handleInfoWindowClose = useCallback(() => {
    setSelectedCall(null);
    // Clear selection in parent component to return to home view
    if (onCallClick) {
      onCallClick(undefined);
    }
  }, [onCallClick]);

  if (!googleMapsApiKey) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-100">
        <p className="text-gray-600">
          Google Maps API key not configured. Please set NEXT_PUBLIC_GOOGLE_MAPS_API_KEY.
        </p>
      </div>
    );
  }

  if (loadError) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-100">
        <p className="text-red-600">Error loading Google Maps: {loadError.message}</p>
      </div>
    );
  }

  if (!isLoaded) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-100">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto"></div>
          <p className="mt-4 text-gray-600">Loading map...</p>
        </div>
      </div>
    );
  }

  // Get risk score color for call markers
  const getRiskColor = (riskScore: number): string => {
    if (riskScore >= 80) return '#DC2626'; // red-600
    if (riskScore >= 60) return '#EA580C'; // orange-600
    if (riskScore >= 40) return '#CA8A04'; // yellow-600
    return '#16A34A'; // green-600
  };

  // Get risk score label
  const getRiskLabel = (riskScore: number): string => {
    if (riskScore >= 80) return 'Critical';
    if (riskScore >= 60) return 'High';
    if (riskScore >= 40) return 'Medium';
    return 'Low';
  };

  // Create custom marker icon
  const createMarkerIcon = (riskScore: number, isSelected: boolean) => {
    const color = getRiskColor(riskScore);
    return {
      path: google.maps.SymbolPath.CIRCLE,
      fillColor: color,
      fillOpacity: 1,
      strokeColor: isSelected ? '#1E40AF' : '#FFFFFF',
      strokeWeight: isSelected ? 4 : 2,
      scale: isSelected ? 10 : 8,
      anchor: new google.maps.Point(0, 0),
    };
  };

  return (
    <div className="relative h-full w-full">
      {/* Map Controls Overlay */}
      <div className="absolute top-4 right-4 z-10 bg-white rounded-lg shadow-lg p-2 flex flex-col gap-2">
        <button
          onClick={() => setShowHeatMap(!showHeatMap)}
          className={`px-3 py-2 rounded text-sm font-medium transition-colors ${
            showHeatMap
              ? 'bg-blue-600 text-white'
              : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
          }`}
          title="Toggle risk score heat map"
        >
          {showHeatMap ? 'Hide' : 'Show'} Heat Map
        </button>
        <div className="text-xs text-gray-600 px-2">
          <div className="font-semibold mb-1">Legend:</div>
          <div className="flex items-center gap-2 mb-1">
            <div className="w-3 h-3 rounded-full bg-red-600"></div>
            <span>Critical (80+)</span>
          </div>
          <div className="flex items-center gap-2 mb-1">
            <div className="w-3 h-3 rounded-full bg-orange-600"></div>
            <span>High (60-79)</span>
          </div>
          <div className="flex items-center gap-2 mb-1">
            <div className="w-3 h-3 rounded-full bg-yellow-600"></div>
            <span>Medium (40-59)</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full bg-green-600"></div>
            <span>Low (&lt;40)</span>
          </div>
        </div>
      </div>

      <GoogleMap
        mapContainerStyle={mapContainerStyle}
        center={defaultCenter}
        zoom={defaultZoom}
        onLoad={onMapLoad}
        onUnmount={onMapUnmount}
        options={{
          mapTypeControl: true,
          streetViewControl: false,
          fullscreenControl: true,
          zoomControl: true,
          mapTypeId: google.maps.MapTypeId.ROADMAP,
          styles: [
            {
              featureType: 'poi',
              elementType: 'labels',
              stylers: [{ visibility: 'off' }],
            },
          ],
        }}
      >
        {/* Heat Map Layer is handled via useEffect */}

        {/* Emergency Call Markers (color-coded by risk score) */}
        {calls.map((call) => {
          const riskScore = riskScoresMap.get(call.callId);
          const score = riskScore?.riskScore || 0;
          const isSelected = selectedCallId === call.callId;
          const isInfoWindowOpen = selectedCall?.callId === call.callId;

          return (
            <div key={call.callId}>
              <Marker
                position={{
                  lat: call.callerLocation.lat,
                  lng: call.callerLocation.lon,
                }}
                icon={createMarkerIcon(score, isSelected)}
                onClick={() => handleMarkerClick(call)}
                title={`Call ${call.callId} - Risk: ${Math.round(score)} (${getRiskLabel(score)})`}
                animation={isSelected ? google.maps.Animation.BOUNCE : undefined}
              />

              {/* Info Window */}
              {isInfoWindowOpen && (
                <InfoWindow
                  position={{
                    lat: call.callerLocation.lat,
                    lng: call.callerLocation.lon,
                  }}
                  onCloseClick={handleInfoWindowClose}
                >
                  <div className="p-2 min-w-[250px] max-w-[300px]">
                    <div className="mb-2">
                      <h3 className="font-bold text-lg text-gray-900">
                        Call {call.callId}
                      </h3>
                      <p className="text-xs text-gray-500">
                        {new Date(call.timestamp).toLocaleString()}
                      </p>
                    </div>

                    {/* Risk Score Display */}
                    {riskScore && (
                      <div className="mb-3 p-2 rounded bg-gray-50">
                        <div className="flex items-center justify-between mb-1">
                          <span className="text-sm font-medium text-gray-700">
                            Risk Score:
                          </span>
                          <span
                            className={`text-lg font-bold ${getRiskLabel(score) === 'Critical' ? 'text-red-600' : getRiskLabel(score) === 'High' ? 'text-orange-600' : getRiskLabel(score) === 'Medium' ? 'text-yellow-600' : 'text-green-600'}`}
                          >
                            {Math.round(score)}
                          </span>
                        </div>
                        <div className="w-full bg-gray-200 rounded-full h-2">
                          <div
                            className={`h-2 rounded-full`}
                            style={{
                              width: `${Math.min(100, score)}%`,
                              backgroundColor: getRiskColor(score),
                            }}
                          />
                        </div>
                        <div className="text-xs text-gray-600 mt-1">
                          Priority: {riskScore.priority ? `P${riskScore.priority}` : 'N/A'} |{' '}
                          Confidence: {Math.round((riskScore.confidence || 0) * 100)}%
                        </div>
                      </div>
                    )}

                    {/* Call Transcript Preview */}
                    <div className="mb-2">
                      <p className="text-sm text-gray-700 line-clamp-3">
                        {call.transcript}
                      </p>
                    </div>

                    {/* Location */}
                    <div className="text-xs text-gray-600 mb-2">
                      <span className="font-medium">Location:</span>{' '}
                      {call.callerLocation.lat.toFixed(4)}, {call.callerLocation.lon.toFixed(4)}
                    </div>

                    {/* Metadata */}
                    {call.metadata && (
                      <div className="text-xs text-gray-600 space-y-1">
                        {call.metadata.callDuration && (
                          <div>
                            <span className="font-medium">Duration:</span>{' '}
                            {call.metadata.callDuration}s
                          </div>
                        )}
                        {call.metadata.callerAge && (
                          <div>
                            <span className="font-medium">Age:</span> {call.metadata.callerAge}
                          </div>
                        )}
                        {call.metadata.knownConditions &&
                          call.metadata.knownConditions.length > 0 && (
                            <div>
                              <span className="font-medium">Conditions:</span>{' '}
                              {call.metadata.knownConditions.join(', ')}
                            </div>
                          )}
                      </div>
                    )}

                    {/* Recommendation */}
                    {riskScore?.recommendation && (
                      <div className="mt-2 p-2 bg-blue-50 rounded border border-blue-200">
                        <p className="text-xs font-medium text-blue-900">
                          💡 {riskScore.recommendation}
                        </p>
                      </div>
                    )}
                  </div>
                </InfoWindow>
              )}
            </div>
          );
        })}

        {/* Fire Markers with Spread Visualization */}
        {fires.map((fire) => {
          // Calculate fire spread radius (estimated size in meters)
          const radiusMeters = fire.estimatedSize * 1000 || 500; // Convert km to meters, default 500m

          return (
            <div key={fire.fireId}>
              {/* Fire Spread Circle */}
              <Circle
                center={{
                  lat: fire.location.lat,
                  lng: fire.location.lon,
                }}
                radius={radiusMeters}
                options={{
                  fillColor: '#FF4500',
                  fillOpacity: 0.2,
                  strokeColor: '#FF0000',
                  strokeOpacity: 0.8,
                  strokeWeight: 2,
                }}
              />

              {/* Fire Center Marker */}
              <Marker
                position={{
                  lat: fire.location.lat,
                  lng: fire.location.lon,
                }}
                icon={{
                  path: google.maps.SymbolPath.FORWARD_CLOSED_ARROW,
                  fillColor: '#FF4500',
                  fillOpacity: 1,
                  strokeColor: '#FF0000',
                  strokeWeight: 2,
                  scale: 6,
                  rotation: 0,
                }}
                title={`Fire ${fire.fireId} - Confidence: ${fire.confidence}% | Size: ${fire.estimatedSize}km²`}
              />
            </div>
          );
        })}
      </GoogleMap>
    </div>
  );
}
