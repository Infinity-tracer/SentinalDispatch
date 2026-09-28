/**
 * React hook for WebSocket connection
 */

import { useEffect, useRef, useState, useCallback } from 'react';
import { WebSocketClient } from '@/lib/websocket';
import { WebSocketMessage } from '@/types';

export function useWebSocket(url: string) {
  const [isConnected, setIsConnected] = useState(false);
  const [lastMessage, setLastMessage] = useState<WebSocketMessage | null>(null);
  const clientRef = useRef<WebSocketClient | null>(null);

  useEffect(() => {
    if (!url) {
      return;
    }

    const client = new WebSocketClient(url);
    clientRef.current = client;

    const unsubscribeConnection = client.onConnectionStateChange((connected) => {
      setIsConnected(connected);
    });

    const unsubscribeMessage = client.onMessage((message) => {
      setLastMessage(message);
    });

    client.connect();

    return () => {
      unsubscribeConnection();
      unsubscribeMessage();
      client.disconnect();
    };
  }, [url]);

  const sendMessage = useCallback((message: object) => {
    if (clientRef.current) {
      clientRef.current.send(message);
    }
  }, []);

  return {
    isConnected,
    lastMessage,
    sendMessage,
  };
}

