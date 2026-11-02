import React, { createContext, useState, useEffect, useContext, ReactNode } from 'react';
// Import only on client side
import type Paho from 'paho-mqtt';

interface Message {
  topic: string;
  message: string;
  timestamp: Date;
  parsedJSON?: any;
  retained?: boolean;
}

interface MQTTContextType {
  client: any;
  isConnected: boolean;
  messages: Message[];
  subscribedTopics: string[];
  errorMessage: string;
  connect: (brokerUrl: string, options?: any) => void;
  disconnect: () => void;
  subscribe: (topic: string) => void;
  unsubscribe: (topic: string) => void;
  publish: (topic: string, message: string, retained?: boolean) => void;
  clearMessages: () => void;
  clearError: () => void;
}

export const MQTTContext = createContext<MQTTContextType>({
  client: null,
  isConnected: false,
  messages: [],
  subscribedTopics: [],
  errorMessage: "",
  connect: () => {},
  disconnect: () => {},
  subscribe: () => {},
  unsubscribe: () => {},
  publish: () => {},
  clearMessages: () => {},
  clearError: () => {},
});

interface MQTTProviderProps {
  children: ReactNode;
}

export const MQTTProvider: React.FC<MQTTProviderProps> = ({ children }) => {
  const [client, setClient] = useState<Paho.Client | null>(null);
  const [isConnected, setIsConnected] = useState<boolean>(false);
  const [messages, setMessages] = useState<Message[]>([]);
  const [subscribedTopics, setSubscribedTopics] = useState<string[]>([]);
  const [pahoMqtt, setPahoMqtt] = useState<any>(null);
  const [errorMessage, setErrorMessage] = useState<string>("");

  // Load Paho MQTT client on client side only
  useEffect(() => {
    if (typeof window !== 'undefined') {
      // Only in browser environment
      const loadPaho = async () => {
        try {
          const paho = await import('paho-mqtt');
          setPahoMqtt(paho);
        } catch (err) {
          console.error('Failed to load Paho MQTT client:', err);
        }
      };
      loadPaho();
    }
  }, []);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (client) {
        try {
          client.disconnect();
        } catch (e) {
          // Ignore disconnect errors
        }
      }
    };
  }, [client]);

  const connect = (brokerUrl: string, options?: any) => {
    // Clear any previous errors
    setErrorMessage("");
    
    if (typeof window === 'undefined' || !pahoMqtt) {
      setErrorMessage("MQTT client not initialized. Please reload the page.");
      return; // Do nothing during SSR or if Paho not loaded
    }

    try {
      // Parse URL to extract host, port, and path
      let url: URL;
      try {
        url = new URL(brokerUrl);
      } catch (error) {
        setErrorMessage("Invalid broker URL. Please enter a valid URL.");
        return;
      }
      
      // Check if we need to force WSS based on page protocol
      if (window.location.protocol === 'https:' && url.protocol === 'ws:') {
        console.warn('Upgrading insecure WebSocket to secure WebSocket due to HTTPS page');
        // Change the protocol to wss if the page is served over https
        url = new URL(`wss://${url.host}${url.pathname}${url.search}`);
      }
      
      // Remove protocol prefix (ws:// or wss://) from hostname
      const host = url.hostname;
      // Port defaults to 8000 if not provided
      const port = url.port ? parseInt(url.port) : (url.protocol === 'wss:' ? 443 : 8000);
      // Path defaults to /mqtt if not provided
      const path = url.pathname === "/" ? "/mqtt" : url.pathname;
      
      // Generate a random client ID if not provided
      const clientId = options?.clientId || `mqttjs_${Math.random().toString(16).substring(2, 8)}`;
      
      console.log(`Connecting to ${host}:${port}${path} with client ID ${clientId}`);
      
      // Create Paho MQTT client
      const mqttClient = new pahoMqtt.Client(host, port, path, clientId);
      
      // Set callback handlers
      mqttClient.onConnectionLost = (responseObject) => {
        console.log("MQTT connection lost:", responseObject.errorMessage);
        setIsConnected(false);
        setErrorMessage(`Connection lost: ${responseObject.errorMessage}`);
      };
      
      mqttClient.onMessageArrived = (message) => {
        const topic = message.destinationName;
        const payload = message.payloadString;
        const retained = message.retained;
        
        let parsedJSON = undefined;
        try {
          parsedJSON = JSON.parse(payload);
        } catch (e) {
          // Not JSON, which is fine
        }
        
        setMessages((prev) => [
          ...prev.slice(-999),
          { topic, message: payload, timestamp: new Date(), parsedJSON, retained },
        ]);
      };
      
      // Connect to broker
      mqttClient.connect({
        onSuccess: () => {
          console.log('Connected to MQTT broker');
          setIsConnected(true);
          setClient(mqttClient);
          setErrorMessage("");
        },
        onFailure: (error) => {
          console.error('MQTT connection failed:', error.errorMessage);
          setIsConnected(false);
          setErrorMessage(`Connection failed: ${error.errorMessage}`);
        },
        useSSL: url.protocol === 'wss:',
        reconnect: true,
        timeout: 30
      });
      
    } catch (error) {
      console.error('Failed to connect to MQTT broker:', error);
      setErrorMessage(`Connection error: ${error instanceof Error ? error.message : String(error)}`);
    }
  };

  const disconnect = () => {
    setErrorMessage("");
    if (client && isConnected) {
      try {
        client.disconnect();
        console.log('Disconnected from MQTT broker');
      } catch (e) {
        console.error('Error disconnecting from MQTT broker:', e);
        setErrorMessage(`Error disconnecting: ${e instanceof Error ? e.message : String(e)}`);
      }
      setClient(null);
      setIsConnected(false);
      setSubscribedTopics([]);
      setMessages([]); // Clear messages when disconnecting
    }
  };

  const subscribe = (topic: string) => {
    setErrorMessage("");
    if (!topic || !topic.trim()) {
      setErrorMessage("Topic cannot be empty");
      return;
    }
    
    if (client && isConnected) {
      try {
        client.subscribe(topic);
        setSubscribedTopics((prev) => 
          prev.includes(topic) ? prev : [...prev, topic]
        );
      } catch (e) {
        console.error(`Error subscribing to ${topic}:`, e);
        setErrorMessage(`Error subscribing to ${topic}: ${e instanceof Error ? e.message : String(e)}`);
      }
    } else {
      setErrorMessage("Not connected to MQTT broker");
    }
  };

  const unsubscribe = (topic: string) => {
    setErrorMessage("");
    if (client && isConnected) {
      try {
        client.unsubscribe(topic);
        setSubscribedTopics((prev) => 
          prev.filter((t) => t !== topic)
        );
      } catch (e) {
        console.error(`Error unsubscribing from ${topic}:`, e);
        setErrorMessage(`Error unsubscribing from ${topic}: ${e instanceof Error ? e.message : String(e)}`);
      }
    }
  };

  const publish = (topic: string, message: string, retained: boolean = false) => {
    setErrorMessage("");
    if (!topic || !topic.trim()) {
      setErrorMessage("Topic cannot be empty");
      return;
    }
    
    if (!message || !message.trim()) {
      setErrorMessage("Message cannot be empty");
      return;
    }
    
    if (client && isConnected && pahoMqtt) {
      try {
        const mqttMessage = new pahoMqtt.Message(message);
        mqttMessage.destinationName = topic;
        mqttMessage.retained = retained; // Use the parameter value
        client.send(mqttMessage);
      } catch (e) {
        console.error(`Error publishing to ${topic}:`, e);
        setErrorMessage(`Error publishing to ${topic}: ${e instanceof Error ? e.message : String(e)}`);
      }
    } else {
      setErrorMessage("Not connected to MQTT broker");
    }
  };

  const clearMessages = () => {
    setMessages([]);
  };
  
  const clearError = () => {
    setErrorMessage("");
  };

  return (
    <MQTTContext.Provider
      value={{
        client,
        isConnected,
        messages,
        subscribedTopics,
        errorMessage,
        connect,
        disconnect,
        subscribe,
        unsubscribe,
        publish,
        clearMessages,
        clearError,
      }}
    >
      {children}
    </MQTTContext.Provider>
  );
};

export const useMQTT = () => useContext(MQTTContext);