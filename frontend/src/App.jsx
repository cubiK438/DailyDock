import React, { useState, useEffect } from 'react';
import './App.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '';

export default function App() {
  const [connectionStatus, setConnectionStatus] = useState('loading'); // 'loading' | 'connected' | 'disconnected'
  const [healthData, setHealthData] = useState(null);
  const [errorMessage, setErrorMessage] = useState(null);
  const [lastChecked, setLastChecked] = useState(null);

  const checkHealth = async () => {
    setConnectionStatus('loading');
    setErrorMessage(null);
    try {
      const response = await fetch(`${API_BASE_URL}/api/health`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      setHealthData(data);
      setConnectionStatus('connected');
    } catch (err) {
      setConnectionStatus('disconnected');
      setHealthData(null);
      setErrorMessage(err.message || 'Failed to connect to backend server');
    } finally {
      setLastChecked(new Date().toLocaleTimeString());
    }
  };

  useEffect(() => {
    checkHealth();
  }, []);

  return (
    <div className="container">
      <header className="header">
        <h1 className="title">DailyDock</h1>
        <p className="subtitle">Personal Infrastructure &amp; Automation Platform</p>
      </header>

      <main>
        <section className={`status-card ${connectionStatus}`}>
          <div className="status-header">
            <span className="status-label">Backend Status</span>
            <span className={`badge ${connectionStatus}`}>
              <span className="badge-dot" />
              {connectionStatus === 'connected' && 'Connected'}
              {connectionStatus === 'disconnected' && 'Disconnected'}
              {connectionStatus === 'loading' && 'Checking...'}
            </span>
          </div>

          {connectionStatus === 'connected' && healthData && (
            <div className="status-details">
              <div className="detail-row">
                <span className="detail-label">Service Health:</span>
                <span className="detail-value">{healthData.status}</span>
              </div>
              <div className="detail-row">
                <span className="detail-label">Application:</span>
                <span className="detail-value">{healthData.app}</span>
              </div>
              <div className="detail-row">
                <span className="detail-label">Version:</span>
                <span className="detail-value">{healthData.version}</span>
              </div>
              <div className="detail-row">
                <span className="detail-label">Environment:</span>
                <span className="detail-value">{healthData.environment}</span>
              </div>
            </div>
          )}

          {connectionStatus === 'disconnected' && (
            <div className="error-message">
              <strong>Error:</strong> {errorMessage}
            </div>
          )}

          {lastChecked && (
            <div className="status-details" style={{ marginTop: '0.75rem', fontSize: '0.75rem', opacity: 0.8 }}>
              Last checked: {lastChecked}
            </div>
          )}
        </section>

        <div className="actions">
          <button
            type="button"
            className="button"
            onClick={checkHealth}
            disabled={connectionStatus === 'loading'}
          >
            {connectionStatus === 'loading' ? 'Checking...' : 'Refresh Status'}
          </button>
        </div>
      </main>
    </div>
  );
}

