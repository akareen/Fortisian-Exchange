/**
 * Configuration Loader for Fortisian Exchange Frontend
 * Loads configuration from JSON files in the config folder
 */

class ConfigLoader {
  constructor() {
    this.configs = {};
    this.loaded = false;
  }

  async load() {
    if (this.loaded) return this.configs;

    try {
      // Load colors config
      const colorsResponse = await fetch('./config/colors.json');
      if (!colorsResponse.ok) throw new Error('Colors config not found');
      this.configs.colors = await colorsResponse.json();

      // Load frontend config
      const frontendResponse = await fetch('./config/frontend.json');
      if (!frontendResponse.ok) throw new Error('Frontend config not found');
      this.configs.frontend = await frontendResponse.json();

      // Resolve environment-specific URLs
      const isDev = window.location.hostname === 'localhost';
      this.configs.frontend.API_URL = isDev 
        ? this.configs.frontend.api_url.development
        : this.configs.frontend.api_url.production.replace('${window.location.protocol}', window.location.protocol).replace('${window.location.hostname}', window.location.hostname);
      
      this.configs.frontend.WS_URL = isDev
        ? this.configs.frontend.ws_url.development
        : this.configs.frontend.ws_url.production
            .replace('${window.location.protocol === \'https:\' ? \'wss:\' : \'ws:\'}', window.location.protocol === 'https:' ? 'wss:' : 'ws:')
            .replace('${window.location.hostname}', window.location.hostname);

      this.loaded = true;
      return this.configs;
    } catch (error) {
      console.error('Failed to load config:', error);
      // Fallback to defaults
      return this.getDefaults();
    }
  }

  getDefaults() {
    return {
      colors: {
        bg: '#0a0a0c',
        bgDark: '#060608',
        panel: '#101013',
        panelHover: '#18181c',
        border: '#2d2d30',
        borderLight: '#3a3a3d',
        orange: '#ffa028',
        orangeHover: '#ffb850',
        green: '#00c26a',
        greenBg: 'rgba(0,194,106,0.12)',
        red: '#f74747',
        redBg: 'rgba(247,71,71,0.12)',
        white: '#fff',
        text: '#d0d0d0',
        muted: '#818181',
        mutedDark: '#5c5c5c',
        yellow: '#ffcc00',
        yellowBg: 'rgba(255,204,0,0.12)',
        cyan: '#00a0c6',
        cyanBg: 'rgba(0,160,198,0.12)',
        purple: '#9c27b0',
        purpleBg: 'rgba(156,39,176,0.12)'
      },
      frontend: {
        API_URL: window.location.hostname === 'localhost' ? 'http://localhost:8080' : `${window.location.protocol}//${window.location.hostname}:8080`,
        WS_URL: window.location.hostname === 'localhost' ? 'ws://localhost:8080/ws' : `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.hostname}:8080/ws`,
        ADMIN_URL: 'fortisian_admin.html',
        RECONNECT_DELAYS: [500, 1000, 2000, 4000, 8000, 15000, 30000],
        PING_INTERVAL: 25000,
        MAX_TRADES: 200,
        MAX_PRICE_HISTORY: 1000,
        BOOK_FLASH_DURATION: 400,
        VERSION: '2.1.1'
      }
    };
  }

  getColors() {
    return this.configs.colors || this.getDefaults().colors;
  }

  getFrontendConfig() {
    return this.configs.frontend || this.getDefaults().frontend;
  }
}

// Create global instance
window.configLoader = new ConfigLoader();

