# **Admin API Endpoints (Improved & Standardized)**

The admin panel communicates with the backend using the following API endpoints:

| Endpoint               | Method | Description                          |
| ---------------------- | ------ | ------------------------------------ |
| `/admin/stats`         | GET    | Retrieve system statistics           |
| `/admin/users`         | GET    | List all users                       |
| `/admin/users/create`  | POST   | Create a new user                    |
| `/admin/users/reset`   | POST   | Reset a user’s position              |
| `/admin/ban`           | POST   | Ban a user                           |
| `/admin/unban`         | POST   | Unban a user                         |
| `/admin/sessions`      | GET    | List all active sessions             |
| `/admin/sessions/kick` | POST   | Forcefully disconnect a session      |
| `/admin/markets`       | GET    | List markets                         |
| `/admin/market/start`  | POST   | Start market trading                 |
| `/admin/market/halt`   | POST   | Halt a market (no new orders)        |
| `/admin/market/stop`   | POST   | Stop market trading and close market |
| `/admin/audit`         | GET    | Retrieve audit log events            |

---

# **Production Deployment Guide (Improved)**

## **Server Requirements**

Recommended minimum for a small production deployment:

* Ubuntu 22.04+ or Debian 12+
* 2+ CPU cores
* 4GB+ RAM
* 20GB+ SSD storage
* Python 3.12+
* MongoDB 7.0+ (optional but recommended)

---

## **Step 1: Server Setup**

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Python 3.12
sudo apt install -y python3.12 python3.12-venv python3-pip
```

### **Install MongoDB 7.0 (Optional, Recommended)**

```bash
curl -fsSL https://pgp.mongodb.com/server-7.0.asc \
  | sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor

echo "deb [ signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] \
https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" \
| sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list

sudo apt update
sudo apt install -y mongodb-org
sudo systemctl enable --now mongod
```

### **Install Nginx + Certbot**

```bash
sudo apt install -y nginx certbot python3-certbot-nginx
```

### **Create Application User**

```bash
sudo useradd -m -s /bin/bash fortisian
sudo mkdir -p /opt/fortisian
sudo chown fortisian:fortisian /opt/fortisian
```

---

## **Step 2: Deploy Application**

```bash
sudo su - fortisian

mkdir -p /opt/fortisian/{backend,frontend,logs}

# Upload your application files via SCP:
# scp -r exchange/* fortisian@your-server:/opt/fortisian/backend/
# scp fortisian_v2.html fortisian_admin.html fortisian@your-server:/opt/fortisian/frontend/

cd /opt/fortisian/backend

python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Test run
python server.py --port 8080
# Press Ctrl+C after confirming it works
```

---

## **Step 3: Create Systemd Service**

```bash
sudo tee /etc/systemd/system/fortisian.service << 'EOF'
[Unit]
Description=Fortisian Trading Exchange
After=network.target mongodb.service
Wants=mongodb.service

[Service]
Type=simple
User=fortisian
Group=fortisian
WorkingDirectory=/opt/fortisian/backend
Environment=PATH=/opt/fortisian/backend/venv/bin:/usr/bin
ExecStart=/opt/fortisian/backend/venv/bin/python server.py --host 127.0.0.1 --port 8080
Restart=always
RestartSec=5
StandardOutput=append:/opt/fortisian/logs/server.log
StandardError=append:/opt/fortisian/logs/error.log

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable fortisian
sudo systemctl start fortisian
sudo systemctl status fortisian
```

---

# **Domain Setup (fortisian.com)**

## **Step 1: DNS Configuration**

| Type | Name | Value          | TTL |
| ---- | ---- | -------------- | --- |
| A    | @    | YOUR_SERVER_IP | 300 |
| A    | www  | YOUR_SERVER_IP | 300 |
| A    | api  | YOUR_SERVER_IP | 300 |

Example:

```
Host: @
IP: 203.0.113.50
TTL: Auto/300
```

---

## **Step 2: Nginx Configuration**

*(Improved for clarity and correctness)*

```bash
sudo tee /etc/nginx/sites-available/fortisian << 'EOF'
# Redirect HTTP → HTTPS
server {
    listen 80;
    listen [::]:80;
    server_name fortisian.com www.fortisian.com;
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    listen [::]:443 ssl http2;
    server_name fortisian.com www.fortisian.com;

    # SSL (certbot will fill these in)
    ssl_certificate /etc/letsencrypt/live/fortisian.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/fortisian.com/privkey.pem;
    include /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;

    # Security headers
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;
    add_header Referrer-Policy "strict-origin-when-cross-origin" always;

    # Frontend
    root /opt/fortisian/frontend;
    index fortisian_v2.html;

    location / {
        try_files $uri $uri/ /fortisian_v2.html;
    }

    # API Proxy
    location /api/ {
        proxy_pass http://127.0.0.1:8080/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # CORS
        add_header Access-Control-Allow-Origin "https://fortisian.com" always;
        add_header Access-Control-Allow-Credentials "true" always;
        add_header Access-Control-Allow-Methods "GET, POST, OPTIONS" always;
        add_header Access-Control-Allow-Headers "Content-Type, Authorization" always;

        if ($request_method = OPTIONS) {
            return 204;
        }
    }

    # WebSockets
    location /ws {
        proxy_pass http://127.0.0.1:8080/ws;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_read_timeout 86400;
        proxy_send_timeout 86400;
    }

    # Admin page
    location /admin {
        alias /opt/fortisian/frontend;
        try_files /fortisian_admin.html =404;
    }

    # Health check
    location /health {
        proxy_pass http://127.0.0.1:8080/health;
    }

    access_log /var/log/nginx/fortisian_access.log;
    error_log /var/log/nginx/fortisian_error.log;
}
EOF

sudo ln -sf /etc/nginx/sites-available/fortisian /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl reload nginx
```

---

# **SSL (HTTPS) Setup**

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d fortisian.com -d www.fortisian.com
sudo certbot renew --dry-run
sudo systemctl status certbot.timer
```

---

# **Frontend Production Configuration**

```javascript
const CONFIG = {
  API_URL: window.location.hostname === 'localhost'
    ? 'http://localhost:8080'
    : `${window.location.protocol}//${window.location.hostname}/api`,

  WS_URL: window.location.hostname === 'localhost'
    ? 'ws://localhost:8080/ws'
    : `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.hostname}/ws`,
};
```

---

# **Firewall**

```bash
sudo ufw allow 22/tcp
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable
sudo ufw status
```

---

# **Monitoring & Logs (Improved)**

```bash
tail -f /opt/fortisian/logs/server.log
tail -f /opt/fortisian/logs/error.log
tail -f /var/log/nginx/fortisian_access.log
sudo journalctl -u fortisian -f
```

---

# **Troubleshooting (Cleaner & More Direct)**

### **CORS Issues**

* Mismatch between frontend CONFIG and Nginx proxy
* Missing CORS headers in server or Nginx

### **WebSocket Failures**

Test:

```bash
wscat -c ws://localhost:8080/ws         # Direct
wscat -c wss://fortisian.com/ws         # Through Nginx
```

### **502 Bad Gateway**

* Backend not running
* Wrong port in Nginx
* Crash logs in `journalctl`

### **MongoDB Issues**

* Check service: `sudo systemctl status mongod`
* Ping: `mongosh --eval "db.adminCommand('ping')"`

### **Login Sessions Breaking**

* Cookies not being set
* Missing `credentials: 'include'`
* CORS misconfiguration

---

# **Quick Reference**

### **Production**

* Trading UI: `https://fortisian.com/`
* Admin: `https://fortisian.com/admin`
* API: `https://fortisian.com/api/`
* WebSocket: `wss://fortisian.com/ws`
* Health: `https://fortisian.com/health`

### **Service Commands**

```bash
sudo systemctl restart fortisian
sudo systemctl reload nginx
sudo systemctl status mongod
```

---

# **Security Checklist (Improved)**

* [ ] Change default admin password immediately
* [ ] Enable HTTPS
* [ ] Configure UFW firewall
* [ ] Install Fail2Ban for SSH
* [ ] Use a secure session secret in production
* [ ] Enable MongoDB authentication
* [ ] Perform automatic backups
* [ ] Keep packages updated
* [ ] Monitor logs regularly