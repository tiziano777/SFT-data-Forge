#!/bin/bash

# =================================================================
# Nginx configuration script for Streamlit HTTPS Reverse Proxy
# Date: 2026-01-23
# Version: 3.0 (Optimized configuration for Streamlit)
# =================================================================

# 1. Configuration variables
VM_IP="10.121.194.153"
CONF_NAME="sft-data-forge"
NGINX_AVAILABLE="/etc/nginx/sites-available/$CONF_NAME"
NGINX_ENABLED="/etc/nginx/sites-enabled/$CONF_NAME"
SSL_DIR="/etc/nginx/ssl"

# Check if script is running as root
if [ "$EUID" -ne 0 ]; then
  echo "Please run this script as root (sudo ./setup_nginx_https_streamlit.sh)"
  exit 1
fi

echo "================================================================="
echo "NGINX CONFIGURATION FOR STREAMLIT - VERSION 3.0"
echo "================================================================="

echo ""
echo "--- 1. Kernel Optimization (Fix inotify limit for Watchdog) ---"
# Fixes OSError: [Errno 24] inotify instance limit reached
sysctl -w fs.inotify.max_user_instances=1024 > /dev/null
sysctl -w fs.inotify.max_user_watches=524288 > /dev/null
echo "fs.inotify.max_user_instances=1024" | tee -a /etc/sysctl.conf > /dev/null
echo "fs.inotify.max_user_watches=524288" | tee -a /etc/sysctl.conf > /dev/null
echo "Kernel optimization completed"

echo ""
echo "--- 2. Nginx and OpenSSL Installation ---"
if ! command -v nginx &> /dev/null; then
    echo "Installing Nginx and dependencies..."
    apt-get update && apt-get install -y nginx openssl curl
    echo "Nginx installed"
else
    echo "Nginx already installed"
fi

echo ""
echo "--- 3. SSL Certificate Management (Self-Signed) ---"
if [ ! -d "$SSL_DIR" ]; then
    mkdir -p "$SSL_DIR"
    echo "SSL directory created: $SSL_DIR"
fi

if [ ! -f "$SSL_DIR/$CONF_NAME.crt" ]; then
    echo "Generating Self-Signed certificates..."
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
      -keyout "$SSL_DIR/$CONF_NAME.key" \
      -out "$SSL_DIR/$CONF_NAME.crt" \
      -subj "/C=IT/ST=Lazio/L=Rome/O=SFTDataForge/CN=$VM_IP"
    echo "SSL certificates generated"
else
    echo "SSL certificates already present"
fi

echo ""
echo "--- 4. Firewall Configuration (UFW) ---"
echo "Configuring ports..."
ufw allow 80/tcp > /dev/null
echo "  Port 80 (HTTP) opened"
ufw allow 443/tcp > /dev/null
echo "  Port 443 (HTTPS) opened"
ufw allow 22/tcp > /dev/null
echo "  Port 22 (SSH) opened"
ufw --force enable > /dev/null 2>&1
echo "Firewall configured and enabled"

echo ""
echo "--- 5. Creating optimized Nginx configuration file ---"
echo "Creating Streamlit configuration..."

cat <<EOF > $NGINX_AVAILABLE
# ====================================================
# Streamlit configuration with WebSocket support
# ====================================================

# HTTP -> HTTPS redirect
server {
    listen 80;
    server_name $VM_IP;
    # Automatic redirect to HTTPS
    return 301 https://\$host\$request_uri;
}

# HTTPS main server
server {
    listen 443 ssl;
    server_name $VM_IP;

    # ==================== SSL CONFIGURATION ====================
    ssl_certificate $SSL_DIR/$CONF_NAME.crt;
    ssl_certificate_key $SSL_DIR/$CONF_NAME.key;
    
    # SSL security settings
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers ECDHE-RSA-AES256-GCM-SHA512:DHE-RSA-AES256-GCM-SHA512:ECDHE-RSA-AES256-GCM-SHA384:DHE-RSA-AES256-GCM-SHA384;
    ssl_prefer_server_ciphers off;
    ssl_session_cache shared:SSL:10m;
    ssl_session_timeout 10m;
    
    # ==================== STREAMLIT PROXY CONFIG ====================
    # File upload size (for datasets)
    client_max_body_size 200M;
    
    # Proxy buffering disabled for Streamlit
    proxy_buffering off;
    proxy_request_buffering off;
    
    # ==================== MAIN APPLICATION ====================
    location / {
        proxy_pass http://127.0.0.1:8501;
        
        # Standard proxy headers
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header X-Forwarded-Host \$host;
        proxy_set_header X-Forwarded-Port \$server_port;
        
        # WebSocket support (CRITICAL for Streamlit)
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        
        # Timeout settings for long operations
        proxy_read_timeout 86400s;
        proxy_send_timeout 86400s;
        proxy_connect_timeout 86400s;
        
        # Disable cache for dynamic content
        proxy_no_cache 1;
        proxy_cache_bypass 1;
    }
    
    # ==================== STREAMLIT SPECIFIC ENDPOINTS ====================
    # Health check endpoint
    location /_stcore/health {
        proxy_pass http://127.0.0.1:8501/_stcore/health;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        access_log off;
    }
    
    # Static assets - cache for performance
    location ~* \.(js|css|png|jpg|jpeg|gif|ico|svg|woff|woff2|ttf|eot)$ {
        proxy_pass http://127.0.0.1:8501;
        proxy_cache_valid 200 302 1h;
        proxy_cache_valid 404 1m;
        expires 1y;
        add_header Cache-Control "public, immutable";
    }
    
    # ==================== ERROR PAGES ====================
    error_page 502 /502.html;
    location = /502.html {
        root /usr/share/nginx/html;
        internal;
    }
}
EOF

echo "Nginx configuration created"

echo ""
echo "--- 6. Site Activation and Cleanup ---"
# Remove default to avoid conflicts on port 80
if [ -f "/etc/nginx/sites-enabled/default" ]; then
    echo "Removing default configuration..."
    rm -f /etc/nginx/sites-enabled/default
    echo "Default configuration removed"
fi

# Create symbolic link (forced -sf)
if [ -f "$NGINX_ENABLED" ]; then
    rm -f "$NGINX_ENABLED"
fi
ln -sf "$NGINX_AVAILABLE" "$NGINX_ENABLED"
echo "Site enabled in sites-enabled"

echo ""
echo "--- 7. Configuration Verification and Application ---"
echo "Testing Nginx configuration..."
if nginx -t 2>/dev/null; then
    echo "Configuration syntax OK"

    # Reload Nginx (safer than restart)
    echo "Applying configuration..."
    systemctl reload nginx

    if [ $? -eq 0 ]; then
        echo "Nginx reloaded successfully"
    else
        echo "Falling back to full restart..."
        systemctl restart nginx
        echo "Nginx restarted"
    fi

    # Wait a few seconds for stabilization
    sleep 3

    # Base connection test
    echo ""
    echo "--- 8. Connection Test ---"
    echo "Testing local access..."
    if curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8501/_stcore/health 2>/dev/null | grep -q "200"; then
        echo "Streamlit is responding on port 8501"
    else
        echo "Streamlit is not responding on port 8501"
        echo "  Check that the Docker container is running"
    fi

    echo "Testing access via Nginx HTTPS..."
    if curl -k -s -o /dev/null -w "%{http_code}" https://$VM_IP/_stcore/health 2>/dev/null | grep -q "200"; then
        echo "Nginx proxy is working"
    else
        echo "Nginx is not responding correctly"
        echo "  Check the logs: sudo tail -f /var/log/nginx/error.log"
    fi

    echo ""
    echo "================================================================="
    echo "CONFIGURATION COMPLETED SUCCESSFULLY!"
    echo "================================================================="
    echo ""
    echo "The application is accessible at the following URLs:"
    echo "  HTTPS (public): https://$VM_IP"
    echo "  HTTP (local): http://127.0.0.1:8501"
    echo ""
    echo "To monitor logs:"
    echo "  Nginx access: sudo tail -f /var/log/nginx/access.log"
    echo "  Nginx error:  sudo tail -f /var/log/nginx/error.log"
    echo "  Docker logs:  docker logs -f sft-data-forge-prod"
    echo ""
    echo "To troubleshoot common issues:"
    echo "  1. Verify the container is running: docker ps"
    echo "  2. Check the app logs: docker logs sft-data-forge-prod"
    echo "  3. Test the proxy: curl -k https://$VM_IP/_stcore/health"
    echo ""
    echo "================================================================="
    
else
    echo ""
    echo "CRITICAL ERROR IN NGINX CONFIGURATION"
    echo ""
    echo "Check the errors above."
    echo "For manual debugging:"
    echo "  1. sudo nginx -t"
    echo "  2. sudo nano $NGINX_AVAILABLE"
    echo "  3. Check Nginx syntax"
    echo ""
    exit 1
fi

# Show final service status
echo ""
echo "Final service status:"
echo "---------------------"
systemctl status nginx --no-pager -l | head -20