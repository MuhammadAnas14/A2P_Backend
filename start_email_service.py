#!/usr/bin/env python3
"""
Email Service Starter
--------------------
Starts the email analyst service as a Flask API server.
This runs separately from the main app.py to handle email processing.
"""

import os
import sys
from email_analyst import run_email_service

if __name__ == "__main__":
    # Set default port for email service
    port = int(os.getenv('EMAIL_SERVICE_PORT', 5001))
    host = os.getenv('EMAIL_SERVICE_HOST', '0.0.0.0')
    debug = os.getenv('EMAIL_SERVICE_DEBUG', 'false').lower() == 'true'
    
    print(f"Starting Email Service on {host}:{port}")
    run_email_service(host=host, port=port, debug=debug)
