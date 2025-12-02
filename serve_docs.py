#!/usr/bin/env python3
import http.server
import socketserver
import os

PORT = 5000

class MyHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        super().end_headers()
    
    def do_GET(self):
        if self.path == '/':
            self.path = '/index.html'
        return super().do_GET()

os.chdir(os.path.dirname(os.path.abspath(__file__)))

with socketserver.TCPServer(("0.0.0.0", PORT), MyHTTPRequestHandler) as httpd:
    print(f"Documentation server running at http://0.0.0.0:{PORT}")
    print("=" * 80)
    print("⚠️  IMPORTANT: This is NOT the Streamlit app!")
    print("=" * 80)
    print("")
    print("This server serves deployment documentation for the Snowflake Streamlit app.")
    print("The actual app runs ONLY in Snowflake Snowsight, not here.")
    print("")
    print("To deploy:")
    print("1. Open the web preview to view deployment instructions")
    print("2. Follow the guide to deploy to Snowsight")
    print("3. Copy streamlit_app.py code into Snowsight's Streamlit editor")
    print("")
    httpd.serve_forever()
