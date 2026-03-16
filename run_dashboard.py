#!/usr/bin/env python
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

if __name__ == '__main__':
    from src.dashboard.app import create_app
    app = create_app()
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
