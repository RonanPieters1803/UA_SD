import argparse
import hmac
import json
import os
import socket
import sqlite3
import sys
import threading
import time
import uuid
from collections import deque
from datetime import datetime

class DataBase:
    def __init__(self, path):
        folder = os.path.dirname(path)
        if folder:
            os.makedirs(folder, exist_ok=True)
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        with self.lock:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS operators (
                    id TEXT PRIMARY KEY,
                    name TEXT 
                );
                CREATE TABLE IF NOT EXISTS watering_stations (
                    id            TEXT PRIMARY KEY,
                    location      TEXT NOT NULL,
                    blocked       INTEGER NOT NULL DEFAULT 0,
                    registered_at TEXT
                    );""")