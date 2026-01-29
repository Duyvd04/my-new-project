import pandas as pd
import numpy as np
import os

num_rows = 1000000 # 1 triệu dòng
file_path = "/opt/airflow/data/input/clickstream.csv"

data = {
    'timestamp': pd.date_range(start='2026-01-27', periods=num_rows, freq='S'),
    'user_id': np.random.randint(1, 100, num_rows),
    'product_id': np.random.randint(1, 10, num_rows),
    'action': np.random.choice(['view', 'add_to_cart', 'purchase'], num_rows),
    'channel_id': np.random.choice([1, 2, 3], num_rows),
    'session_id': [f"sess_{i}" for i in range(num_rows)],
    'ip_address': [f"192.168.1.{np.random.randint(1, 255)}" for _ in range(num_rows)],
    'device_type': np.random.choice(['mobile', 'desktop', 'tablet'], num_rows),
    'browser': np.random.choice(['Chrome', 'Safari', 'Firefox'], num_rows),
    'os': np.random.choice(['Windows', 'MacOS', 'Android', 'iOS'], num_rows),
    'region': np.random.choice(['North', 'South', 'Central'], num_rows),
    'referrer_url': 'google.com',
    'stay_duration': np.random.randint(5, 300, num_rows),
    'is_logged_in': True,
    'screen_res': '1920x1080',
    'url': '/home',
    'campaign': 'tet_2026',
    'lat': 21.02,
    'long': 105.83,
    'error_msg': None
}

df = pd.DataFrame(data)
os.makedirs(os.path.dirname(file_path), exist_ok=True)
df.to_csv(file_path, index=False)
print(f"✅ Đã tạo file Clickstream tại {file_path}")
