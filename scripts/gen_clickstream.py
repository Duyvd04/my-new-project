import csv
import random
import os
from datetime import datetime, timedelta

def gen_clickstream():
    # Tạo thư mục nếu chưa có
    os.makedirs('data/raw/clickstream', exist_ok=True)
    
    file_path = 'data/raw/clickstream/log_01.csv'
    actions = ['view', 'add_to_cart', 'purchase']
    channels = [1, 2, 3] # Google, Facebook, Organic
    
    print(f"--- Đang tạo dữ liệu mẫu clickstream tại {file_path} ---")
    
    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['session_id', 'user_id', 'product_id', 'action', 'timestamp', 'channel_id'])
        
        for i in range(10000): # Tạo 10k dòng để test nhanh
            user_id = random.randint(1, 10)
            product_id = random.randint(1, 5)
            action = random.choice(actions)
            # Tạo thời gian ngẫu nhiên trong 24h qua
            dt = datetime.now() - timedelta(minutes=random.randint(0, 1440))
            
            writer.writerow([
                f'sess_{random.randint(1000, 9999)}',
                user_id,
                product_id,
                action,
                dt.strftime('%Y-%m-%d %H:%M:%S'),
                random.choice(channels)
            ])
    print("✅ Đã tạo xong file Clickstream mẫu!")

if __name__ == "__main__":
    gen_clickstream()