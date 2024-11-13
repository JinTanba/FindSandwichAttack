import calendar
import random
import subprocess
import json
import gzip
import os
from typing import Generator, Dict, Any


class RcloneProcessor:
    def __init__(self, base_path: str):
        """
        Initialize the processor with base path for rclone
        
        Args:
            base_path (str): Base path for rclone (e.g., 'ovhgrap:block-submission-bundles-prod')
        """
        self.base_path = base_path

    def list_files(self, path: str) -> list[str]:
        """
        List all .ndjson.gz files in the specified path
        
        Args:
            path (str): Relative path to list files from
            
        Returns:
            list[str]: List of file paths
        """
        cmd = ['rclone', 'ls', os.path.join(self.base_path, path)]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"Failed to list files: {result.stderr}")
            
        files = []
        for line in result.stdout.splitlines():
            if line.strip():
                # rclone ls outputs format: "size filepath"
                parts = line.strip().split()
                if len(parts) >= 2 and parts[-1].endswith('.ndjson.gz'):
                    files.append(parts[-1])
        
        return files

    def read_json_file(self, file_path: str) -> Generator[Dict[str, Any], None, None]:
        """
        Read and parse a .ndjson.gz file from rclone
        
        Args:
            file_path (str): Path to the file relative to base_path
            
        Yields:
            dict: Each JSON object from the file
        """
        cmd = ['rclone', 'cat', os.path.join(self.base_path, file_path)]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        
        # Use gzip to decompress the stream
        with gzip.open(process.stdout, 'rt') as f:
            for line in f:
                if line.strip():
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON from {file_path}: {e}")
                        continue



def generate_random_path():
    """
    2023年の9~11月の中からランダムで日付と時間を選びます!
    Returns:
        str: Random path in format YYYY/MM/DD/HH/MM
    """
    # 月と年の組み合わせの定義
    year_month_pairs = [
        (2023, month) for month in range(9, 12)  # 9月から11月
    ]
    
    # ランダムに年と月を選択
    year, month = random.choice(year_month_pairs)
    
    # 選択された月の最終日を取得
    _, last_day = calendar.monthrange(year, month)
    
    # ランダムに日付を選択 (1-30/31)
    day = random.randint(1, last_day)
    
    # ランダムに時間を選択 (0-23)
    hour = random.randint(0, 23)
    
    # ランダムに分を選択 (0-59)
    minute = random.randint(0, 59)
    
    # パスを構築
    return f"{year}/{month:02d}/{day:02d}/{hour:02d}/{minute:02d}"