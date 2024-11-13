import json
import os
import asyncio
from datetime import datetime
import random
import calendar
import aiohttp
from web3 import AsyncWeb3
from web3.exceptions import BlockNotFound
from typing import Optional, Dict, Any, List, Tuple, Callable
from rconfReader import RcloneProcessor, generate_random_path
from functools import partial
import requests

# Configuration
RPC_URL = "https://mainnet.infura.io/v3/05c6709f3eed48eb89c7e82d7a43c0dc"
rpc_url_list = [
    "https://eth-mainnet.g.alchemy.com/v2/z0DcWozljD4dS9s_QU1aPLp04-TwKF4L",
    "https://eth-mainnet.g.alchemy.com/v2/AHvTHUHmlCKWoa5hezH-MTrKWw_MjtUZ",
    "https://eth-mainnet.g.alchemy.com/v2/JQQZbWHiH0HV1pFYyFMb1ByJWSh8qfio",
    "https://mainnet.infura.io/v3/05c6709f3eed48eb89c7e82d7a43c0dc",
    "https://eth-mainnet.g.alchemy.com/v2/lv6VTUpYhxl8NgJCfopCi4_chCvMNQ7D"
]
CHUNK_SIZE = len(rpc_url_list)
BASE_PATH = "ovhgrap:block-submission-bundles-prod"

# Pure functions for data transformation
def normalize_block_hash(block_hash: str) -> str:
    return f"0x{block_hash}" if not block_hash.startswith('0x') else block_hash

def format_block_info(block: Dict) -> Dict[str, Any]:
    return {
        'number': block.number,
        'hash': block.hash.hex(),
        'timestamp': block.timestamp,
        'miner': block.miner,
        'transactions': [
            tx.hex() if isinstance(tx, bytes) else tx 
            for tx in block.transactions
        ]
    }

def is_sandwich_attack(tx_hash_list_onchain: List[str], bundle_tsx: List[str], start_idx: int) -> bool:
    """tx_hash_list_onchainからbundle_tsxを部分列の探索する"""
    if start_idx + len(bundle_tsx) > len(tx_hash_list_onchain):
        return False
    return all(
        tx_hash_list_onchain[start_idx + i] == bundle_tsx[i]
        for i in range(len(bundle_tsx))
    )


async def create_web3_connection() -> AsyncWeb3:
    """Web3接続を作成"""
    return AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(RPC_URL))

def create_web3_pool(rpc_url_list: List[str]) -> List[AsyncWeb3]:
    """Web3コネクションプールを作成。負荷分散を使用する際はsessionを使用する"""
    return [AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(rpc_url)) for rpc_url in rpc_url_list]

async def fetch_block_by_hash(w3: AsyncWeb3, block_hash: str) -> Optional[Dict[str, Any]]:
    """実際にmainnetに乗ったブロックの詳細をblock_hashから取得"""
    try:
        normalized_hash = normalize_block_hash(block_hash)
        if not await w3.eth.get_block_transaction_count(normalized_hash):
            return None
            
        block = await w3.eth.get_block(normalized_hash, full_transactions=True)
        return format_block_info(block)
    except (BlockNotFound, Exception) as e:
        # print(f"Block fetch error: {e}")
        return None


def fetch_bundle_by_block_number(block_number: int) -> Optional[Dict[str, Any]]:
    """libmevのAPIを使用して、特定のブロックに含まれるバンドル情報を取得"""
    try:
        url = "https://api.libmev.com/v1/bundles"
        params = {"blockRange": f"{block_number},{block_number}"}
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        return response.json()
    except Exception as e:
        print(f"Bundle fetch error: {e}")
        return None

async def process_single_submission(
    w3: AsyncWeb3,
    submission: Dict,
) -> Optional[Dict]:
    """単一のサブミッション処理"""
    try:
        block_hash = submission["payload"]["execution_payload"]["block_hash"]
        block_info = await fetch_block_by_hash(w3, block_hash)
        
        return {
            'block_info': block_info,
            'submission': submission
        } if block_info else None
        
    except Exception as e:
        print(f"Submission processing error: {e}")
        return None

async def process_submissions_concurrently(
    submissions: List[Dict],
    web3_pool: List[AsyncWeb3],
    max_concurrent: int = 5
) -> Optional[Dict]:
    """ここで、実際にmainnetに乗ったブロックを特定しています.
    持っているrpcノード個数にすべてのブロックを分割して、並列に実行されます
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    found_result = None
    
    async def bounded_process(submission: Dict, task_id: int) -> Optional[Dict]:
        async with semaphore:
            return await process_single_submission(
                web3_pool[0],
                submission
            )
    
    # シャッフルした新しいリストを作成（不変性を保持）
    shuffled_submissions = random.sample(submissions, len(submissions))
    
    tasks = [
        asyncio.create_task(bounded_process(submission, i))
        for i, submission in enumerate(shuffled_submissions)
    ]
    
    for completed in asyncio.as_completed(tasks):
        try:
            result = await completed
            if result and result.get('block_info'):
                # 残りのタスクをキャンセル
                for task in tasks:
                    task.cancel()
                return result
        except asyncio.CancelledError:
            pass
    
    return None

async def analyze_bundles(block_info: Dict) -> None:
    tx_hash_list_onchain = ["0x"+str(tx_data["hash"].hex()) for tx_data in block_info["transactions"]]
    bundle_info = fetch_bundle_by_block_number(block_info['number'])
    bundle = bundle_info
    for bundle in bundle_info["data"]:
        bundle_tsx = bundle['txs']
        for start_idx, onchain_tx in enumerate(tx_hash_list_onchain):
                if bundle_tsx[0] == onchain_tx:
                    if is_sandwich_attack(tx_hash_list_onchain, bundle_tsx, start_idx):
                        print(f"🥪 Sandwich attack detected in block")
                        print(tx_hash_list_onchain[start_idx: start_idx + len(bundle_tsx)])

async def analyze_file(processor: RcloneProcessor, file_path: str, web3_pool: List[AsyncWeb3]) -> None:
    try:
        submissions = processor.read_json_file(file_path)
        print(f"Processing {len(submissions)} submissions...")
        
        result = await process_submissions_concurrently(submissions, web3_pool, CHUNK_SIZE)
        if result:
            block_info = result['block_info']
            print(f"🔥 Found block in chain: {block_info['hash']}")
            # print(block_info)
            await analyze_bundles(block_info)
            
    except Exception as e:
        print(f"File analysis error: {e}")


async def analyze_local_file(file_path: str) -> None:
    web3_pool = create_web3_pool(rpc_url_list)
    """
    Args:
        file_path: ローカルファイルのパス
        web3_pool: Web3コネクションプール
    """
    try:
        submissions = [];
        with open(file_path, 'r') as file:
            submissions = [json.loads(line) for line in file]
            
        print(f"Processing local file with {len(submissions)} submissions...")
        
        result = await process_submissions_concurrently(submissions, web3_pool, CHUNK_SIZE)
        if result:
            block_info = result['block_info']
            print(f"🔥 Found block in chain: {block_info['hash']}")
            # print(block_info)
            await analyze_bundles(block_info)
            
    except Exception as e:
        print(f"Local file analysis error: {e}")




async def main():
    processor = RcloneProcessor(BASE_PATH)
    web3_pool = create_web3_pool(rpc_url_list)
    local_file_path = "./7664691.ndjson"; ##ここはローカルのファイルを使用してください

    if not local_file_path:
        for analysis_round in range(100):
            try:
                date_path = generate_random_path()
                print(f"\n=== Analysis Round {analysis_round + 1} ===")
                print(f"Analyzing path: {date_path}")
                
                files = processor.list_files(date_path)
                print(f"Found {len(files)} files")
                
                if files:
                    selected_file = random.choice(files)
                    print(f"\nAnalyzing file: {selected_file}")
                    await analyze_file(
                        processor,
                        os.path.join(date_path, selected_file),
                        web3_pool
                    )
                    
            except Exception as e:
                print(f"Round {analysis_round + 1} error: {e}")
    else:
        print("Analyzing local file...", local_file_path)
        await analyze_local_file(local_file_path)

if __name__ == "__main__":
    asyncio.run(main())