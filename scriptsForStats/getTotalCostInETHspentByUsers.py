from functools import partial
from multiprocessing import Pool
from web3 import Web3
import time

web3 = Web3(Web3.HTTPProvider('https://mainnet.era.zksync.io'))

TOKEN_ADDRESS = '0x366d17aDB24A7654DbE82e79F85F9Cb03c03cD0D'
ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'

def get_transaction_cost(tx_hash):
    try:
        tx = web3.eth.get_transaction(tx_hash)
        receipt = web3.eth.get_transaction_receipt(tx_hash)
        gas_used = receipt['gasUsed']
        gas_price = tx['gasPrice']
        return gas_used * gas_price
    except:
        print("ERROR HAPPENED WAITING 30 SECONDS THEN RETRYING LETS GO")
        time.sleep(20)
        return get_transaction_cost(tx_hash)
def decode_log_data(data):
    return int.from_bytes(data, byteorder='big')

def fetch_non_zero_transactions_from_zero_address(start_block, end_block, step=100000):
    transfer_event_signature = web3.keccak(text='Transfer(address,address,uint256)').hex()
    results = []
    for block in range(start_block, end_block, step):
        time.sleep(0.15)
        to_block = min(block + step - 1, end_block)
        print("Searching: "+str(block)+" to " + str(to_block) +" blks");
        logs = web3.eth.get_logs({
            'fromBlock': block,
            'toBlock': to_block,
            'address': TOKEN_ADDRESS,
            'topics': [transfer_event_signature]
        })
        with Pool(NUM_THREADS) as pool:
            for r in pool.map(docrap, logs):
                if r:
                    results.append(r)
    return results

NUM_THREADS = 8

def docrap(log):
    from_address = '0x' + log['topics'][1].hex()[-40:]
    to_address = '0x' + log['topics'][2].hex()[-40:]
    print("log['data']: "+str(decode_log_data(log['data'])))
    print("from_address: "+from_address)
    print("tx_hash: "+str(log['transactionHash'].hex()))
    value = decode_log_data(log['data'])
    if from_address == ZERO_ADDRESS and value != 0:
        tx_hash = log['transactionHash']
        cost = get_transaction_cost(tx_hash)
        return {
            'tx_hash': tx_hash.hex(),
            'from': from_address,
            'to': to_address,
            'value': value,
            'cost': cost
        }
    return None

if __name__ == '__main__':
    # Fetch and print the non-zero transactions from 0x0# Define the block range
    start_block = 28876372
    end_block = web3.eth.block_number

    # Fetch and print the non-zero transactions from 0x0
    transactions = fetch_non_zero_transactions_from_zero_address(start_block, end_block)
    totalCostFF = 0
    for tx in transactions:
        print(f"Transaction Hash: {tx['tx_hash']}")
        print(f"From: {tx['from']}")
        print(f"To: {tx['to']}")
        print(f"Value: {tx['value']}")
        print(f"Transaction Cost: {tx['cost']}\n")
        totalCostFF += int(tx['cost'])

    print("Total Cost: "+ str(totalCostFF));
