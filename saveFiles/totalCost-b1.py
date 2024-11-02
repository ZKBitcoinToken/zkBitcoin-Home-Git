from functools import partial
from multiprocessing import Pool
from web3 import Web3
import time
import random
import json
import os

web3 = Web3(Web3.HTTPProvider('https://zksync-mainnet.g.alchemy.com/v2/7KlXx0nexhjU5Cqp28a1kaB3GLudQzE-'))

TOKEN_ADDRESS = '0x366d17aDB24A7654DbE82e79F85F9Cb03c03cD0D'
ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'

def get_transaction_cost(tx_hash):
    try:
        #tx = web3.eth.get_transaction(tx_hash)
        time.sleep(0.1)
        receipt = web3.eth.get_transaction_receipt(tx_hash)
        gas_used = receipt['gasUsed']
        gas_price = receipt['effectiveGasPrice']
        #print("gas price tx: ", tx)
        #print("gas price: ", gas_price)
        #print("gas used receipt: ", receipt)
        #print("gas used: ", gas_used)
        return gas_used * gas_price
    except Exception as e:
        #print("Error:, "+e)
        print("ERROR HAPPENED WAITING 12 SECONDS THEN RETRYING LETS GO")
        random_float_rangef = random.uniform(3, 12)
        time.sleep(random_float_rangef)
        return get_transaction_cost(tx_hash)
def decode_log_data(data):
    return int.from_bytes(data, byteorder='big')


def fetch_non_zero_transactions_from_zero_address(start_block, end_block, step=100000):
    transfer_event_signature = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    mint_event_signature = '0xcf6fbb9dcea7d07263ab4f5c3a92f53af33dffc421d9d121e1c74b307e68189d'
    results = []
    for block in range(start_block, end_block, step):
        time.sleep(0.15)
        to_block = min(block + step - 1, end_block)
        print("Searching: "+str(block)+" to " + str(to_block) +" blks");
        try:
            logs = web3.eth.get_logs({
                'fromBlock': block,
                'toBlock': to_block,
                'address': TOKEN_ADDRESS,
                'topics': [transfer_event_signature]
            })
        except Exception as e:
            print("Error: "+str(e))
            print("sleeping 20 sec")
            time.sleep(20)
            logs = web3.eth.get_logs({
                    'fromBlock': block,
                    'toBlock': to_block,
                    'address': TOKEN_ADDRESS,
                    'topics': [transfer_event_signature]
            })
            time.sleep(0.35)
            
        
        try:
            time.sleep(0.35)
            logs2 = web3.eth.get_logs({
                'fromBlock': block,
                'toBlock': to_block,
                'address': TOKEN_ADDRESS,
                'topics': [mint_event_signature]
            })
        except Exception as e:
            print("Error: "+str(e))
            print("sleeping 20 sec")
            time.sleep(20)
            time.sleep(0.35)
            logs2 = web3.eth.get_logs({
                    'fromBlock': block,
                    'toBlock': to_block,
                    'address': TOKEN_ADDRESS,
                    'topics': [mint_event_signature]
            })
        #print("Logs = "+ str(logs))
        #print("logs2 = "+str(logs))
	#I Want to remove any logs that dont also have logs2 blockHash if the blockHash isnt in logs and logs2 remove the entry from logs please
        # Create a set of block hashes from logs2
        logs2_block_hashes = {log['blockHash'] for log in logs2}
        
        # Filter logs to keep only those with blockHash in logs2
        filtered_logs = [log for log in logs if log['blockHash'] in logs2_block_hashes]
        
        # Update logs with the filtered logs
        logs = filtered_logs
        
# Use a set to track seen transaction hashes
        seen_tx_hashes = set()

# Filter logs to keep only those with blockHash in logs2 and unique transaction hashes
        filtered_logs = []
        for log in logs:
            tx_hash = log['transactionHash']
            if log['blockHash'] in logs2_block_hashes and tx_hash not in seen_tx_hashes:
                filtered_logs.append(log)
                seen_tx_hashes.add(tx_hash)  # Add the transaction hash to the set

        # Update logs with the filtered logs
        logs = filtered_logs
        #print("Filtered Logs = "+ str(logs))
        
        time.sleep(1)  # Adjust sleep time as necessary for rate limits
	#I Want to remove any logs that dont also have logs2 blockHash if the blockHash isnt in logs and logs2 remove the entry from logs please
        with Pool(NUM_THREADS) as pool:
            for r in pool.map(docrap, logs):
                if r:
                    results.append(r)
    return results

NUM_THREADS = 4

def docrap(log):
    from_address = '0x' + log['topics'][1].hex()[-40:]
    to_address = '0x' + log['topics'][2].hex()[-40:]
    print("log['data']: "+str(decode_log_data(log['data'])))
    print("from_address: "+from_address)
    print("tx_hash: "+str(log['transactionHash'].hex()))
    value = decode_log_data(log['data'])
    if from_address == ZERO_ADDRESS and value != 0:
        tx_hash = log['transactionHash']
        # Generate a random float between 1.0 and 10.0
        random_float_range = random.uniform(0.01, 1)
        print("WAITING THIS MANY RANDOM to find transaction cost "+ str(random_float_range))
        time.sleep(random_float_range)
        cost = get_transaction_cost(tx_hash)
        return {
            'tx_hash': tx_hash.hex(),
            'from': from_address,
            'to': to_address,
            'value': value,
            'cost': cost
        }
    return None

def save_results(address_totals, filename_prefix='transaction_analysis'):
    # Save detailed results
    
    detailed_filename = f'saveFiles/{filename_prefix}_detailed.json'
    
        # Load existing data if the file exists
    if os.path.exists(detailed_filename):
        with open(detailed_filename, 'r') as f:
            try:
                # Load past data
                past_data = json.load(f)
                # Ensure past_data is a dictionary
                
                if isinstance(past_data, dict):
                    past_data = [past_data]  # Wrap the dict in a list
                elif not isinstance(past_data, list):
                    past_data = []  # Reset to empty list if it's neither	
            except json.JSONDecodeError:
                past_data = []
    else:
        past_data = []
        
    # Combine current address_totals with past data
    # Assuming address_totals is a list of dictionaries
    combined_data = past_data + address_totals
    #print("COMBINED DATA: ", combined_data)
    with open(detailed_filename, 'w') as f:
        json.dump(combined_data, f, indent=2)
    
    # Dictionary to store aggregated results
    aggregated_data = {}
    # Save summary results
    # Loop over each transaction
    for txn in combined_data:
        to_address = txn["to"]
        if to_address not in aggregated_data:
            # Initialize entry if not already present
            aggregated_data[to_address] = {
                "to": to_address,
                "total_value": 0,
                "total_cost": 0,
                "transaction_count": 0
            }
    
            # Aggregate the values
        aggregated_data[to_address]["total_value"] += txn["value"]
        aggregated_data[to_address]["total_cost"] += txn["cost"]
        aggregated_data[to_address]["transaction_count"] +=1

    # Convert the result to a list if desired
    result = list(aggregated_data.values())

    # Sort the result by total_cost in descending order
    sorted_result = sorted(result, key=lambda x: x["total_cost"], reverse=True)

    # Display result
    sorted_result
	
    summary_filename = f'saveFiles/{filename_prefix}_cost_summary.json'
    with open(summary_filename, 'w') as json_file:
            json.dump(sorted_result, json_file, indent=2)
    summary_filename2 = f'saveFiles/{filename_prefix}_summary.txt'
    with open(summary_filename2, 'w') as f:
        f.write("Address Cost Summary\n")
        f.write("==================\n\n")
        # Loop over each transaction
    
        grand_total = 0
        for tx in sorted_result:
            total_cost = tx['total_cost']
            grand_total += total_cost
            f.write(f"Address: {tx['to']}\n")
            f.write(f"Total Cost: {tx['total_cost']}\n")
            f.write(f"Transaction Count: {tx['transaction_count']}\n")
            f.write(f"Average Cost per Transaction: {total_cost / tx['transaction_count']:.2f}\n")
            f.write("-" * 50 + "\n\n")
            
        f.write(f"Grand Total Cost Across All Addresses: {grand_total}\n")
        f.write(f"Total Unique Addresses: {len(aggregated_data)}\n")
        f.write(f"Total Mint Transactions : {len(combined_data)}\n")
        
        # Create a dictionary with simplified statistics
        stats = {
    "TotalCost": grand_total,
    "UniqueAddresses": len(aggregated_data),
    "MintTransactions": len(combined_data)
        }

        # Write the dictionary to a JSON file
        
        summary_total_stats_filename = f'saveFiles/{filename_prefix}_cost_summary_total_stats.json'
        with open(summary_total_stats_filename, 'w') as json_file:
            json.dump(stats, json_file, indent=4)
    
    

if __name__ == '__main__':
    # Path to the file
    file_path = "LastBlock_ETH.txt"

    # Reading the number from the file
    with open(file_path, "r") as file:
        # Read and strip any extra whitespace
        last_block = int(file.read().strip()) + 1
    print("Last Block: ", last_block)
    # Fetch and print the non-zero transactions from 0x0# Define the block range
    start_block = last_block #28876372 #last_block
    end_block =  last_block  + 1000000 #web3.eth.block_number #29460418
    if(end_block > web3.eth.block_number):
    	end_block = web3.eth.block_number
    print("start block: ",start_block)	
    print("end_block: ", end_block)
    # Fetch and print the non-zero transactions from 0x0
    transactions = fetch_non_zero_transactions_from_zero_address(start_block, end_block)
    totalCostFF = 0
    NumberofTransactions = 0

    # Sort transactions by cost (largest to smallest)
    transactions_sorted = sorted(transactions, key=lambda x: x['cost'], reverse=True)

    # Ensure the directory exists
    #os.makedirs('/saveFiles', exist_ok=True)
    # Prepare to write output to a file
    with open('saveFiles/transaction_costs.txt', 'w') as file:
        total_cost = 0
        NumberofTransactions = len(transactions_sorted)


    # Write header
        file.write(f"Total Transactions: {NumberofTransactions}\n")
        file.write("Transaction Costs (sorted from largest to smallest):\n")
        file.write("---------------------------------------------------\n")

    # Write transaction details
        for tx in transactions_sorted:
            file.write(f"Transaction Hash: {tx['tx_hash']}\n")
            file.write(f"From: {tx['from']}\n")
            file.write(f"To: {tx['to']}\n")
            file.write(f"Value: {tx['value']}\n")
            file.write(f"Transaction Cost: {tx['cost']}\n")
            file.write("---------------------------------------------------\n")
            total_cost += tx['cost']

    # Write total cost
        file.write(f"\n\nTotal Cost: {total_cost}\n")
        file.write(f"Number of Transactions: {NumberofTransactions}\n")
        file.write(f"From block : {start_block}\n")
        file.write(f"to block : {end_block}\n")
    print(f"Total Cost: {total_cost}")
    print(f"Number of Transactions: {NumberofTransactions}")
    print(f"Transaction details written to 'transaction_costs.txt'.")
    print("Tx srted: ", transactions_sorted)
    # Save results
    save_results(transactions_sorted)
    # Write end_block to the file
    with open(file_path, "w") as file:
        file.write(str(end_block))  # Write the end_block as a string

    print("End Block written to file:", end_block)
