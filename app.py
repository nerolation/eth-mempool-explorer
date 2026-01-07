import os
import pandas as pd
import pyxatu
from flask import Flask, render_template, request, jsonify
from datetime import datetime

app = Flask(__name__)

# Initialize xatu client (lazy loading)
_xatu = None

# Load builder mapping at startup
_builder_mapping = {}
def load_builder_mapping():
    global _builder_mapping
    try:
        mapping_path = os.path.join(os.path.dirname(__file__), 'builder_mapping.csv')
        df = pd.read_csv(mapping_path)
        for _, row in df.iterrows():
            if pd.notna(row['builder_pubkey']):
                _builder_mapping[row['builder_pubkey']] = row['builder']
    except Exception as e:
        print(f"Warning: Could not load builder mapping: {e}")

load_builder_mapping()

def get_xatu():
    global _xatu
    if _xatu is None:
        _xatu = pyxatu.PyXatu(use_env_variables=True)
    return _xatu


def get_builder_info(slot, slot_start_date_time):
    """Get MEV-boost builder info for a slot."""
    xatu = get_xatu()

    # Query the winning bid, using slot_start_date_time for partition pruning
    result = xatu.execute_query(f"""
        SELECT builder_pubkey
        FROM default.mev_relay_bid_trace
        WHERE meta_network_name = 'mainnet'
          AND slot_start_date_time = toDateTime64('{slot_start_date_time}', 3)
          AND slot = {slot}
        ORDER BY value DESC
        LIMIT 1
    """, columns="builder_pubkey")

    if result is None or result.empty:
        return {'is_mev': False, 'builder': 'Locally Built', 'builder_pubkey': None}

    builder_pubkey = result['builder_pubkey'].iloc[0]
    builder_name = _builder_mapping.get(builder_pubkey, 'Unknown Builder')

    return {
        'is_mev': True,
        'builder': builder_name,
        'builder_pubkey': builder_pubkey
    }


def get_latest_block():
    """Get the latest block number from the database."""
    xatu = get_xatu()
    # Use recent time filter for partition pruning (3x faster)
    result = xatu.execute_query("""
        SELECT max(execution_payload_block_number) as block_number
        FROM default.canonical_beacon_block
        WHERE meta_network_name = 'mainnet'
          AND slot_start_date_time >= now() - INTERVAL 1 HOUR
    """, columns="block_number")
    if result is not None and not result.empty and result['block_number'].iloc[0] is not None:
        return int(result['block_number'].iloc[0])
    return None


def get_block_data(block_number):
    """Fetch block data including transactions and mempool times."""
    xatu = get_xatu()

    # Step 1: Get block info
    block_info = xatu.execute_query(f"""
        SELECT slot, slot_start_date_time
        FROM default.canonical_beacon_block
        WHERE meta_network_name = 'mainnet'
          AND execution_payload_block_number = {block_number}
        LIMIT 1
    """, columns="slot,slot_start_date_time")

    if block_info is None or block_info.empty:
        return None

    slot = int(block_info['slot'].iloc[0])
    block_time = block_info['slot_start_date_time'].iloc[0]

    # Get builder info (pass block_time for partition pruning)
    builder_info = get_builder_info(slot, block_time)

    # Step 2: Get all transactions in the block
    # First try canonical_execution_transaction (has more detailed info)
    txs = xatu.execute_query(f"""
        SELECT transaction_hash, from_address, to_address, value, gas_used, transaction_index
        FROM default.canonical_execution_transaction
        WHERE meta_network_name = 'mainnet'
          AND block_number = {block_number}
        ORDER BY transaction_index
    """, columns="transaction_hash,from_address,to_address,value,gas_used,transaction_index")

    # Fallback to canonical_beacon_block_execution_transaction if no results
    # (this table is indexed by slot and has data for recent blocks)
    if txs is None or txs.empty:
        txs = xatu.execute_query(f"""
            SELECT hash as transaction_hash, `from` as from_address, `to` as to_address,
                   value, gas as gas_used, position as transaction_index
            FROM default.canonical_beacon_block_execution_transaction
            WHERE meta_network_name = 'mainnet'
              AND slot_start_date_time = toDateTime64('{block_time}', 3)
              AND slot = {slot}
            ORDER BY position
        """, columns="transaction_hash,from_address,to_address,value,gas_used,transaction_index")

    if txs is None or txs.empty:
        return {
            'block_number': block_number,
            'slot': slot,
            'block_time': str(block_time),
            'builder': builder_info,
            'transactions': [],
            'stats': {
                'total_txs': 0,
                'public_txs': 0,
                'private_txs': 0,
                'private_percentage': 0,
                'avg_mempool_time_ms': 0,
                'min_mempool_time_ms': 0,
                'max_mempool_time_ms': 0
            }
        }

    # Step 3: Get mempool first seen times in batches
    tx_hashes = txs['transaction_hash'].tolist()
    batch_size = 50
    mempool_results = []

    for i in range(0, len(tx_hashes), batch_size):
        batch = tx_hashes[i:i+batch_size]
        hash_list = ",".join([f"'{h}'" for h in batch])

        q = f"""
        SELECT
            hash AS tx_hash,
            min(event_date_time) AS first_seen_time
        FROM default.mempool_transaction
        WHERE meta_network_name = 'mainnet'
          AND event_date_time >= toDateTime64('{block_time}', 3) - INTERVAL 2 HOUR
          AND event_date_time <= toDateTime64('{block_time}', 3)
          AND hash IN ({hash_list})
        GROUP BY hash
        """

        batch_df = xatu.execute_query(q, columns="tx_hash,first_seen_time")
        if batch_df is not None and not batch_df.empty:
            mempool_results.append(batch_df)

    mempool_df = pd.concat(mempool_results) if mempool_results else pd.DataFrame()

    # Step 4: Merge results
    result = txs.copy()
    result['block_number'] = block_number
    result['slot'] = slot
    result['block_time'] = pd.to_datetime(block_time)

    if not mempool_df.empty:
        result = result.merge(mempool_df, left_on='transaction_hash', right_on='tx_hash', how='left')
        result['first_seen_time'] = pd.to_datetime(result['first_seen_time'])
        result['mempool_time_ms'] = (result['block_time'] - result['first_seen_time']).dt.total_seconds() * 1000
        result = result.drop(columns=['tx_hash'], errors='ignore')
    else:
        result['first_seen_time'] = pd.NaT
        result['mempool_time_ms'] = None

    # Determine if transaction is private (not seen in mempool)
    result['is_private'] = result['first_seen_time'].isna()

    # Calculate stats
    total_txs = len(result)
    public_txs = result['first_seen_time'].notna().sum()
    private_txs = total_txs - public_txs

    public_mempool_times = result.loc[result['first_seen_time'].notna(), 'mempool_time_ms']

    stats = {
        'total_txs': total_txs,
        'public_txs': int(public_txs),
        'private_txs': int(private_txs),
        'private_percentage': round(private_txs / total_txs * 100, 1) if total_txs > 0 else 0,
        'avg_mempool_time_ms': round(public_mempool_times.mean(), 0) if len(public_mempool_times) > 0 else 0,
        'min_mempool_time_ms': round(public_mempool_times.min(), 0) if len(public_mempool_times) > 0 else 0,
        'max_mempool_time_ms': round(public_mempool_times.max(), 0) if len(public_mempool_times) > 0 else 0
    }

    # Convert to list of dicts for template
    transactions = []
    for _, row in result.iterrows():
        tx = {
            'hash': row['transaction_hash'],
            'index': int(row['transaction_index']),
            'from': row['from_address'],
            'to': row['to_address'] if pd.notna(row['to_address']) else 'Contract Creation',
            'value_eth': round(float(row['value']) / 1e18, 6) if pd.notna(row['value']) else 0,
            'gas_used': int(row['gas_used']) if pd.notna(row['gas_used']) else 0,
            'is_private': bool(row['is_private']),
            'mempool_time_ms': round(row['mempool_time_ms'], 0) if pd.notna(row['mempool_time_ms']) else None,
            'first_seen': str(row['first_seen_time']) if pd.notna(row['first_seen_time']) else None
        }
        transactions.append(tx)

    return {
        'block_number': block_number,
        'slot': slot,
        'block_time': str(block_time),
        'builder': builder_info,
        'transactions': transactions,
        'stats': stats
    }


@app.route('/')
def index():
    """Home page with search."""
    latest_block = get_latest_block()
    return render_template('index.html', latest_block=latest_block)


@app.route('/block/<int:block_number>')
def block_view(block_number):
    """Block details page."""
    data = get_block_data(block_number)
    if data is None:
        latest = get_latest_block()
        if latest and block_number > latest:
            message = f"Block {block_number:,} not yet indexed. Latest available: {latest:,}"
        else:
            message = f"Block {block_number:,} not found"
        return render_template('error.html', message=message), 404
    return render_template('block.html', data=data)


@app.route('/api/block/<int:block_number>')
def api_block(block_number):
    """API endpoint for block data."""
    data = get_block_data(block_number)
    if data is None:
        return jsonify({'error': f'Block {block_number} not found'}), 404
    return jsonify(data)


@app.route('/api/latest')
def api_latest():
    """API endpoint for latest block number."""
    latest = get_latest_block()
    if latest is None:
        return jsonify({'error': 'Could not fetch latest block'}), 500
    return jsonify({'block_number': latest})


@app.route('/search')
def search():
    """Handle search form submission."""
    block_number = request.args.get('block', type=int)
    if block_number:
        return block_view(block_number)
    return index()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug)
