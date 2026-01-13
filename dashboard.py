import streamlit as st
import pandas as pd
from trino.dbapi import connect
import pydeck as pdk
from datetime import datetime

# --- 1. CONFIGURATION ---
st.set_page_config(page_title="Morocco Retail Manager", layout="wide", page_icon="🇲🇦")

# --- 2. TRINO CONNECTION ---
TRINO_HOST = "localhost"
TRINO_PORT = 8080
TRINO_USER = "admin"

@st.cache_data(ttl=60)
def run_query(query):
    try:
        conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog="hive", schema="default")
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.error(f"Database Error: {e}")
        return pd.DataFrame()

# --- 3. MASTER DATA & MAPPING ---
STORES = {
    "STORE-CAS-01": {"name": "Marjane Californie", "city": "Casablanca", "lat": 33.5552, "lon": -7.6366},
    "STORE-CAS-02": {"name": "Morocco Mall", "city": "Casablanca", "lat": 33.5956, "lon": -7.6989},
    "STORE-RAB-01": {"name": "Marjane Hay Riad", "city": "Rabat", "lat": 33.9604, "lon": -6.8653},
    "STORE-TNG-01": {"name": "Socco Alto", "city": "Tangier", "lat": 35.7595, "lon": -5.8340},
    "STORE-MAR-01": {"name": "Menara Mall", "city": "Marrakech", "lat": 31.6214, "lon": -8.0163},
    "STORE-AGA-01": {"name": "Carrefour Agadir", "city": "Agadir", "lat": 30.4278, "lon": -9.5981},
    "STORE-FES-01": {"name": "Borj Fez", "city": "Fes", "lat": 34.0456, "lon": -4.9966}
}

# UPDATED: One Local Warehouse per Store (Matches generate_orders_bulk.py)
WAREHOUSES = {
    "WH-STORE-CAS-01": {"name": "Depot: Marjane Californie", "lat": 33.5552, "lon": -7.6366},
    "WH-STORE-CAS-02": {"name": "Depot: Morocco Mall", "lat": 33.5956, "lon": -7.6989},
    "WH-STORE-RAB-01": {"name": "Depot: Marjane Hay Riad", "lat": 33.9604, "lon": -6.8653},
    "WH-STORE-TNG-01": {"name": "Depot: Socco Alto", "lat": 35.7595, "lon": -5.8340},
    "WH-STORE-MAR-01": {"name": "Depot: Menara Mall", "lat": 31.6214, "lon": -8.0163},
    "WH-STORE-AGA-01": {"name": "Depot: Carrefour Agadir", "lat": 30.4278, "lon": -9.5981},
    "WH-STORE-FES-01": {"name": "Depot: Borj Fez", "lat": 34.0456, "lon": -4.9966}
}

PRODUCTS = {
    "PRD-001": "Sidi Ali 1.5L", "PRD-002": "Oulmes 1L", "PRD-003": "Couscous Dari 1kg",
    "PRD-004": "Thé Sultan Vert", "PRD-005": "Aicha Confiture Fraise", "PRD-006": "Lait Centrale Danone",
    "PRD-007": "Raibi Jamila", "PRD-008": "Huile d'Olive Al Horra", "PRD-009": "Fromage La Vache Qui Rit",
    "PRD-010": "Merendina", "PRD-011": "Pasta Tria", "PRD-012": "Sardines Titus",
    "PRD-013": "Coca-Cola 1L", "PRD-014": "Atay Sebou", "PRD-015": "Eau Ciel 5L"
}

# UPDATED: 1-to-1 Mapping (Each store is supplied by its own depot)
STORE_SUPPLY_CHAIN = {
    "STORE-CAS-01": "WH-STORE-CAS-01",
    "STORE-CAS-02": "WH-STORE-CAS-02",
    "STORE-RAB-01": "WH-STORE-RAB-01",
    "STORE-TNG-01": "WH-STORE-TNG-01",
    "STORE-MAR-01": "WH-STORE-MAR-01",
    "STORE-AGA-01": "WH-STORE-AGA-01",
    "STORE-FES-01": "WH-STORE-FES-01"
}

# --- 4. SESSION STATE ---
if 'selected_store' not in st.session_state:
    st.session_state.selected_store = None
if 'selected_warehouse' not in st.session_state:
    st.session_state.selected_warehouse = None

# --- 5. DATA FETCHING ---
today = datetime.now()
date_str = st.sidebar.date_input("Select Data Date", today).strftime("%Y-%m-%d")

# Fetch Sales
sales_query = f"""
    SELECT store_id, sku, SUM(t.quantity) as qty_sold 
    FROM raw_orders 
    CROSS JOIN UNNEST(items) AS t(sku, quantity, unit_price)
    WHERE dt = '{date_str}'
    GROUP BY store_id, sku
"""
sales_df = run_query(sales_query)

# Fetch Inventory
inv_query = f"""
    SELECT warehouse_id, sku, SUM(CAST(available_qty AS INT)) as qty 
    FROM raw_inventory 
    WHERE dt = '{date_str}'
    GROUP BY warehouse_id, sku
"""
inv_df = run_query(inv_query)

# Add Product Names
if not sales_df.empty:
    sales_df['product_name'] = sales_df['sku'].map(PRODUCTS)
if not inv_df.empty:
    inv_df['product_name'] = inv_df['sku'].map(PRODUCTS)

# --- 6. LAYOUT ---
st.title("🇲🇦 Morocco Retail Manager")
st.markdown(f"**Data Date:** {date_str}")

tab_stores, tab_inv, tab_prod = st.tabs(["🏪 Stores", "🏭 Inventory", "🛍️ Products"])

# === TAB 1: STORES ===
with tab_stores:
    if st.session_state.selected_store is None:
        st.subheader("Select a Store")
        cols = st.columns(3)
        for idx, (sid, info) in enumerate(STORES.items()):
            with cols[idx % 3]:
                if st.button(f"📍 {info['name']}\n({info['city']})", key=f"btn_store_{sid}", use_container_width=True):
                    st.session_state.selected_store = sid
                    st.rerun()
    else:
        sid = st.session_state.selected_store
        info = STORES[sid]
        supply_wh_id = STORE_SUPPLY_CHAIN.get(sid, "WH-CAS-01")
        supply_wh_name = WAREHOUSES[supply_wh_id]['name']

        c1, c2 = st.columns([8, 2])
        c1.subheader(f"🛒 {info['name']}")
        c1.caption(f"📍 Location: {info['city']} | 🚛 Supplied by: {supply_wh_name}")
        
        if c2.button("← Back to List"):
            st.session_state.selected_store = None
            st.rerun()
            
        st.divider()
        
        # 1. Get Sales for this store
        store_sales = sales_df[sales_df['store_id'] == sid].copy()
        
        # 2. Get Inventory for the supplying warehouse
        wh_stock = inv_df[inv_df['warehouse_id'] == supply_wh_id][['sku', 'qty']].rename(columns={'qty': 'stock_available'})
        
        if not store_sales.empty:
            # 3. MERGE Sales + Stock
            merged_view = pd.merge(store_sales, wh_stock, on='sku', how='left')
            merged_view['stock_available'] = merged_view['stock_available'].fillna(0).astype(int)

            st.metric("Total Items Sold Today", f"{store_sales['qty_sold'].sum():,}")
            
            
            # Formatting for display
            display_df = merged_view[['product_name', 'qty_sold', 'stock_available']].sort_values(by='qty_sold', ascending=False)
            
            st.dataframe(
                display_df,
                use_container_width=True,
                column_config={
                    "product_name": "Product", 
                    "qty_sold": st.column_config.NumberColumn("Units Sold", format="%d"),
                    "stock_available": st.column_config.ProgressColumn(
                        "Stock at Warehouse", 
                        format="%d", 
                        min_value=0, 
                        max_value=int(display_df['stock_available'].max() * 1.1)
                    )
                }
            )
        else:
            st.warning("No sales data available for this store today.")

# === TAB 2: INVENTORY ===
with tab_inv:
    if st.session_state.selected_warehouse is None:
        st.subheader("Select a Warehouse")
        cols = st.columns(2)
        for idx, (wid, info) in enumerate(WAREHOUSES.items()):
            with cols[idx % 2]:
                if st.button(f"🏭 {info['name']}", key=f"btn_wh_{wid}", use_container_width=True):
                    st.session_state.selected_warehouse = wid
                    st.rerun()
    else:
        wid = st.session_state.selected_warehouse
        info = WAREHOUSES[wid]
        c1, c2 = st.columns([8, 2])
        c1.subheader(f"📦 Inventory: {info['name']}")
        if c2.button("← Back to Warehouses"):
            st.session_state.selected_warehouse = None
            st.rerun()
        st.divider()
        wh_stock = inv_df[inv_df['warehouse_id'] == wid]
        if not wh_stock.empty:
            st.metric("Total Stock Units", f"{wh_stock['qty'].sum():,}")
            st.dataframe(wh_stock[['product_name', 'qty']].sort_values(by='qty', ascending=False), use_container_width=True)
        else:
            st.warning("Warehouse appears empty.")

# === TAB 3: PRODUCTS ===
with tab_prod:
    st.subheader("🛍️ National Product Catalog")
    if not inv_df.empty:
        prod_agg = inv_df.groupby(['sku', 'product_name'])['qty'].sum().reset_index()
        st.dataframe(prod_agg.rename(columns={"product_name": "Name", "qty": "Total National Quantity", "sku": "SKU"}), use_container_width=True, hide_index=True)
    else:
        st.info("No product data found.")

# --- 7. MAP ---
st.divider()
with st.expander("🌍 CLICK TO OPEN FULL SCREEN MAP", expanded=False):
    map_data = []
    for sid, i in STORES.items():
        map_data.append({"name": i["name"], "lat": i["lat"], "lon": i["lon"], "type": "Store", "color": [0, 128, 255, 200], "size": 100})
    for wid, i in WAREHOUSES.items():
        map_data.append({"name": i["name"], "lat": i["lat"], "lon": i["lon"], "type": "Warehouse", "color": [255, 0, 0, 200], "size": 150})
    map_df = pd.DataFrame(map_data)
    st.pydeck_chart(pdk.Deck(map_style=None, initial_view_state=pdk.ViewState(latitude=33.5, longitude=-7.5, zoom=6, pitch=0), layers=[pdk.Layer("ScatterplotLayer", map_df, get_position='[lon, lat]', get_color='color', get_radius='size', radius_scale=100, pickable=True)], tooltip={"text": "{name} ({type})"}))