"""
ASAP Sales Dashboard — generate.py
Shaker Ventures Intelligence Platform

Reads:
  data/vcust.csv
  data/cust_status.csv
  data/se_lookup.xlsx
  data/pipeline.csv          (saved-as-CSV from pipeline.xlsx)
  data/sales_snapshot.csv    (first run — converts to parquet)
  data/sales_snapshot.parquet (subsequent runs)
  data/sales_delta_*.csv     (monthly additions, auto-merged)

Outputs:
  docs/data/asap_accounts.json
  docs/data/asap_se.json
  docs/data/asap_teams.json
  docs/data/asap_pipeline.json
  docs/data/asap_products.json
  docs/asap.html
"""

import os, sys, json, glob, shutil, subprocess
import numpy as np
import pandas as pd
from rapidfuzz import fuzz

# ── Constants ────────────────────────────────────────────────────────────────

VALID_TEAMS = {
    'East Projects', 'West Projects', 'Central Projects',
    'ACS - CR', 'ACS - ER', 'ACS - WR', 'Direct Channel'
}

# Pipeline team names → normalised VALID_TEAMS names
PIPELINE_TEAM_MAP = {
    'ACS - Central':          'ACS - CR',
    'ACS - CR':               'ACS - CR',
    'ACS - East':             'ACS - ER',
    'ACS - ER':               'ACS - ER',
    'ACS - West':             'ACS - WR',
    'ACS - WR':               'ACS - WR',
    'Central Projects Team':  'Central Projects',
    'Central Projects':       'Central Projects',
    'East':                   'East Projects',
    'East Projects':          'East Projects',
    'West':                   'West Projects',
    'West Projects':          'West Projects',
    'Direct Sales Channel':   'Direct Channel',
    'Direct Channel':         'Direct Channel',
}

PIPELINE_GROUPS = {'CAC Ducted', 'CAC Non-Ducted', 'RAC', 'Applied'}

HOT_PROBS   = {'50% - PJT-2ND QUO', '70% - PJT-FINAL QUO'}
AWARD_PROBS = {'90% - PO ONLY', '100% - PO & PAYMENT'}
VALID_DOC   = 'VALID/NOT DUPLICATE'
VALID_STATUS = {'WON', 'NO DECISION/IN PROGRESS'}

SE_ALIAS = {
    'Ahmed Assem':                    'Ahmed Hassanien',
    'Anas Mohammad Ahmad Ayyadat':    'Anas Ayyadat',
    'Yazan Ahmad Abdellatif Abusaa':  'Yazan Abu Saa',
}

CONTRACTOR_SIZE_TIERS = [
    ('A+', 20_000_000),
    ('A',  10_000_000),
    ('B',   5_000_000),
    ('C',   1_000_000),
    ('D',           0),
]

DATA_DIR = 'data'
DOCS_DIR = os.path.join('docs', 'data')

# ── Utilities ────────────────────────────────────────────────────────────────

def safe_val(v):
    if isinstance(v, (np.integer,)):  return int(v)
    if isinstance(v, (np.floating,)): return None if np.isnan(v) else float(v)
    if isinstance(v, (np.bool_,)):    return bool(v)
    if isinstance(v, float) and np.isnan(v): return None
    if pd.isna(v) if not isinstance(v, (list, dict)) else False: return None
    return v

def apply_alias(s):
    if not isinstance(s, str): return s
    s = s.strip()
    return SE_ALIAS.get(s, s)

def norm_str(s):
    if not isinstance(s, str): return ''
    return s.strip().lower()

def fmt_sar(v):
    if v is None or (isinstance(v, float) and np.isnan(v)): return None
    return round(float(v), 2)

def tier_for_value(v):
    if v is None or v == 0: return None
    for tier, threshold in CONTRACTOR_SIZE_TIERS:
        if v >= threshold:
            return tier
    return 'D'

# ── SE Lookup ────────────────────────────────────────────────────────────────

def load_se_lookup():
    path = os.path.join(DATA_DIR, 'se_lookup.xlsx')
    df = pd.read_excel(path, sheet_name='New version (2)',
                       header=None, usecols=[4, 5])
    df = df.iloc[2:].copy()
    df.columns = ['salesman_name', 'team_new']
    df = df.dropna(subset=['salesman_name'])
    df['salesman_name'] = df['salesman_name'].astype(str).str.strip().apply(apply_alias)
    df['team_new']      = df['team_new'].astype(str).str.strip()
    return dict(zip(df['salesman_name'], df['team_new']))

# ── Customer Number Bridge ───────────────────────────────────────────────────

def load_cust_bridge():
    """
    Returns dict: old_customer_number -> 118_customer_number
    Three-tier resolution:
      1. Exact name match in cust_status
      2. Fuzzy name match (score >= 50 = strong, 30-50 = likely)
      3. Unresolved — kept as-is, flagged
    """
    path = os.path.join(DATA_DIR, 'cust_status.csv')
    cs = pd.read_csv(path)
    cs['Customer'] = cs['Customer'].astype(str).str.strip()
    cs['name_norm'] = cs['Customer Name'].astype(str).str.strip().str.lower()

    old_rows = cs[~cs['Customer'].str.startswith('118')].copy()
    new_rows = cs[cs['Customer'].str.startswith('118')].copy()

    # Index new rows by normalised name
    new_by_name = {}
    for _, r in new_rows.iterrows():
        new_by_name[r['name_norm']] = r['Customer']

    bridge = {}       # old_num -> {'new_num': ..., 'method': ...}
    unresolved = []

    for _, r in old_rows.iterrows():
        old_num  = r['Customer']
        name     = r['name_norm']

        # Tier 1: exact name match
        if name in new_by_name:
            bridge[old_num] = {'new_num': new_by_name[name], 'method': 'exact'}
            continue

        # Tier 2: fuzzy match against all new-series names
        best_score, best_name = 0, None
        for new_name in new_by_name:
            score = fuzz.ratio(name, new_name)
            if score > best_score:
                best_score, best_name = score, new_name

        if best_score >= 50:
            bridge[old_num] = {
                'new_num': new_by_name[best_name],
                'method': 'fuzzy_strong' if best_score >= 70 else 'fuzzy_likely'
            }
        else:
            unresolved.append(old_num)

    print(f"  Customer bridge: {len(bridge)} resolved "
          f"({sum(1 for v in bridge.values() if v['method']=='exact')} exact, "
          f"{sum(1 for v in bridge.values() if 'fuzzy' in v['method'])} fuzzy), "
          f"{len(unresolved)} unresolved")

    return bridge

# ── vCust ────────────────────────────────────────────────────────────────────

def load_vcust():
    path = os.path.join(DATA_DIR, 'vcust.csv')
    vc = pd.read_csv(path, dtype={'Customer': str})
    vc['Customer']              = vc['Customer'].astype(str).str.strip()
    vc['Sales Employee Name']   = vc['Sales Employee Name'].apply(apply_alias)
    vc['OLD SE']                = vc['OLD SE'].apply(
        lambda x: apply_alias(x) if pd.notna(x) else x)
    vc['name_norm']             = vc['Name 1'].astype(str).str.strip().str.lower()

    # Transfer type classification
    def transfer_type(row):
        old_team  = str(row['OLD TEAM']).strip() if pd.notna(row['OLD TEAM']) else ''
        curr_team = str(row['Team/Region']).strip() if pd.notna(row['Team/Region']) else ''
        old_se    = str(row['OLD SE']).strip() if pd.notna(row['OLD SE']) else ''
        curr_se   = str(row['Sales Employee Name']).strip() if pd.notna(row['Sales Employee Name']) else ''
        if not old_se:
            return 'new'
        if old_team and curr_team and old_team not in ('0', '') and old_team != curr_team:
            return 'inter'
        if old_se != curr_se:
            return 'intra'
        return 'same'

    vc['transfer_type'] = vc.apply(transfer_type, axis=1)
    print(f"  vCust: {len(vc)} accounts loaded")
    return vc

# ── Sales Data ───────────────────────────────────────────────────────────────

def load_sales(bridge):
    snap_parquet = os.path.join(DATA_DIR, 'sales_snapshot.parquet')
    snap_csv     = os.path.join(DATA_DIR, 'sales_snapshot.csv')
    delta_files  = sorted(glob.glob(os.path.join(DATA_DIR, 'sales_delta_*.csv')))

    SALES_COLS = [
        'Customer', 'Customer Name',
        'Sales Employee Name (Transaction)', 'Team (Sales Transaction)',
        'Sales Employee Name (Master)', 'Team (Owner)',
        'Invoice Date', 'year',
        'Quantity', 'Value',
        'Group', 'Category', 'Class',
        'Quotation No', 'Invoice Number',
    ]

    def read_sales_csv(path, label=''):
        print(f"  Reading {label or path}...")
        df = pd.read_csv(path, dtype={'Customer': str}, low_memory=False)
        df['Customer'] = df['Customer'].astype(str).str.strip()

        # Keep only available cols that exist
        keep = [c for c in SALES_COLS if c in df.columns]
        df = df[keep].copy()

        # Apply SE aliases
        for col in ['Sales Employee Name (Transaction)', 'Sales Employee Name (Master)']:
            if col in df.columns:
                df[col] = df[col].apply(apply_alias)

        # Filter to valid teams only
        team_col = 'Team (Sales Transaction)'
        if team_col in df.columns:
            df = df[df[team_col].isin(VALID_TEAMS)].copy()

        # Force Value and Quantity to numeric (CSV reads them as string)
        for num_col in ['Value', 'Quantity']:
            if num_col in df.columns:
                df[num_col] = pd.to_numeric(df[num_col], errors='coerce').fillna(0)

        # Parse Invoice Date
        if 'Invoice Date' in df.columns:
            df['Invoice Date'] = pd.to_datetime(df['Invoice Date'],
                                                dayfirst=False, errors='coerce')
            df['year']  = df['Invoice Date'].dt.year
            df['month'] = df['Invoice Date'].dt.month

        # Bridge old customer numbers
        def resolve_cust(cnum):
            if str(cnum).startswith('118'):
                return cnum, False
            if cnum in bridge:
                return bridge[cnum]['new_num'], False
            return cnum, True  # unresolved

        df[['Customer', 'unresolved_cust']] = df['Customer'].apply(
            lambda x: pd.Series(resolve_cust(x)))

        print(f"    → {len(df)} rows after team filter")
        return df

    # First run: convert CSV snapshot to parquet
    if not os.path.exists(snap_parquet):
        if not os.path.exists(snap_csv):
            sys.exit("ERROR: No sales data found. Place sales_snapshot.csv in data/")
        df = read_sales_csv(snap_csv, 'sales_snapshot.csv (first run)')
        df.to_parquet(snap_parquet, index=False)
        print(f"  Saved sales_snapshot.parquet ({len(df)} rows)")
        archive = snap_csv.replace('.csv', '_archived.csv')
        os.rename(snap_csv, archive)
        print(f"  Archived CSV → {archive}")
    else:
        print("  Loading sales_snapshot.parquet...")
        df = pd.read_parquet(snap_parquet)
        # Ensure Value is numeric (older snapshots may have string)
        for num_col in ['Value', 'Quantity']:
            if num_col in df.columns:
                df[num_col] = pd.to_numeric(df[num_col], errors='coerce').fillna(0)
        print(f"    → {len(df)} rows")

    # Process any delta files
    for delta_path in delta_files:
        fname = os.path.basename(delta_path)
        delta = read_sales_csv(delta_path, fname)
        df = pd.concat([df, delta], ignore_index=True)
        df.to_parquet(snap_parquet, index=False)
        archive = delta_path.replace('.csv', '_merged.csv')
        os.rename(delta_path, archive)
        print(f"  Delta merged + archived: {fname} → {len(delta)} rows added")

    print(f"  Sales total: {len(df)} rows, years: {sorted(df['year'].dropna().unique().astype(int).tolist())}")
    return df

# ── Pipeline ─────────────────────────────────────────────────────────────────

def load_pipeline(bridge):
    path = os.path.join(DATA_DIR, 'pipeline.csv')
    print(f"  Loading pipeline.csv...")

    PIPE_COLS = [
        'Customer', 'Customer Name', 'Quotation No.',
        'Project Name', 'Sales Employee Name', 'Team',
        'Valid from', 'Expected Award Date',
        'Q-Gross Value', 'Back-log Val', 'Back-log Qty',
        'Document Status', 'Quote Status', 'Quote Probability',
        'Group',
    ]

    df = pd.read_csv(path, dtype={'Customer': str}, low_memory=False)
    df['Customer'] = df['Customer'].astype(str).str.strip()

    # Strip whitespace from all column names (CSV export often adds spaces)
    df.columns = df.columns.str.strip()
    keep = [c for c in PIPE_COLS if c in df.columns]
    df = df[keep].copy()

    if 'Sales Employee Name' in df.columns:
        df['Sales Employee Name'] = df['Sales Employee Name'].apply(apply_alias)

    # Normalise pipeline team names to match VALID_TEAMS
    if 'Team' in df.columns:
        df['Team'] = df['Team'].map(PIPELINE_TEAM_MAP).fillna(df['Team'])
        df = df[df['Team'].isin(VALID_TEAMS)].copy()
        print(f"  Pipeline team distribution after normalisation:")
        print(df['Team'].value_counts().to_string())

    # Force numeric value columns
    for num_col in ['Q-Gross Value', 'Back-log Val', 'Back-log Qty']:
        if num_col in df.columns:
            df[num_col] = pd.to_numeric(df[num_col], errors='coerce').fillna(0)

    # Parse dates
    for dcol in ['Valid from', 'Expected Award Date']:
        if dcol in df.columns:
            df[dcol] = pd.to_datetime(df[dcol], dayfirst=False, errors='coerce')

    # Bridge old customer numbers
    def resolve_cust(cnum):
        if str(cnum).startswith('118'):
            return cnum
        return bridge.get(cnum, {}).get('new_num', cnum)

    df['Customer'] = df['Customer'].apply(resolve_cust)

    # Apply pipeline filters
    valid_mask = (
        (df['Document Status'] == VALID_DOC) &
        (df['Quote Status'].isin(VALID_STATUS))
    )
    df = df[valid_mask].copy()

    # Classify each row
    df['is_hot']    = df['Quote Probability'].isin(HOT_PROBS)
    df['is_award']  = df['Quote Probability'].isin(AWARD_PROBS)
    df['in_groups'] = df['Group'].isin(PIPELINE_GROUPS)

    print(f"  Pipeline: {len(df)} valid rows after filters")
    return df

# ── Computed Fields ───────────────────────────────────────────────────────────

def compute_sales_by_year(sales_df, customer):
    rows = sales_df[sales_df['Customer'] == customer]
    result = {}
    for yr in [2022, 2023, 2024, 2025, 2026]:
        val = rows[rows['year'] == yr]['Value'].sum()
        result[yr] = fmt_sar(val) if val > 0 else None
    return result

def compute_contractor_size(sales_by_year):
    values = [v for yr, v in sales_by_year.items() if v and yr in [2022,2023,2024,2025]]
    if not values:
        return None, None

    peak_val  = max(values)
    peak_tier = tier_for_value(peak_val)

    # Trend: check consecutive decline
    yr_vals = [(yr, sales_by_year.get(yr)) for yr in [2022,2023,2024,2025]
               if sales_by_year.get(yr)]
    if len(yr_vals) < 2:
        return peak_tier, None

    # Count consecutive declining years from most recent
    yr_vals_sorted = sorted(yr_vals, key=lambda x: x[0])
    decline_streak = 0
    for i in range(len(yr_vals_sorted)-1, 0, -1):
        if yr_vals_sorted[i][1] < yr_vals_sorted[i-1][1]:
            decline_streak += 1
        else:
            break

    # Recent avg
    recent_vals = [v for yr, v in yr_vals_sorted if yr in [2024, 2025] and v]
    if not recent_vals:
        return peak_tier, None
    recent_avg  = sum(recent_vals) / len(recent_vals)
    recent_tier = tier_for_value(recent_avg)

    tier_order = ['A+', 'A', 'B', 'C', 'D']
    peak_idx   = tier_order.index(peak_tier) if peak_tier in tier_order else 4
    recent_idx = tier_order.index(recent_tier) if recent_tier in tier_order else 4

    # Trend arrow
    trend = None
    if decline_streak >= 2 and recent_idx > peak_idx + 1:
        trend = 'down'
    elif recent_avg > max(v for _, v in yr_vals_sorted[:-1]):
        trend = 'up'

    return peak_tier, trend

def compute_customer_status(sales_by_year, hot_pipeline, award_2025, award_2026, created_on):
    s22 = sales_by_year.get(2022)
    s23 = sales_by_year.get(2023)
    s24 = sales_by_year.get(2024)
    s25 = sales_by_year.get(2025)
    s26 = sales_by_year.get(2026)

    has_pre25 = bool(s22 or s23 or s24)
    has_25_26 = bool(s25 or s26)

    # Parse creation year
    try:
        create_yr = pd.to_datetime(created_on).year
    except:
        create_yr = None

    if not has_pre25 and not has_25_26:
        base_status = 'Inactive'
    elif has_pre25 and not has_25_26:
        base_status = 'Inactive'
    elif s24 and s25:
        base_status = 'Active'
    elif not s24 and s25 and s23:
        base_status = 'Active'
    elif not s23 and not s24 and has_25_26:
        if create_yr and create_yr >= 2025:
            base_status = 'New'
        else:
            base_status = 'Reactivated'
    elif s23 and not s24 and s25:
        base_status = 'Reactivated'
    else:
        base_status = 'Unknown'

    # Truly inactive override
    if base_status == 'Inactive':
        if hot_pipeline or award_2025 or award_2026:
            return 'Active/Potential'

    return base_status

def compute_product_shift(sales_df, customer):
    rows = sales_df[sales_df['Customer'] == customer]
    if 'Group' not in rows.columns:
        return None, None

    def top_group(year):
        yr_rows = rows[rows['year'] == year]
        if yr_rows.empty:
            return None
        g = yr_rows.groupby('Group')['Value'].sum()
        if g.empty:
            return None
        return g.idxmax()

    g24 = top_group(2024)
    g25 = top_group(2025)
    return g24, g25

def compute_se_history(sales_df, customer, current_owner):
    rows = sales_df[sales_df['Customer'] == customer].copy()
    if rows.empty:
        return []

    se_col = 'Sales Employee Name (Transaction)'
    if se_col not in rows.columns:
        return []

    total_val = rows['Value'].sum()
    se_groups = rows.groupby(se_col)

    history = []
    for se_name, grp in se_groups:
        if not isinstance(se_name, str) or not se_name.strip():
            continue
        yr_sales = {}
        for yr in [2022, 2023, 2024, 2025, 2026]:
            val = grp[grp['year'] == yr]['Value'].sum()
            yr_sales[str(yr)] = fmt_sar(val) if val > 0 else None

        contrib = round((grp['Value'].sum() / total_val * 100), 1) if total_val > 0 else 0
        is_owner = (norm_str(se_name) == norm_str(current_owner))

        history.append({
            'se_name':   se_name,
            'is_owner':  is_owner,
            'yr_sales':  yr_sales,
            'contrib_pct': contrib,
        })

    history.sort(key=lambda x: sum(v for v in x['yr_sales'].values() if v), reverse=True)
    return history

def compute_projects(pipe_df, customer):
    if pipe_df is None or pipe_df.empty or 'Customer' not in pipe_df.columns:
        return []
    rows = pipe_df[pipe_df['Customer'] == customer].copy()
    if rows.empty:
        return []

    projects = []
    for _, r in rows.iterrows():
        projects.append({
            'project_name':  safe_val(r.get('Project Name')),
            'quotation_no':  safe_val(r.get('Quotation No.')),
            'group':         safe_val(r.get('Group')),
            'q_gross_value': fmt_sar(r.get('Q-Gross Value')),
            'backlog_val':   fmt_sar(r.get('Back-log Val')),
            'is_hot':        bool(r.get('is_hot', False)),
            'is_award':      bool(r.get('is_award', False)),
            'expected_award': r['Expected Award Date'].strftime('%Y-%m') if pd.notna(r.get('Expected Award Date')) else None,
        })
    return projects

def compute_se_verdict(se_history):
    if len(se_history) < 2:
        return None

    owner = next((s for s in se_history if s['is_owner']), None)
    others = [s for s in se_history if not s['is_owner']]
    if not owner or not others:
        return None

    best_other = others[0]
    owner_total  = sum(v for v in owner['yr_sales'].values() if v) or 0
    other_total  = sum(v for v in best_other['yr_sales'].values() if v) or 0

    if other_total > owner_total * 1.5:
        return {
            'type': 'warn',
            'text': f"Previous SE ({best_other['se_name']}) outperformed current owner on invoiced sales. Review account coverage."
        }
    elif owner_total > other_total * 1.2:
        return {
            'type': 'good',
            'text': f"Current owner ({owner['se_name']}) performing well on this account vs historical SEs."
        }
    return {
        'type': 'info',
        'text': f"Multiple SEs transacted on this account. Performance broadly comparable."
    }

# ── Monthly Sales (for SE & Team charts) ─────────────────────────────────────

def compute_monthly_sales(sales_df, group_col, group_val, years=[2024, 2025]):
    rows = sales_df[sales_df[group_col] == group_val] if group_val else sales_df
    result = {}
    for yr in years:
        yr_rows = rows[rows['year'] == yr]
        monthly = {}
        for m in range(1, 13):
            val = yr_rows[yr_rows['month'] == m]['Value'].sum()
            monthly[m] = fmt_sar(val) if val > 0 else None
        result[yr] = monthly
    return result

# ── Main Data Assembly ────────────────────────────────────────────────────────

def assemble_accounts(vc, sales, pipe):
    print("  Assembling account data...")
    accounts = []

    # Pre-index pipeline by customer for speed
    pipe_by_cust = {k: v.reset_index(drop=True) for k, v in pipe.groupby('Customer')}

    for idx, row in vc.iterrows():
        cust_num  = row['Customer']
        cust_name = row['Name 1']
        curr_se   = str(row['Sales Employee Name']).strip() if pd.notna(row['Sales Employee Name']) else ''
        curr_team = str(row['Team/Region']).strip() if pd.notna(row['Team/Region']) else ''
        old_se    = str(row['OLD SE']).strip() if pd.notna(row['OLD SE']) else ''
        old_team  = str(row['OLD TEAM']).strip() if pd.notna(row['OLD TEAM']) else ''
        desc      = str(row['Description']).strip() if pd.notna(row['Description']) else ''
        created   = row['Created On']
        transfer  = row['transfer_type']

        # Sales by year
        sby = compute_sales_by_year(sales, cust_num)

        # Pipeline aggregates for this customer
        cust_pipe = pipe_by_cust.get(cust_num, pd.DataFrame())
        hot_pipe  = fmt_sar(cust_pipe[cust_pipe['is_hot'] & cust_pipe['in_groups']]['Q-Gross Value'].sum()) if not cust_pipe.empty else None
        award_val = fmt_sar(cust_pipe[cust_pipe['is_award'] & cust_pipe['in_groups']]['Q-Gross Value'].sum()) if not cust_pipe.empty else None
        backlog   = fmt_sar(cust_pipe['Back-log Val'].sum()) if not cust_pipe.empty else None

        # Award split by year for status calc
        if not cust_pipe.empty and 'Expected Award Date' in cust_pipe.columns:
            award_25 = cust_pipe[
                cust_pipe['is_award'] & cust_pipe['in_groups'] &
                (cust_pipe['Expected Award Date'].dt.year == 2025)
            ]['Q-Gross Value'].sum()
            award_26 = cust_pipe[
                cust_pipe['is_award'] & cust_pipe['in_groups'] &
                (cust_pipe['Expected Award Date'].dt.year == 2026)
            ]['Q-Gross Value'].sum()
        else:
            award_25 = award_26 = 0

        # Computed fields
        size_tier, size_trend = compute_contractor_size(sby)
        cust_status = compute_customer_status(sby, hot_pipe, award_25, award_26, created)
        grp_24, grp_25 = compute_product_shift(sales, cust_num)
        se_history  = compute_se_history(sales, cust_num, curr_se)
        projects    = compute_projects(cust_pipe if isinstance(cust_pipe, pd.DataFrame) else pd.DataFrame(), cust_num)
        verdict     = compute_se_verdict(se_history)

        # YoY
        yoy_25v24 = None
        if sby.get(2024) and sby.get(2025):
            yoy_25v24 = round((sby[2025] - sby[2024]) / sby[2024] * 100, 1)

        accounts.append({
            'customer':       cust_num,
            'name':           cust_name,
            'description':    desc,
            'curr_se':        curr_se,
            'curr_team':      curr_team,
            'old_se':         old_se,
            'old_team':       old_team,
            'transfer_type':  transfer,
            'rep_changed':    (old_se != curr_se and bool(old_se)),
            'created_on':     str(created) if pd.notna(created) else None,
            'sales':          sby,
            'yoy_25v24':      yoy_25v24,
            'hot_pipeline':   hot_pipe,
            'award':          award_val,
            'backlog':        backlog,
            'size_tier':      size_tier,
            'size_trend':     size_trend,
            'cust_status':    cust_status,
            'group_2024':     grp_24,
            'group_2025':     grp_25,
            'group_shifted':  bool(grp_24 and grp_25 and grp_24 != grp_25),
            'se_history':     se_history,
            'projects':       projects,
            'verdict':        verdict,
        })

        if (idx + 1) % 200 == 0:
            print(f"    {idx+1}/{len(vc)} accounts processed...")

    print(f"  Accounts assembled: {len(accounts)}")
    return accounts

def assemble_se_data(accounts, sales):
    print("  Assembling SE data...")
    se_map = {}

    for acc in accounts:
        se = acc['curr_se']
        team = acc['curr_team']
        if not se:
            continue
        if se not in se_map:
            se_map[se] = {
                'se_name':   se,
                'team':      team,
                'accounts':  [],
                'sales':     {yr: 0 for yr in [2022,2023,2024,2025,2026]},
                'hot_pipeline': 0,
                'award':     0,
                'backlog':   0,
                'size_counts': {},
            }
        entry = se_map[se]
        entry['accounts'].append(acc['customer'])
        for yr in [2022, 2023, 2024, 2025, 2026]:
            entry['sales'][yr] += acc['sales'].get(yr) or 0
        entry['hot_pipeline'] += acc['hot_pipeline'] or 0
        entry['award']        += acc['award'] or 0
        entry['backlog']      += acc['backlog'] or 0
        tier = acc['size_tier']
        if tier:
            entry['size_counts'][tier] = entry['size_counts'].get(tier, 0) + 1

    se_list = []
    for se, d in se_map.items():
        s24 = d['sales'].get(2024) or 0
        s25 = d['sales'].get(2025) or 0
        yoy = round((s25 - s24) / s24 * 100, 1) if s24 > 0 else None

        # Monthly sales for charts
        se_col = 'Sales Employee Name (Transaction)'
        monthly = {}
        if se_col in sales.columns:
            monthly = compute_monthly_sales(sales, se_col, se)

        se_list.append({
            'se_name':      se,
            'team':         d['team'],
            'account_count': len(d['accounts']),
            'accounts':     d['accounts'],
            'sales':        {str(k): fmt_sar(v) for k, v in d['sales'].items()},
            'yoy_25v24':    yoy,
            'hot_pipeline': fmt_sar(d['hot_pipeline']),
            'award':        fmt_sar(d['award']),
            'backlog':      fmt_sar(d['backlog']),
            'size_counts':  d['size_counts'],
            'monthly_sales': {str(k): v for k, v in monthly.items()},
        })

    se_list.sort(key=lambda x: x['sales'].get('2025') or 0, reverse=True)
    print(f"  SEs assembled: {len(se_list)}")
    return se_list

def assemble_team_data(accounts, sales):
    print("  Assembling team data...")
    team_map = {}

    for acc in accounts:
        team = acc['curr_team']
        if not team:
            continue
        if team not in team_map:
            team_map[team] = {
                'team': team,
                'accounts': [],
                'sales': {yr: 0 for yr in [2022,2023,2024,2025,2026]},
                'hot_pipeline': 0,
                'award': 0,
                'backlog': 0,
                'size_counts': {},
                'status_counts': {},
                'transfer_counts': {},
            }
        d = team_map[team]
        d['accounts'].append(acc['customer'])
        for yr in [2022,2023,2024,2025,2026]:
            d['sales'][yr] += acc['sales'].get(yr) or 0
        d['hot_pipeline'] += acc['hot_pipeline'] or 0
        d['award']        += acc['award'] or 0
        d['backlog']      += acc['backlog'] or 0
        tier = acc['size_tier']
        if tier:
            d['size_counts'][tier] = d['size_counts'].get(tier, 0) + 1
        st = acc['cust_status']
        if st:
            d['status_counts'][st] = d['status_counts'].get(st, 0) + 1
        tt = acc['transfer_type']
        if tt:
            d['transfer_counts'][tt] = d['transfer_counts'].get(tt, 0) + 1

    team_list = []
    for team, d in team_map.items():
        s24 = d['sales'].get(2024) or 0
        s25 = d['sales'].get(2025) or 0
        yoy = round((s25 - s24) / s24 * 100, 1) if s24 > 0 else None

        monthly = compute_monthly_sales(sales, 'Team (Sales Transaction)', team)

        # Product group breakdown
        team_sales = sales[sales['Team (Sales Transaction)'] == team] if 'Team (Sales Transaction)' in sales.columns else pd.DataFrame()
        group_data = {}
        if not team_sales.empty and 'Group' in team_sales.columns:
            for yr in [2023, 2024, 2025]:
                g = team_sales[team_sales['year'] == yr].groupby('Group')['Value'].sum()
                group_data[str(yr)] = {k: fmt_sar(v) for k, v in g.items()}

        team_list.append({
            'team':           team,
            'account_count':  len(d['accounts']),
            'accounts':       d['accounts'],
            'sales':          {str(k): fmt_sar(v) for k, v in d['sales'].items()},
            'yoy_25v24':      yoy,
            'hot_pipeline':   fmt_sar(d['hot_pipeline']),
            'award':          fmt_sar(d['award']),
            'backlog':        fmt_sar(d['backlog']),
            'size_counts':    d['size_counts'],
            'status_counts':  d['status_counts'],
            'transfer_counts': d['transfer_counts'],
            'monthly_sales':  {str(k): v for k, v in monthly.items()},
            'group_sales':    group_data,
        })

    team_list.sort(key=lambda x: x['sales'].get('2025') or 0, reverse=True)
    print(f"  Teams assembled: {len(team_list)}")
    return team_list

def assemble_product_data(sales, accounts):
    print("  Assembling product data...")
    if 'Group' not in sales.columns:
        return {}

    result = {}
    for yr in [2022, 2023, 2024, 2025, 2026]:
        g = sales[sales['year'] == yr].groupby('Group')['Value'].sum()
        result[str(yr)] = {k: fmt_sar(v) for k, v in g.items()}

    # Account-level group shifts
    shifts = {}
    for acc in accounts:
        g24, g25 = acc['group_2024'], acc['group_2025']
        if g24 and g25 and g24 != g25:
            key = f"{g24} → {g25}"
            shifts[key] = shifts.get(key, 0) + 1

    return {
        'by_year':  result,
        'shifts':   dict(sorted(shifts.items(), key=lambda x: -x[1])),
    }

# ── JSON Output ───────────────────────────────────────────────────────────────

def write_json(data, filename):
    os.makedirs(DOCS_DIR, exist_ok=True)
    path = os.path.join(DOCS_DIR, filename)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, default=safe_val)
    print(f"  Written: {path}")

# ── HTML Dashboard ────────────────────────────────────────────────────────────

def build_html():
    html = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ASAP Sales Dashboard — Shaker Group</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#f8f9fa;--surface:#ffffff;--surface2:#f1f3f5;
  --border:#e5e7eb;--border2:#d1d5db;
  --text:#111827;--text2:#6b7280;--text3:#9ca3af;
  --accent:#1d4ed8;--accent-light:#eff6ff;
  --success:#16a34a;--success-bg:#f0fdf4;
  --warn:#d97706;--warn-bg:#fffbeb;
  --danger:#dc2626;--danger-bg:#fef2f2;
  --info:#2563eb;--info-bg:#eff6ff;
  --radius:8px;--radius-lg:12px;
  --font:'DM Sans',sans-serif;
}
body{font-family:var(--font);font-size:13px;color:var(--text);background:var(--bg);min-height:100vh}

/* Nav */
.nav{display:flex;align-items:center;background:var(--surface);border-bottom:1px solid var(--border);padding:0 24px;position:sticky;top:0;z-index:100}
.nav-brand{font-size:12px;font-weight:500;color:var(--text2);padding:14px 16px 14px 0;border-right:1px solid var(--border);margin-right:4px;white-space:nowrap}
.nav-link{font-size:12px;padding:14px 14px;color:var(--text2);cursor:pointer;border-bottom:2px solid transparent;white-space:nowrap;user-select:none}
.nav-link:hover{color:var(--text)}
.nav-link.active{color:var(--text);font-weight:500;border-bottom:2px solid var(--accent)}
.nav-updated{margin-left:auto;font-size:11px;color:var(--text3)}

/* Pages */
.page{display:none;padding:20px 24px 40px}
.page.active{display:block}
.page-header{margin-bottom:16px}
.page-title{font-size:15px;font-weight:500;margin-bottom:2px}
.page-sub{font-size:11px;color:var(--text2)}

/* KPI grid */
.kpi-grid{display:grid;gap:10px;margin-bottom:16px}
.g2{grid-template-columns:repeat(2,minmax(0,1fr))}
.g3{grid-template-columns:repeat(3,minmax(0,1fr))}
.g4{grid-template-columns:repeat(4,minmax(0,1fr))}
.g5{grid-template-columns:repeat(5,minmax(0,1fr))}
.kpi{background:var(--surface2);border-radius:var(--radius);padding:12px 14px}
.kpi-label{font-size:11px;color:var(--text2);margin-bottom:5px}
.kpi-val{font-size:20px;font-weight:500}
.kpi-sub{font-size:11px;color:var(--text2);margin-top:3px}
.kpi-pos{font-size:11px;color:var(--success);margin-top:3px}
.kpi-neg{font-size:11px;color:var(--danger);margin-top:3px}

/* Cards */
.card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:14px 16px;margin-bottom:12px}
.card-title{font-size:12px;font-weight:500;margin-bottom:3px}
.card-sub{font-size:11px;color:var(--text2);margin-bottom:10px}

/* Layout */
.two-col{display:grid;grid-template-columns:minmax(0,1.3fr) minmax(0,1fr);gap:12px}
.three-col{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}
.half{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
.divider{height:1px;background:var(--border);margin:10px 0}

/* Filters */
.filter-bar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:12px;padding:10px 12px;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius)}
.filter-bar select,.filter-bar input{font-size:11px;padding:5px 8px;border:1px solid var(--border2);border-radius:var(--radius);background:var(--surface);color:var(--text);height:30px}
.filter-bar input[type=text]{width:160px}
.filter-chips{display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.chip{display:inline-flex;align-items:center;gap:4px;font-size:11px;padding:3px 8px;border-radius:20px;background:var(--info-bg);color:var(--info);border:1px solid #bfdbfe}
.chip-x{cursor:pointer;font-size:12px;line-height:1;padding:0 2px;opacity:.7}
.chip-x:hover{opacity:1}
.btn-clear{font-size:11px;padding:4px 10px;border:1px solid var(--border2);border-radius:var(--radius);background:var(--surface);color:var(--text2);cursor:pointer;height:30px}
.btn-clear:hover{background:var(--surface2)}
.btn-export{font-size:11px;padding:4px 12px;border:1px solid var(--accent);border-radius:var(--radius);background:var(--accent-light);color:var(--accent);cursor:pointer;height:30px;font-weight:500}
.btn-export:hover{background:var(--accent);color:#fff}
.filter-count{margin-left:auto;font-size:11px;color:var(--text3);white-space:nowrap}

/* Tables */
.tbl-wrap{overflow-x:auto;border-radius:var(--radius-lg);border:1px solid var(--border)}
table{width:100%;border-collapse:collapse;font-size:11px;background:var(--surface)}
th{font-size:10px;font-weight:500;color:var(--text2);text-align:left;padding:7px 10px;border-bottom:1px solid var(--border);background:var(--surface2);white-space:nowrap;position:sticky;top:0}
td{padding:7px 10px;border-bottom:1px solid var(--border);color:var(--text);vertical-align:middle}
tr:last-child td{border-bottom:none}
.r{text-align:right;font-variant-numeric:tabular-nums}
.c{text-align:center}
tr.clickable{cursor:pointer}
tr.clickable:hover td{background:var(--surface2)}
tr.selected td{background:var(--accent-light)!important}
.dim{color:var(--text3)}
.pos{color:var(--success)}
.neg{color:var(--danger)}
.section-head td{background:var(--surface2);font-size:10px;font-weight:500;color:var(--text2);padding:5px 10px;letter-spacing:.3px}

/* Badges */
.badge{display:inline-block;font-size:10px;font-weight:500;padding:2px 7px;border-radius:20px;white-space:nowrap}
.b-inter{background:#fee2e2;color:#991b1b}
.b-intra{background:#fef3c7;color:#92400e}
.b-same{background:#dcfce7;color:#14532d}
.b-new{background:#dbeafe;color:#1e3a8a}
.b-active{background:#dcfce7;color:#14532d}
.b-inactive{background:#f3f4f6;color:#374151}
.b-react{background:#fef3c7;color:#92400e}
.b-pot{background:#ede9fe;color:#4c1d95}
.b-owner{background:#dcfce7;color:#14532d}
.b-transact{background:#fef3c7;color:#92400e}
.b-aplus{background:#dbeafe;color:#1e3a8a}
.b-a{background:#dcfce7;color:#14532d}
.b-b{background:#fef3c7;color:#92400e}
.b-c{background:#f3f4f6;color:#374151}
.b-d{background:#fee2e2;color:#991b1b}
.b-warn{background:#fef3c7;color:#92400e}
.b-danger{background:#fee2e2;color:#991b1b}
.b-good{background:#dcfce7;color:#14532d}
.b-unresolved{background:#fce7f3;color:#9d174d}

/* Drilldown */
.drilldown-row td{padding:0;background:var(--surface2)!important}
.drilldown-inner{padding:16px 20px}
.drilldown-title{font-size:12px;font-weight:500;margin-bottom:10px;display:flex;align-items:center;justify-content:space-between}
.dd-close{font-size:11px;color:var(--text2);cursor:pointer;padding:3px 10px;border:1px solid var(--border2);border-radius:var(--radius)}
.dd-close:hover{background:var(--surface)}

/* Verdict */
.verdict{padding:9px 12px;border-radius:var(--radius);font-size:11px;line-height:1.5;margin-bottom:10px;border-left:3px solid}
.verdict.good{background:var(--success-bg);color:#14532d;border-color:var(--success)}
.verdict.warn{background:var(--warn-bg);color:#92400e;border-color:var(--warn)}
.verdict.danger{background:var(--danger-bg);color:#991b1b;border-color:var(--danger)}
.verdict.info{background:var(--info-bg);color:#1e3a8a;border-color:var(--info)}

/* Insights */
.insight{padding:8px 12px;border-radius:0 var(--radius) var(--radius) 0;font-size:11px;line-height:1.5;margin-bottom:6px;border-left:3px solid var(--info);background:var(--surface2);color:var(--text2)}
.insight.warn{border-left-color:var(--warn)}
.insight.danger{border-left-color:var(--danger)}
.insight.good{border-left-color:var(--success)}

/* Bar charts */
.bar-wrap{margin-bottom:8px}
.bar-label{display:flex;justify-content:space-between;font-size:11px;color:var(--text2);margin-bottom:3px}
.bar-track{height:6px;background:var(--surface2);border-radius:3px;overflow:hidden;display:flex;gap:2px}
.bar-fill{height:100%;border-radius:3px;min-width:2px}
.fill-blue{background:#3b82f6}
.fill-blue-light{background:#93c5fd}
.fill-green{background:#22c55e}
.fill-red{background:#ef4444}
.fill-amber{background:#f59e0b}
.fill-gray{background:#9ca3af}

/* Monthly chart */
.month-chart{display:flex;align-items:flex-end;gap:2px;height:70px}
.m-wrap{display:flex;flex-direction:column;align-items:center;gap:2px;flex:1}
.m-bars{display:flex;gap:1px;align-items:flex-end;height:60px;width:100%}
.m-bar{flex:1;border-radius:2px 2px 0 0;min-height:2px}
.m-2024{background:#93c5fd}
.m-2025{background:#3b82f6}
.m-label{font-size:9px;color:var(--text3);text-align:center}
.chart-legend{display:flex;gap:14px;margin-top:6px;font-size:10px;color:var(--text2)}
.legend-dot{width:10px;height:6px;border-radius:2px;display:inline-block;margin-right:4px}

/* Contribution bar */
.contrib-bar{display:flex;height:8px;border-radius:4px;overflow:hidden;margin:6px 0}

/* Segment bar */
.seg-bar{display:flex;height:10px;border-radius:5px;overflow:hidden;margin:8px 0}

/* Cross-filter selected state */
.panel-selected{border:1px solid var(--accent)!important;box-shadow:0 0 0 3px rgba(29,78,216,.1)}
</style>
</head>
<body>

<nav class="nav">
  <span class="nav-brand">Shaker / ASAP Sales</span>
  <span class="nav-link active" data-page="home" onclick="showPage('home',this)">Home</span>
  <span class="nav-link" data-page="accounts" onclick="showPage('accounts',this)">Account explorer</span>
  <span class="nav-link" data-page="se" onclick="showPage('se',this)">SE &amp; team</span>
  <span class="nav-link" data-page="transfer" onclick="showPage('transfer',this)">Transfer impact</span>
  <span class="nav-link" data-page="pipeline" onclick="showPage('pipeline',this)">Pipeline &amp; coverage</span>
  <span class="nav-link" data-page="product" onclick="showPage('product',this)">Product analysis</span>
  <span class="nav-updated" id="last-updated"></span>
</nav>

<!-- PAGE: HOME -->
<div id="page-home" class="page active">
  <div class="page-header">
    <div class="page-title">Overview</div>
    <div class="page-sub" id="home-sub">Loading...</div>
  </div>
  <div class="kpi-grid g5" id="home-kpis"></div>
  <div class="two-col">
    <div class="card">
      <div class="card-title">Sales trend 2022–2026</div>
      <div class="card-sub">Annual invoiced value · 2026 = Jan–Feb actuals</div>
      <div id="home-trend-chart" style="height:120px;display:flex;align-items:flex-end;gap:8px;padding-top:8px"></div>
      <div class="divider"></div>
      <div class="kpi-grid g3" style="margin-bottom:0" id="home-mom-kpis"></div>
    </div>
    <div>
      <div class="card">
        <div class="card-title">Account status breakdown</div>
        <div class="card-sub" id="home-status-sub"></div>
        <div class="seg-bar" id="home-status-bar"></div>
        <div id="home-status-legend" style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:10px;font-size:10px"></div>
        <div class="divider"></div>
        <div id="home-transfer-bars"></div>
      </div>
      <div class="card">
        <div class="card-title">Key insights</div>
        <div id="home-insights"></div>
      </div>
    </div>
  </div>
  <div class="three-col">
    <div class="card">
      <div class="card-title">Sales by team (2025)</div>
      <div id="home-team-bars"></div>
    </div>
    <div class="card">
      <div class="card-title">Contractor size distribution</div>
      <div id="home-size-bars"></div>
    </div>
    <div class="card">
      <div class="card-title">Top product groups (2025)</div>
      <div id="home-group-bars"></div>
    </div>
  </div>
</div>

<!-- PAGE: ACCOUNTS -->
<div id="page-accounts" class="page">
  <div class="page-header">
    <div class="page-title">Account explorer</div>
    <div class="page-sub">Click any row to drill down into SE history and project breakdown</div>
  </div>
  <div class="filter-bar">
    <select id="acc-f-transfer" onchange="applyAccFilters()">
      <option value="">All transfer types</option>
      <option value="inter">Inter-team</option>
      <option value="intra">Intra-team</option>
      <option value="same">Same SE</option>
      <option value="new">New account</option>
    </select>
    <select id="acc-f-team" onchange="applyAccFilters()"><option value="">All teams (current)</option></select>
    <select id="acc-f-oldteam" onchange="applyAccFilters()"><option value="">All teams (old)</option></select>
    <select id="acc-f-se" onchange="applyAccFilters()"><option value="">All SEs</option></select>
    <select id="acc-f-status" onchange="applyAccFilters()">
      <option value="">All statuses</option>
      <option value="Active">Active</option>
      <option value="Inactive">Inactive</option>
      <option value="New">New</option>
      <option value="Reactivated">Reactivated</option>
      <option value="Active/Potential">Active/Potential</option>
    </select>
    <select id="acc-f-size" onchange="applyAccFilters()">
      <option value="">All sizes</option>
      <option value="A+">A+</option>
      <option value="A">A</option>
      <option value="B">B</option>
      <option value="C">C</option>
      <option value="D">D</option>
    </select>
    <select id="acc-f-type" onchange="applyAccFilters()">
      <option value="">All types</option>
      <option value="MEP Contractor">MEP Contractor</option>
      <option value="Main Contractor">Main Contractor</option>
      <option value="AC Specialist">AC Specialist</option>
      <option value="Developer">Developer</option>
      <option value="End Customer">End Customer</option>
    </select>
    <input type="text" id="acc-f-search" placeholder="Search account…" oninput="applyAccFilters()">
    <button class="btn-clear" onclick="clearAccFilters()">Clear filters</button>
    <button class="btn-export" onclick="exportAccTable()">Export ↓</button>
    <span class="filter-count" id="acc-count"></span>
  </div>
  <div id="acc-chips" class="filter-chips" style="margin-bottom:8px"></div>
  <div class="tbl-wrap">
    <table id="acc-table">
      <thead>
        <tr>
          <th style="width:200px">Customer</th>
          <th>Size</th><th>Status</th><th>Type</th><th>Transfer</th>
          <th class="r">2023</th><th class="r">2024</th><th class="r">2025</th><th class="r">2026 YTD</th>
          <th class="r">YoY 25v24</th><th class="r">Hot pipeline</th><th class="r">Award</th><th class="r">Backlog</th>
          <th>Group 24→25</th><th>Current SE · team</th>
        </tr>
      </thead>
      <tbody id="acc-tbody"></tbody>
    </table>
  </div>
</div>

<!-- PAGE: SE & TEAM -->
<div id="page-se" class="page">
  <div class="page-header">
    <div class="page-title">SE &amp; team performance</div>
    <div class="page-sub">Click a team → filters SE panel → click an SE → filters accounts, monthly trend, and product mix</div>
  </div>
  <div class="kpi-grid g4" id="se-kpis"></div>
  <div class="three-col" style="align-items:start">
    <div class="card panel" id="se-team-panel" style="padding:0;overflow:hidden">
      <div style="padding:12px 14px 8px;display:flex;justify-content:space-between;align-items:center">
        <div><div class="card-title">Team summary</div><div class="card-sub">Click to filter</div></div>
        <button class="btn-export" onclick="exportTeamTable()">Export ↓</button>
      </div>
      <table id="se-team-table">
        <thead><tr><th>Team</th><th class="r">Accts</th><th class="r">2024</th><th class="r">2025</th><th class="r">YoY</th><th class="r">Pipeline</th></tr></thead>
        <tbody id="se-team-tbody"></tbody>
      </table>
    </div>
    <div class="card panel" id="se-se-panel" style="padding:0;overflow:hidden">
      <div style="padding:12px 14px 8px;display:flex;justify-content:space-between;align-items:center">
        <div><div class="card-title" id="se-se-title">SE breakdown</div><div class="card-sub">Click to filter</div></div>
        <button class="btn-export" onclick="exportSETable()">Export ↓</button>
      </div>
      <table id="se-se-table">
        <thead><tr><th>SE</th><th class="r">Accts</th><th class="r">2025</th><th class="r">YoY</th><th class="r">Pipeline</th><th>Mix</th></tr></thead>
        <tbody id="se-se-tbody"></tbody>
      </table>
    </div>
    <div class="card panel" id="se-acc-panel" style="padding:0;overflow:hidden">
      <div style="padding:12px 14px 8px;display:flex;justify-content:space-between;align-items:center">
        <div><div class="card-title" id="se-acc-title">Accounts</div><div class="card-sub">Owner vs transacted flag</div></div>
        <button class="btn-export" onclick="exportSEAccTable()">Export ↓</button>
      </div>
      <table id="se-acc-table">
        <thead><tr><th>Account</th><th>Owner?</th><th class="r">2025</th><th class="r">YoY</th><th class="r">Pipeline</th></tr></thead>
        <tbody id="se-acc-tbody"></tbody>
      </table>
    </div>
  </div>
  <div class="half">
    <div class="card">
      <div class="card-title" id="se-chart-title">Monthly sales</div>
      <div class="card-sub">2024 vs 2025 · SAR · bars grouped by month</div>
      <div class="month-chart" id="se-monthly-chart"></div>
      <div class="chart-legend">
        <span><span class="legend-dot" style="background:#93c5fd"></span>2024</span>
        <span><span class="legend-dot" style="background:#3b82f6"></span>2025</span>
      </div>
    </div>
    <div class="card">
      <div class="card-title" id="se-group-title">Sales by product group</div>
      <div class="card-sub">2024 vs 2025</div>
      <div id="se-group-bars"></div>
    </div>
  </div>
  <div class="card">
    <div class="card-title" id="se-flags-title">Decision flags</div>
    <div class="card-sub">Auto-generated from performance data</div>
    <div id="se-flags" style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px"></div>
  </div>
</div>

<!-- PAGE: TRANSFER IMPACT -->
<div id="page-transfer" class="page">
  <div class="page-header">
    <div class="page-title">Transfer impact analysis</div>
    <div class="page-sub">Before (old SE) vs after (new SE) · sales, pipeline, award</div>
  </div>
  <div class="filter-bar">
    <select id="tr-f-type" onchange="applyTransferFilters()">
      <option value="inter">Inter-team transfers</option>
      <option value="intra">Intra-team transfers</option>
      <option value="">All transfers</option>
    </select>
    <select id="tr-f-oldteam" onchange="applyTransferFilters()"><option value="">All old teams</option></select>
    <select id="tr-f-newteam" onchange="applyTransferFilters()"><option value="">All new teams</option></select>
    <select id="tr-f-size" onchange="applyTransferFilters()">
      <option value="">All sizes</option><option value="A+">A+</option><option value="A">A</option><option value="B">B</option>
    </select>
    <button class="btn-clear" onclick="clearTransferFilters()">Clear</button>
    <button class="btn-export" onclick="exportTransferTable()">Export ↓</button>
    <span class="filter-count" id="tr-count"></span>
  </div>
  <div class="kpi-grid g4" id="tr-kpis"></div>
  <div class="two-col">
    <div class="card" style="padding:0;overflow:hidden">
      <div style="padding:12px 14px 8px"><div class="card-title">Account-level before vs after</div><div class="card-sub">Sales · pipeline · award comparison</div></div>
      <div class="tbl-wrap" style="border:none;border-radius:0">
        <table id="tr-table">
          <thead><tr>
            <th>Account</th><th>Size</th><th>Old SE</th><th>New SE</th>
            <th class="r">2024 sales</th><th class="r">2025 sales</th><th class="r">Sales Δ</th>
            <th class="r">Pipeline Δ</th><th class="r">Award Δ</th>
          </tr></thead>
          <tbody id="tr-tbody"></tbody>
        </table>
      </div>
    </div>
    <div>
      <div class="card">
        <div class="card-title">Transfer route breakdown</div>
        <div class="card-sub">Old team → new team · avg sales change</div>
        <table id="tr-routes-table" class="tbl">
          <thead><tr><th>Route</th><th class="r">Accounts</th><th class="r">Avg Δ sales</th></tr></thead>
          <tbody id="tr-routes-tbody"></tbody>
        </table>
      </div>
      <div class="card">
        <div class="card-title">Transfer insights</div>
        <div id="tr-insights"></div>
      </div>
    </div>
  </div>
</div>

<!-- PAGE: PIPELINE & COVERAGE -->
<div id="page-pipeline" class="page">
  <div class="page-header">
    <div class="page-title">Pipeline &amp; coverage</div>
    <div class="page-sub">Hot pipeline (50%+70%) · Award (90%+100%) · Backlog · VALID/NOT DUPLICATE only</div>
  </div>
  <div class="filter-bar">
    <select id="pl-f-team" onchange="applyPipelineFilters()"><option value="">All teams</option></select>
    <select id="pl-f-se" onchange="applyPipelineFilters()"><option value="">All SEs</option></select>
    <select id="pl-f-group" onchange="applyPipelineFilters()"><option value="">All groups</option></select>
    <button class="btn-clear" onclick="clearPipelineFilters()">Clear</button>
    <button class="btn-export" onclick="exportPipelineTable()">Export ↓</button>
  </div>
  <div class="kpi-grid g4" id="pl-kpis"></div>
  <div class="two-col">
    <div class="card">
      <div class="card-title">Pipeline by team</div>
      <div class="card-sub">Hot pipeline + award · SAR</div>
      <div id="pl-team-bars"></div>
    </div>
    <div class="card">
      <div class="card-title">Coverage gap alerts</div>
      <div id="pl-alerts"></div>
      <div class="divider"></div>
      <div class="card-title" style="margin-bottom:8px">Pipeline probability split</div>
      <div id="pl-prob-bars"></div>
    </div>
  </div>
  <div class="card" style="padding:0;overflow:hidden">
    <div style="padding:12px 14px 8px;display:flex;justify-content:space-between">
      <div><div class="card-title">Pipeline detail by SE</div><div class="card-sub">Hot pipeline · award · backlog · coverage ratio</div></div>
    </div>
    <div class="tbl-wrap" style="border:none;border-radius:0">
      <table id="pl-table">
        <thead><tr>
          <th>SE</th><th>Team</th><th class="r">2025 sales</th>
          <th class="r">Hot pipeline</th><th class="r">Award</th><th class="r">Backlog</th>
          <th class="r">Coverage ratio</th>
        </tr></thead>
        <tbody id="pl-tbody"></tbody>
      </table>
    </div>
  </div>
</div>

<!-- PAGE: PRODUCT ANALYSIS -->
<div id="page-product" class="page">
  <div class="page-header">
    <div class="page-title">Product analysis</div>
    <div class="page-sub">Sales by group · market shift detection · account-level product behaviour</div>
  </div>
  <div class="filter-bar">
    <select id="prod-f-team" onchange="applyProductFilters()"><option value="">All teams</option></select>
    <select id="prod-f-se" onchange="applyProductFilters()"><option value="">All SEs</option></select>
    <button class="btn-clear" onclick="clearProductFilters()">Clear</button>
    <button class="btn-export" onclick="exportProductTable()">Export ↓</button>
  </div>
  <div class="kpi-grid g4" id="prod-kpis"></div>
  <div class="two-col">
    <div class="card">
      <div class="card-title">Market group shift 2023 → 2024 → 2025</div>
      <div class="card-sub">Invoiced SAR by product group</div>
      <div id="prod-group-bars"></div>
    </div>
    <div class="card">
      <div class="card-title">Account-level group shifts</div>
      <div class="card-sub">Top group 2024 → top group 2025 · accounts that shifted</div>
      <div id="prod-shifts"></div>
    </div>
  </div>
  <div class="card" style="padding:0;overflow:hidden">
    <div style="padding:12px 14px 8px"><div class="card-title">Account product detail</div><div class="card-sub">Top group per year · shift flag</div></div>
    <div class="tbl-wrap" style="border:none;border-radius:0">
      <table id="prod-table">
        <thead><tr>
          <th>Account</th><th>Type</th><th>SE · team</th>
          <th>Top group 2023</th><th>Top group 2024</th><th>Top group 2025</th>
          <th class="c">Shifted?</th><th class="r">2025 sales</th>
        </tr></thead>
        <tbody id="prod-tbody"></tbody>
      </table>
    </div>
  </div>
</div>

<script>
var ACCOUNTS=[], SE_DATA=[], TEAM_DATA=[], PIPE_DATA=[], PROD_DATA={};
var ACC_INDEX={};
var SEL_TEAM=null, SEL_SE=null;

var MONTHS=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

function sar(v,decimals){
  if(v==null||v===0)return'—';
  var abs=Math.abs(v);
  var s=v<0?'-':'';
  if(abs>=1e9)return s+(abs/1e9).toFixed(1)+'B';
  if(abs>=1e6)return s+(abs/1e6).toFixed(1)+'M';
  if(abs>=1e3)return s+(abs/1e3).toFixed(0)+'K';
  return s+abs.toFixed(0);
}
function pct(v){if(v==null)return'—';return(v>0?'+':'')+v.toFixed(1)+'%';}
function posNeg(v){if(v==null)return'dim';return v>0?'pos':'neg';}

function sizeBadge(tier,trend){
  if(!tier)return'';
  var t=trend==='up'?' ↑':trend==='down'?' ↓':'';
  var cls={'A+':'b-aplus','A':'b-a','B':'b-b','C':'b-c','D':'b-d'}[tier]||'b-c';
  return'<span class="badge '+cls+'">'+tier+t+'</span>';
}
function statusBadge(s){
  var map={'Active':'b-active','Inactive':'b-inactive','New':'b-new','Reactivated':'b-react','Active/Potential':'b-pot','Unknown':'b-inactive'};
  return s?'<span class="badge '+(map[s]||'b-inactive')+'">'+s+'</span>':'';
}
function transferBadge(t){
  var map={'inter':'b-inter','intra':'b-intra','same':'b-same','new':'b-new'};
  var labels={'inter':'Inter','intra':'Intra','same':'Same','new':'New'};
  return t?'<span class="badge '+(map[t]||'')+'">'+labels[t]+'</span>':'';
}
function groupShift(g24,g25){
  if(!g24&&!g25)return'<span class="dim">—</span>';
  if(!g25)return'<span class="dim">'+g24+'</span>';
  if(!g24)return g25;
  if(g24===g25)return g24;
  return'<span style="color:var(--text2)">'+g24+'</span> <span style="color:var(--warn)">→ '+g25+' ↗</span>';
}

function showPage(id,el){
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.nav-link').forEach(l=>l.classList.remove('active'));
  document.getElementById('page-'+id).classList.add('active');
  if(el)el.classList.add('active');
}

// ── Populate filters ──────────────────────────────────────────────────────
function populateFilter(selId, values, defaultLabel){
  var sel=document.getElementById(selId);
  if(!sel)return;
  var curr=sel.value;
  while(sel.options.length>1)sel.remove(1);
  values.forEach(function(v){
    var o=document.createElement('option');
    o.value=v; o.text=v; sel.appendChild(o);
  });
  if(curr)sel.value=curr;
}

function initFilters(){
  var teams=[...new Set(ACCOUNTS.map(a=>a.curr_team).filter(Boolean))].sort();
  var oldTeams=[...new Set(ACCOUNTS.map(a=>a.old_team).filter(Boolean))].sort();
  var ses=[...new Set(ACCOUNTS.map(a=>a.curr_se).filter(Boolean))].sort();

  populateFilter('acc-f-team',teams);
  populateFilter('acc-f-oldteam',oldTeams);
  populateFilter('acc-f-se',ses);
  populateFilter('tr-f-oldteam',oldTeams);
  populateFilter('tr-f-newteam',teams);
  populateFilter('pl-f-team',teams);
  populateFilter('pl-f-se',ses);
  populateFilter('prod-f-team',teams);
  populateFilter('prod-f-se',ses);

  var groups=[...new Set(ACCOUNTS.flatMap(a=>[a.group_2024,a.group_2025]).filter(Boolean))].sort();
  populateFilter('pl-f-group',groups);
}

// ── Home page ─────────────────────────────────────────────────────────────
function renderHome(){
  if(!TEAM_DATA.length)return;
  var totalAccs=ACCOUNTS.length;
  var total25=TEAM_DATA.reduce((s,t)=>s+(t.sales['2025']||0),0);
  var total24=TEAM_DATA.reduce((s,t)=>s+(t.sales['2024']||0),0);
  var total23=TEAM_DATA.reduce((s,t)=>s+(t.sales['2023']||0),0);
  var total22=TEAM_DATA.reduce((s,t)=>s+(t.sales['2022']||0),0);
  var total26=TEAM_DATA.reduce((s,t)=>s+(t.sales['2026']||0),0);
  var totalPipe=TEAM_DATA.reduce((s,t)=>s+(t.hot_pipeline||0),0);
  var totalAward=TEAM_DATA.reduce((s,t)=>s+(t.award||0),0);
  var totalBL=TEAM_DATA.reduce((s,t)=>s+(t.backlog||0),0);
  var yoy25=(total24>0)?((total25-total24)/total24*100):null;

  document.getElementById('home-sub').textContent=
    totalAccs+' accounts · Sales 2022–2026 · Pipeline & Award data';

  document.getElementById('last-updated').textContent=
    'Generated: '+new Date().toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric'});

  var kpis=[
    {l:'2025 sales (SAR)',v:sar(total25),s:yoy25!=null?(yoy25>=0?'▲ '+yoy25.toFixed(1)+'% vs 2024':'▼ '+Math.abs(yoy25).toFixed(1)+'% vs 2024'),sc:yoy25>=0?'kpi-pos':'kpi-neg'},
    {l:'2024 sales (SAR)',v:sar(total24),s:'vs 2023: '+pct(total23>0?(total24-total23)/total23*100:null),sc:'kpi-sub'},
    {l:'Hot pipeline',v:sar(totalPipe),s:'50% + 70% stages',sc:'kpi-sub'},
    {l:'Award',v:sar(totalAward),s:'90% + 100%',sc:'kpi-sub'},
    {l:'Backlog',v:sar(totalBL),s:'current',sc:'kpi-sub'},
  ];
  document.getElementById('home-kpis').innerHTML=kpis.map(k=>
    '<div class="kpi"><div class="kpi-label">'+k.l+'</div><div class="kpi-val">'+k.v+'</div><div class="'+k.sc+'">'+k.s+'</div></div>'
  ).join('');

  // Trend chart bars
  var yrs=[{y:2022,v:total22},{y:2023,v:total23},{y:2024,v:total24},{y:2025,v:total25},{y:2026,v:total26}];
  var maxV=Math.max(...yrs.map(x=>x.v||0));
  document.getElementById('home-trend-chart').innerHTML=yrs.map(function(yr){
    var h=maxV>0?Math.round((yr.v||0)/maxV*100):0;
    var col=yr.y===2022?'var(--text3)':yr.y===2026?'var(--warn)':'var(--accent)';
    return'<div style="display:flex;flex-direction:column;align-items:center;gap:4px;flex:1">'
      +'<div style="font-size:10px;color:var(--text2)">'+sar(yr.v)+'</div>'
      +'<div style="flex:1;width:100%;display:flex;align-items:flex-end">'
      +'<div style="width:100%;height:'+h+'%;background:'+col+';border-radius:3px 3px 0 0;min-height:4px"></div></div>'
      +'<div style="font-size:10px;color:var(--text3)">'+yr.y+(yr.y===2026?' YTD':'')+'</div>'
      +'</div>';
  }).join('');

  // MoM/QoQ/YoY
  document.getElementById('home-mom-kpis').innerHTML=[
    {l:'YoY (25 vs 24)',v:pct(yoy25),c:yoy25>=0?'kpi-pos':'kpi-neg'},
    {l:'2026 YTD vs 2025 YTD',v:'—',c:'kpi-sub'},
    {l:'2024 vs 2023',v:pct(total23>0?(total24-total23)/total23*100:null),c:'kpi-sub'},
  ].map(k=>'<div><div class="kpi-label">'+k.l+'</div><div style="font-size:14px;font-weight:500" class="'+k.c+'">'+k.v+'</div></div>').join('');

  // Status bar
  var statusCounts={'Active':0,'Inactive':0,'New':0,'Reactivated':0,'Active/Potential':0};
  ACCOUNTS.forEach(a=>{if(a.cust_status&&statusCounts.hasOwnProperty(a.cust_status))statusCounts[a.cust_status]++;});
  var statusColors={'Active':'#22c55e','New':'#3b82f6','Reactivated':'#f59e0b','Active/Potential':'#a78bfa','Inactive':'#d1d5db'};
  var total=Object.values(statusCounts).reduce((a,b)=>a+b,0)||1;
  document.getElementById('home-status-sub').textContent=totalAccs+' total accounts · vCust base';
  document.getElementById('home-status-bar').innerHTML=Object.entries(statusCounts).map(([k,v])=>
    '<div style="flex:'+(v/total*100)+';background:'+statusColors[k]+'" class="seg"></div>'
  ).join('');
  document.getElementById('home-status-legend').innerHTML=Object.entries(statusCounts).map(([k,v])=>
    '<span><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:'+statusColors[k]+';margin-right:3px"></span>'+k+' '+v+'</span>'
  ).join('');

  // Transfer bars
  var tCounts={'inter':0,'intra':0,'same':0,'new':0};
  var tLabels={'inter':'Inter-team','intra':'Intra-team','same':'Same SE','new':'New account'};
  var tColors={'inter':'#3b82f6','intra':'#f59e0b','same':'#22c55e','new':'#9ca3af'};
  ACCOUNTS.forEach(a=>{if(tCounts.hasOwnProperty(a.transfer_type))tCounts[a.transfer_type]++;});
  var maxT=Math.max(...Object.values(tCounts))||1;
  document.getElementById('home-transfer-bars').innerHTML=Object.entries(tCounts).map(([k,v])=>
    '<div class="bar-wrap"><div class="bar-label"><span>'+tLabels[k]+'</span><span>'+v+'</span></div>'
    +'<div class="bar-track"><div class="bar-fill" style="width:'+(v/maxT*100)+'%;background:'+tColors[k]+'"></div></div></div>'
  ).join('');

  // Team bars
  var maxTeam=Math.max(...TEAM_DATA.map(t=>t.sales['2025']||0))||1;
  document.getElementById('home-team-bars').innerHTML=TEAM_DATA.slice(0,7).map(t=>
    '<div class="bar-wrap"><div class="bar-label"><span>'+t.team+'</span><span>'+sar(t.sales['2025'])+'</span></div>'
    +'<div class="bar-track"><div class="bar-fill fill-blue" style="width:'+((t.sales['2025']||0)/maxTeam*100)+'%"></div></div></div>'
  ).join('');

  // Size bars
  var sizeAgg={'A+':0,'A':0,'B':0,'C':0,'D':0,'No history':0};
  ACCOUNTS.forEach(function(a){
    if(!a.size_tier)sizeAgg['No history']++;
    else if(sizeAgg.hasOwnProperty(a.size_tier))sizeAgg[a.size_tier]++;
  });
  var sizeColors={'A+':'#3b82f6','A':'#22c55e','B':'#f59e0b','C':'#9ca3af','D':'#ef4444','No history':'#e5e7eb'};
  var maxS=Math.max(...Object.values(sizeAgg))||1;
  document.getElementById('home-size-bars').innerHTML=Object.entries(sizeAgg).map(([k,v])=>
    '<div class="bar-wrap"><div class="bar-label"><span>'+k+'</span><span>'+v+'</span></div>'
    +'<div class="bar-track"><div class="bar-fill" style="width:'+(v/maxS*100)+'%;background:'+sizeColors[k]+'"></div></div></div>'
  ).join('');

  // Group bars (2025)
  var g25=PROD_DATA.by_year&&PROD_DATA.by_year['2025']||{};
  var g25sorted=Object.entries(g25).sort((a,b)=>b[1]-a[1]).slice(0,6);
  var maxG=g25sorted.length?(g25sorted[0][1]||0):1;
  document.getElementById('home-group-bars').innerHTML=g25sorted.map(([g,v])=>
    '<div class="bar-wrap"><div class="bar-label"><span>'+g+'</span><span>'+sar(v)+'</span></div>'
    +'<div class="bar-track"><div class="bar-fill fill-blue" style="width:'+(v/maxG*100)+'%"></div></div></div>'
  ).join('');

  // Insights
  var insights=[];
  var interAccs=ACCOUNTS.filter(a=>a.transfer_type==='inter');
  if(interAccs.length){
    var with25=interAccs.filter(a=>a.sales&&a.sales['2024']&&a.sales['2025']);
    if(with25.length){
      var avgChg=with25.reduce((s,a)=>(s+(a.sales['2025']-a.sales['2024'])/a.sales['2024']),0)/with25.length*100;
      insights.push({t:'danger',m:interAccs.length+' inter-team transfers — avg sales change on transferred accounts: '+pct(avgChg)});
    }
  }
  var aplusInactive=ACCOUNTS.filter(a=>a.size_tier==='A+'&&a.cust_status==='Inactive');
  if(aplusInactive.length)insights.push({t:'warn',m:aplusInactive.length+' A+ accounts classified as Inactive — review for Active/Potential override'});
  var newAccs=ACCOUNTS.filter(a=>a.transfer_type==='new');
  if(newAccs.length)insights.push({t:'good',m:newAccs.length+' new accounts in 2025/2026 — strongest new account intake'});
  var noLowPipe=SE_DATA.filter(s=>s.hot_pipeline>0&&s.sales['2025']>0&&s.hot_pipeline/s.sales['2025']<1.5);
  if(noLowPipe.length)insights.push({t:'warn',m:noLowPipe.length+' SEs with pipeline coverage below 1.5× 2025 sales — at risk for 2026'});
  document.getElementById('home-insights').innerHTML=insights.map(i=>
    '<div class="insight '+i.t+'">'+i.m+'</div>'
  ).join('');
}

// ── Account Explorer ──────────────────────────────────────────────────────
var ACC_FILTERED=[];
function applyAccFilters(){
  var fTr=document.getElementById('acc-f-transfer').value;
  var fTeam=document.getElementById('acc-f-team').value;
  var fOld=document.getElementById('acc-f-oldteam').value;
  var fSE=document.getElementById('acc-f-se').value;
  var fSt=document.getElementById('acc-f-status').value;
  var fSz=document.getElementById('acc-f-size').value;
  var fTy=document.getElementById('acc-f-type').value;
  var fSrch=document.getElementById('acc-f-search').value.toLowerCase();

  ACC_FILTERED=ACCOUNTS.filter(function(a){
    if(fTr&&a.transfer_type!==fTr)return false;
    if(fTeam&&a.curr_team!==fTeam)return false;
    if(fOld&&a.old_team!==fOld)return false;
    if(fSE&&a.curr_se!==fSE)return false;
    if(fSt&&a.cust_status!==fSt)return false;
    if(fSz&&a.size_tier!==fSz)return false;
    if(fTy&&a.description!==fTy)return false;
    if(fSrch&&!a.name.toLowerCase().includes(fSrch))return false;
    return true;
  });

  document.getElementById('acc-count').textContent=ACC_FILTERED.length+' accounts shown';
  renderAccTable();
  renderAccChips(fTr,fTeam,fOld,fSE,fSt,fSz,fTy,fSrch);
}

function clearAccFilters(){
  ['acc-f-transfer','acc-f-team','acc-f-oldteam','acc-f-se','acc-f-status','acc-f-size','acc-f-type'].forEach(function(id){
    document.getElementById(id).value='';
  });
  document.getElementById('acc-f-search').value='';
  applyAccFilters();
}

function renderAccChips(fTr,fTeam,fOld,fSE,fSt,fSz,fTy,fSrch){
  var chips=[];
  if(fTr)chips.push({l:'Transfer: '+fTr,f:'acc-f-transfer'});
  if(fTeam)chips.push({l:'Team: '+fTeam,f:'acc-f-team'});
  if(fOld)chips.push({l:'Old team: '+fOld,f:'acc-f-oldteam'});
  if(fSE)chips.push({l:'SE: '+fSE,f:'acc-f-se'});
  if(fSt)chips.push({l:'Status: '+fSt,f:'acc-f-status'});
  if(fSz)chips.push({l:'Size: '+fSz,f:'acc-f-size'});
  if(fTy)chips.push({l:'Type: '+fTy,f:'acc-f-type'});
  if(fSrch)chips.push({l:'Search: '+fSrch,f:'acc-f-search',isText:true});
  document.getElementById('acc-chips').innerHTML=chips.map(c=>
    '<span class="chip">'+c.l+'<span class="chip-x" onclick="clearChip(\''+c.f+'\','+!!c.isText+')">×</span></span>'
  ).join('');
}

function clearChip(fieldId,isText){
  var el=document.getElementById(fieldId);
  if(isText)el.value=''; else el.value='';
  applyAccFilters();
}

function renderAccTable(){
  var tbody=document.getElementById('acc-tbody');
  var rows='';
  var SORT_ORDER={inter:0,intra:1,same:2,new:3};
  var sorted=ACC_FILTERED.slice().sort(function(a,b){
    var ta=SORT_ORDER[a.transfer_type]||0, tb=SORT_ORDER[b.transfer_type]||0;
    if(ta!==tb)return ta-tb;
    return(b.sales&&b.sales['2025']||0)-(a.sales&&a.sales['2025']||0);
  });

  var prevSection='';
  sorted.forEach(function(a,i){
    var sec=a.transfer_type;
    if(sec!==prevSection){
      var secLabels={inter:'Inter-team transfers',intra:'Intra-team transfers',same:'Same SE',new:'New accounts'};
      var secCount=sorted.filter(x=>x.transfer_type===sec).length;
      rows+='<tr class="section-head"><td colspan="15">'+secLabels[sec]+' — '+secCount+' accounts</td></tr>';
      prevSection=sec;
    }
    var idx=ACC_INDEX[a.customer]!==undefined?ACC_INDEX[a.customer]:i;
    var unres=a.unresolved_cust?'<span class="badge b-unresolved" style="font-size:9px">!</span>':'';
    rows+='<tr class="clickable" onclick="toggleDD('+idx+')">'
      +'<td style="font-weight:500">'+a.name+unres+'</td>'
      +'<td>'+sizeBadge(a.size_tier,a.size_trend)+'</td>'
      +'<td>'+statusBadge(a.cust_status)+'</td>'
      +'<td><span style="font-size:10px;padding:2px 6px;border-radius:4px;background:var(--surface2);color:var(--text2)">'+a.description+'</span></td>'
      +'<td>'+transferBadge(a.transfer_type)+'</td>'
      +'<td class="r '+(a.sales&&a.sales['2023']?'':'dim')+'">'+sar(a.sales&&a.sales['2023'])+'</td>'
      +'<td class="r '+(a.sales&&a.sales['2024']?'':'dim')+'">'+sar(a.sales&&a.sales['2024'])+'</td>'
      +'<td class="r '+(a.sales&&a.sales['2025']?'':'dim')+'">'+sar(a.sales&&a.sales['2025'])+'</td>'
      +'<td class="r dim">'+sar(a.sales&&a.sales['2026'])+'</td>'
      +'<td class="r '+(a.yoy_25v24!=null?posNeg(a.yoy_25v24):'dim')+'">'+pct(a.yoy_25v24)+'</td>'
      +'<td class="r">'+sar(a.hot_pipeline)+'</td>'
      +'<td class="r">'+sar(a.award)+'</td>'
      +'<td class="r">'+sar(a.backlog)+'</td>'
      +'<td>'+groupShift(a.group_2024,a.group_2025)+'</td>'
      +'<td style="font-size:10px;color:var(--text2)">'+a.curr_se+(a.curr_team?' · <span style="color:var(--text3)">'+a.curr_team+'</span>':'')+'</td>'
      +'</tr>'
      +'<tr id="dd-'+idx+'" class="drilldown-row" style="display:none"><td colspan="15">'+renderDrilldown(a)+'</td></tr>';
  });
  tbody.innerHTML=rows;
}

function renderDrilldown(a){
  var v=a.verdict;
  var verdictHtml=v?'<div class="verdict '+v.type+'">'+v.text+'</div>':'';

  // SE history table
  var seRows=(a.se_history||[]).map(function(s){
    var ownerBadge=s.is_owner?'<span class="badge b-owner">Owner</span>':'<span class="badge b-transact">Transacted</span>';
    var yrs=[2022,2023,2024,2025,2026].map(yr=>'<td class="r '+(s.yr_sales[yr]?'':'dim')+'">'+sar(s.yr_sales[yr])+'</td>').join('');
    return'<tr><td>'+s.se_name+'</td><td>'+ownerBadge+'</td>'+yrs
      +'<td class="r"><b>'+s.contrib_pct+'%</b></td>'
      +'<td class="r dim">—</td></tr>';
  }).join('');

  // Contribution bar
  var totalContrib=100;
  var colors=['#3b82f6','#22c55e','#f59e0b','#9ca3af','#ef4444'];
  var contribBar=(a.se_history||[]).slice(0,5).map(function(s,i){
    return'<div style="flex:'+(s.contrib_pct/100)+';background:'+colors[i]+'" class="contrib-seg"></div>';
  }).join('');
  var contribLegend=(a.se_history||[]).slice(0,5).map(function(s,i){
    return'<span style="font-size:10px;color:var(--text2)"><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:'+colors[i]+';margin-right:3px"></span>'+s.se_name.split(' ')[0]+' '+s.contrib_pct+'%</span>';
  }).join(' ');

  // Projects table
  var projRows=(a.projects||[]).map(function(p){
    var typeBadge=p.is_award?'<span class="badge b-a" style="font-size:9px">Award</span>':p.is_hot?'<span class="badge b-new" style="font-size:9px">Hot</span>':'';
    return'<tr><td style="font-size:10px">'+( p.project_name||'—')+'</td>'
      +'<td style="font-size:10px;color:var(--text2)">'+(p.group||'—')+'</td>'
      +'<td>'+typeBadge+'</td>'
      +'<td class="r">'+sar(p.q_gross_value)+'</td>'
      +'<td class="r">'+sar(p.backlog_val)+'</td>'
      +'<td class="r dim">'+(p.expected_award||'—')+'</td></tr>';
  }).join('');

  return'<div class="drilldown-inner">'
    +'<div class="drilldown-title"><span>'+a.name+' — drilldown</span><span class="dd-close" onclick="closeDD()">close ✕</span></div>'
    +verdictHtml
    +'<div class="half">'
    +'<div>'
    +'<div class="card-title" style="margin-bottom:6px">SE history — all salesmen on this account</div>'
    +'<div style="overflow-x:auto"><table><thead><tr><th>Salesman</th><th>Role</th><th class="r">2022</th><th class="r">2023</th><th class="r">2024</th><th class="r">2025</th><th class="r">2026</th><th class="r">Contrib%</th><th class="r">Backlog</th></tr></thead>'
    +'<tbody>'+seRows+'</tbody></table></div>'
    +(contribBar?'<div style="margin-top:8px"><div style="font-size:10px;color:var(--text2);margin-bottom:4px">Sales contribution split</div><div class="contrib-bar">'+contribBar+'</div><div style="display:flex;gap:10px;flex-wrap:wrap">'+contribLegend+'</div></div>':'')
    +'</div>'
    +'<div>'
    +'<div class="card-title" style="margin-bottom:6px">Projects on this account</div>'
    +'<div style="overflow-x:auto"><table><thead><tr><th>Project</th><th>Group</th><th>Type</th><th class="r">Value</th><th class="r">Backlog</th><th class="r">Exp. award</th></tr></thead>'
    +'<tbody>'+(projRows||'<tr><td colspan="6" class="dim" style="text-align:center;padding:12px">No pipeline data for this account</td></tr>')+'</tbody></table></div>'
    +'</div>'
    +'</div>'
    +'</div>';
}

var OPEN_DD=null;
function toggleDD(idx){
  var row=document.getElementById('dd-'+idx);
  if(!row)return;
  if(OPEN_DD!==null&&OPEN_DD!==idx){
    var prev=document.getElementById('dd-'+OPEN_DD);
    if(prev)prev.style.display='none';
  }
  row.style.display=(row.style.display==='none'||row.style.display==='')?'table-row':'none';
  OPEN_DD=(row.style.display==='table-row')?idx:null;
}
function closeDD(){if(OPEN_DD!==null){var r=document.getElementById('dd-'+OPEN_DD);if(r)r.style.display='none';OPEN_DD=null;}}

// ── SE & Team ─────────────────────────────────────────────────────────────
function renderSEPage(){
  renderTeamTable();
  renderSETable(null);
  renderSEAccs(null,null);
  renderSECharts(null,null);
  renderSEFlags(null);
  updateSEKpis(null,null);
}

function renderTeamTable(){
  var maxS=Math.max(...TEAM_DATA.map(t=>t.sales['2025']||0))||1;
  document.getElementById('se-team-tbody').innerHTML=TEAM_DATA.map(function(t){
    var isSelected=SEL_TEAM===t.team;
    return'<tr class="clickable'+(isSelected?' selected':'')+'" onclick="selectTeam(\''+t.team+'\')">'
      +'<td style="font-weight:'+(isSelected?'500':'400')+'">'+t.team+'</td>'
      +'<td class="r">'+t.account_count+'</td>'
      +'<td class="r">'+sar(t.sales['2024'])+'</td>'
      +'<td class="r">'+sar(t.sales['2025'])+'</td>'
      +'<td class="r '+(t.yoy_25v24!=null?posNeg(t.yoy_25v24):'dim')+'">'+pct(t.yoy_25v24)+'</td>'
      +'<td class="r">'+sar(t.hot_pipeline)+'</td>'
      +'</tr>';
  }).join('');
}

function selectTeam(team){
  SEL_TEAM=(SEL_TEAM===team)?null:team;
  SEL_SE=null;
  renderTeamTable();
  renderSETable(SEL_TEAM);
  renderSEAccs(SEL_TEAM,null);
  renderSECharts(SEL_TEAM,null);
  renderSEFlags(SEL_TEAM);
  updateSEKpis(SEL_TEAM,null);
}

function renderSETable(team){
  var ses=team?SE_DATA.filter(s=>s.team===team):SE_DATA;
  document.getElementById('se-se-title').textContent='SE breakdown'+(team?' — '+team:'');
  document.getElementById('se-se-tbody').innerHTML=ses.map(function(s){
    var isSelected=SEL_SE===s.se_name;
    var mixHtml=Object.entries(s.size_counts||{}).slice(0,2).map(function([k,v]){
      var cls={'A+':'b-aplus','A':'b-a','B':'b-b','C':'b-c','D':'b-d'}[k]||'b-c';
      return'<span class="badge '+cls+'" style="font-size:9px">'+k+'×'+v+'</span>';
    }).join(' ');
    return'<tr class="clickable'+(isSelected?' selected':'')+'" onclick="selectSE(\''+s.se_name.replace(/'/g,"\\'")+'\')">'
      +'<td style="font-weight:'+(isSelected?'500':'400')+'">'+s.se_name+'</td>'
      +'<td class="r">'+s.account_count+'</td>'
      +'<td class="r">'+sar(s.sales['2025'])+'</td>'
      +'<td class="r '+(s.yoy_25v24!=null?posNeg(s.yoy_25v24):'dim')+'">'+pct(s.yoy_25v24)+'</td>'
      +'<td class="r">'+sar(s.hot_pipeline)+'</td>'
      +'<td>'+mixHtml+'</td>'
      +'</tr>';
  }).join('');
}

function selectSE(seName){
  SEL_SE=(SEL_SE===seName)?null:seName;
  renderSETable(SEL_TEAM);
  renderSEAccs(SEL_TEAM,SEL_SE);
  renderSECharts(SEL_TEAM,SEL_SE);
  updateSEKpis(SEL_TEAM,SEL_SE);
}

function renderSEAccs(team,se){
  var accs=ACCOUNTS.filter(function(a){
    if(se)return a.curr_se===se;
    if(team)return a.curr_team===team;
    return false;
  }).sort((a,b)=>(b.sales&&b.sales['2025']||0)-(a.sales&&a.sales['2025']||0));

  document.getElementById('se-acc-title').textContent='Accounts'+(se?' — '+se:team?' — '+team:'');
  document.getElementById('se-acc-tbody').innerHTML=accs.slice(0,50).map(function(a){
    var isOwner=true;
    var ownerBadge=isOwner?'<span class="badge b-owner">Owner</span>':'<span class="badge b-transact">Trans.</span>';
    return'<tr class="clickable"><td style="font-size:11px">'+a.name+'</td>'
      +'<td>'+ownerBadge+'</td>'
      +'<td class="r">'+sar(a.sales&&a.sales['2025'])+'</td>'
      +'<td class="r '+(a.yoy_25v24!=null?posNeg(a.yoy_25v24):'dim')+'">'+pct(a.yoy_25v24)+'</td>'
      +'<td class="r">'+sar(a.hot_pipeline)+'</td>'
      +'</tr>';
  }).join('');
}

function renderSECharts(team,se){
  var data=null;
  if(se){data=SE_DATA.find(s=>s.se_name===se);}
  else if(team){data=TEAM_DATA.find(t=>t.team===team);}
  else if(TEAM_DATA.length){
    // Aggregate all
    var allMonthly={};
    TEAM_DATA.forEach(function(t){
      Object.entries(t.monthly_sales||{}).forEach(function([yr,months]){
        if(!allMonthly[yr])allMonthly[yr]={};
        Object.entries(months||{}).forEach(function([m,v]){
          allMonthly[yr][m]=(allMonthly[yr][m]||0)+(v||0);
        });
      });
    });
    data={monthly_sales:allMonthly,group_sales:PROD_DATA.by_year||{}};
  }

  document.getElementById('se-chart-title').textContent='Monthly sales'+(se?' — '+se:team?' — '+team:' — All teams');
  document.getElementById('se-group-title').textContent='Sales by product group'+(se?' — '+se:team?' — '+team:'');

  if(!data){document.getElementById('se-monthly-chart').innerHTML='<div style="color:var(--text3);font-size:11px">Select a team or SE</div>';return;}

  // Monthly bars
  var m24=data.monthly_sales&&data.monthly_sales['2024']||{};
  var m25=data.monthly_sales&&data.monthly_sales['2025']||{};
  var allVals=Object.values(m24).concat(Object.values(m25)).filter(Boolean);
  var maxM=allVals.length?Math.max(...allVals):1;
  document.getElementById('se-monthly-chart').innerHTML=MONTHS.map(function(ml,i){
    var m=i+1;
    var v24=m24[m]||0, v25=m25[m]||0;
    var h24=maxM>0?Math.round(v24/maxM*100):0;
    var h25=maxM>0?Math.round(v25/maxM*100):0;
    return'<div class="m-wrap"><div class="m-bars">'
      +'<div class="m-bar m-2024" style="height:'+h24+'%"></div>'
      +'<div class="m-bar m-2025" style="height:'+h25+'%"></div>'
      +'</div><div class="m-label">'+ml+'</div></div>';
  }).join('');

  // Group bars
  var g24=data.group_sales&&data.group_sales['2024']||{};
  var g25=data.group_sales&&data.group_sales['2025']||{};
  var groups=[...new Set(Object.keys(g24).concat(Object.keys(g25)))];
  var maxGV=Math.max(...groups.map(g=>Math.max(g24[g]||0,g25[g]||0)))||1;
  document.getElementById('se-group-bars').innerHTML=groups.map(function(g){
    var v24=g24[g]||0, v25=g25[g]||0;
    var delta=v24>0?(v25-v24)/v24*100:null;
    var fill25=delta==null?'fill-blue':delta>=0?'fill-green':'fill-red';
    return'<div class="bar-wrap"><div class="bar-label"><span>'+g+'</span><span>'
      +sar(v24)+' → '+sar(v25)+(delta!=null?' <span style="color:var('+(delta>=0?'--success':'--danger')+')">'+pct(delta)+'</span>':'')
      +'</span></div>'
      +'<div class="bar-track">'
      +'<div class="bar-fill fill-blue-light" style="width:'+(v24/maxGV*100)+'%;flex-shrink:0"></div>'
      +'<div class="bar-fill '+fill25+'" style="width:'+(v25/maxGV*100)+'%;flex-shrink:0"></div>'
      +'</div></div>';
  }).join('');
}

function renderSEFlags(team){
  var ses=team?SE_DATA.filter(s=>s.team===team):SE_DATA;
  var flags=[];
  ses.forEach(function(s){
    var s25=s.sales['2025']||0;
    var s24=s.sales['2024']||0;
    var yoy=s24>0?(s25-s24)/s24*100:null;
    var apluses=s.size_counts&&s.size_counts['A+']||0;
    if(apluses>=3&&yoy!=null&&yoy<-10)
      flags.push({t:'danger',m:s.se_name+' — '+apluses+' A+ accounts, YoY '+pct(yoy)+'. High-value book declining.'});
    else if(yoy!=null&&yoy<-15)
      flags.push({t:'warn',m:s.se_name+' — YoY '+pct(yoy)+'. Review account coverage and pipeline.'});
    else if(yoy!=null&&yoy>15)
      flags.push({t:'good',m:s.se_name+' — Strong YoY growth '+pct(yoy)+'. Pipeline: '+sar(s.hot_pipeline)+'.'});
  });
  document.getElementById('se-flags-title').textContent='Decision flags'+(team?' — '+team:'');
  document.getElementById('se-flags').innerHTML=flags.slice(0,8).map(f=>'<div class="insight '+f.t+'">'+f.m+'</div>').join('');
}

function updateSEKpis(team,se){
  var s25=0,s24=0,pipe=0,accts=0;
  if(se){var d=SE_DATA.find(x=>x.se_name===se);if(d){s25=d.sales['2025']||0;s24=d.sales['2024']||0;pipe=d.hot_pipeline||0;accts=d.account_count;}}
  else if(team){var d=TEAM_DATA.find(x=>x.team===team);if(d){s25=d.sales['2025']||0;s24=d.sales['2024']||0;pipe=d.hot_pipeline||0;accts=d.account_count;}}
  else{s25=TEAM_DATA.reduce((a,t)=>a+(t.sales['2025']||0),0);s24=TEAM_DATA.reduce((a,t)=>a+(t.sales['2024']||0),0);pipe=TEAM_DATA.reduce((a,t)=>a+(t.hot_pipeline||0),0);accts=ACCOUNTS.length;}
  var yoy=s24>0?(s25-s24)/s24*100:null;
  document.getElementById('se-kpis').innerHTML=[
    {l:'Sales 2025',v:sar(s25),s:pct(yoy),sc:yoy>=0?'kpi-pos':'kpi-neg'},
    {l:'Sales 2024',v:sar(s24),s:'prior year',sc:'kpi-sub'},
    {l:'Hot pipeline',v:sar(pipe),s:'50%+70%',sc:'kpi-sub'},
    {l:'Account count',v:accts,s:'current selection',sc:'kpi-sub'},
  ].map(k=>'<div class="kpi"><div class="kpi-label">'+k.l+'</div><div class="kpi-val">'+k.v+'</div><div class="'+k.sc+'">'+k.s+'</div></div>').join('');
}

// ── Transfer Impact ───────────────────────────────────────────────────────
function renderTransferPage(){
  applyTransferFilters();
}

function applyTransferFilters(){
  var fType=document.getElementById('tr-f-type').value;
  var fOld=document.getElementById('tr-f-oldteam').value;
  var fNew=document.getElementById('tr-f-newteam').value;
  var fSz=document.getElementById('tr-f-size').value;

  var filtered=ACCOUNTS.filter(function(a){
    if(fType&&a.transfer_type!==fType)return false;
    else if(!fType&&a.transfer_type==='same'||a.transfer_type==='new')return false;
    if(fOld&&a.old_team!==fOld)return false;
    if(fNew&&a.curr_team!==fNew)return false;
    if(fSz&&a.size_tier!==fSz)return false;
    return true;
  });

  document.getElementById('tr-count').textContent=filtered.length+' transferred accounts';

  // KPIs
  var improved=filtered.filter(a=>a.sales&&a.sales['2024']&&a.sales['2025']&&a.sales['2025']>a.sales['2024']);
  var aAplus=filtered.filter(a=>a.size_tier==='A+'&&(a.cust_status==='Inactive'));
  var withBoth=filtered.filter(a=>a.sales&&a.sales['2024']&&a.sales['2025']);
  var avgChg=withBoth.length?withBoth.reduce((s,a)=>(s+(a.sales['2025']-a.sales['2024'])/a.sales['2024']),0)/withBoth.length*100:null;

  document.getElementById('tr-kpis').innerHTML=[
    {l:'Transferred accounts',v:filtered.length,s:'in selection',sc:'kpi-sub'},
    {l:'Avg sales change',v:pct(avgChg),s:'2025 vs 2024',sc:avgChg>=0?'kpi-pos':'kpi-neg'},
    {l:'Accounts improved',v:improved.length,s:filtered.length?Math.round(improved.length/filtered.length*100)+'% of transferred':'—',sc:'kpi-pos'},
    {l:'A+ accounts lost',v:aAplus.length,s:'now inactive post-transfer',sc:'kpi-neg'},
  ].map(k=>'<div class="kpi"><div class="kpi-label">'+k.l+'</div><div class="kpi-val">'+k.v+'</div><div class="'+k.sc+'">'+k.s+'</div></div>').join('');

  // Table
  document.getElementById('tr-tbody').innerHTML=filtered.slice(0,200).map(function(a){
    var s24=a.sales&&a.sales['2024']||null;
    var s25=a.sales&&a.sales['2025']||null;
    var delta=s24&&s25?(s25-s24)/s24*100:null;
    return'<tr>'
      +'<td style="font-weight:500">'+a.name+'</td>'
      +'<td>'+sizeBadge(a.size_tier,a.size_trend)+'</td>'
      +'<td style="font-size:10px;color:var(--text2)">'+a.old_se+'</td>'
      +'<td style="font-size:10px">'+a.curr_se+'</td>'
      +'<td class="r">'+sar(s24)+'</td>'
      +'<td class="r">'+sar(s25)+'</td>'
      +'<td class="r '+(delta!=null?posNeg(delta):'dim')+'">'+pct(delta)+'</td>'
      +'<td class="r dim">—</td>'
      +'<td class="r dim">—</td>'
      +'</tr>';
  }).join('');

  // Routes
  var routeMap={};
  filtered.forEach(function(a){
    if(!a.old_team||!a.curr_team||a.old_team===a.curr_team)return;
    var key=a.old_team+' → '+a.curr_team;
    if(!routeMap[key])routeMap[key]={count:0,deltas:[]};
    routeMap[key].count++;
    if(a.sales&&a.sales['2024']&&a.sales['2025'])
      routeMap[key].deltas.push((a.sales['2025']-a.sales['2024'])/a.sales['2024']*100);
  });
  var routes=Object.entries(routeMap).sort((a,b)=>b[1].count-a[1].count);
  document.getElementById('tr-routes-tbody').innerHTML=routes.slice(0,8).map(function([k,d]){
    var avg=d.deltas.length?d.deltas.reduce((a,b)=>a+b,0)/d.deltas.length:null;
    return'<tr><td style="font-size:11px">'+k+'</td><td class="r">'+d.count+'</td><td class="r '+(avg!=null?posNeg(avg):'dim')+'">'+pct(avg)+'</td></tr>';
  }).join('');

  // Insights
  var insHtml='';
  if(avgChg!=null&&avgChg<-20)insHtml+='<div class="insight danger">Average sales decline of '+pct(avgChg)+' across transferred accounts suggests transfer disruption.</div>';
  var positiveRoutes=routes.filter(([k,d])=>{var a=d.deltas.reduce((x,y)=>x+y,0)/d.deltas.length;return a>0;});
  if(positiveRoutes.length)insHtml+='<div class="insight good">'+positiveRoutes.length+' transfer routes showing positive avg sales change.</div>';
  if(aAplus.length)insHtml+='<div class="insight warn">'+aAplus.length+' A+ accounts became inactive post-transfer. Immediate review required.</div>';
  document.getElementById('tr-insights').innerHTML=insHtml;
}

function clearTransferFilters(){
  document.getElementById('tr-f-type').value='inter';
  ['tr-f-oldteam','tr-f-newteam','tr-f-size'].forEach(id=>document.getElementById(id).value='');
  applyTransferFilters();
}

// ── Pipeline & Coverage ───────────────────────────────────────────────────
function renderPipelinePage(){applyPipelineFilters();}

function applyPipelineFilters(){
  var fTeam=document.getElementById('pl-f-team').value;
  var fSE=document.getElementById('pl-f-se').value;

  var seFiltered=SE_DATA.filter(function(s){
    if(fTeam&&s.team!==fTeam)return false;
    if(fSE&&s.se_name!==fSE)return false;
    return true;
  });

  var totalPipe=seFiltered.reduce((a,s)=>a+(s.hot_pipeline||0),0);
  var totalAward=seFiltered.reduce((a,s)=>a+(s.award||0),0);
  var totalBL=seFiltered.reduce((a,s)=>a+(s.backlog||0),0);
  var totalS25=seFiltered.reduce((a,s)=>a+(s.sales['2025']||0),0);
  var coverage=totalS25>0?totalPipe/totalS25:null;

  document.getElementById('pl-kpis').innerHTML=[
    {l:'Hot pipeline',v:sar(totalPipe),s:'50%+70%',sc:'kpi-sub'},
    {l:'Award',v:sar(totalAward),s:'90%+100%',sc:'kpi-sub'},
    {l:'Backlog',v:sar(totalBL),s:'current',sc:'kpi-sub'},
    {l:'Coverage ratio',v:coverage?coverage.toFixed(1)+'×':'—',s:'pipeline / 2025 sales',sc:'kpi-sub'},
  ].map(k=>'<div class="kpi"><div class="kpi-label">'+k.l+'</div><div class="kpi-val">'+k.v+'</div><div class="'+k.sc+'">'+k.s+'</div></div>').join('');

  // Team bars
  var teams=fTeam?TEAM_DATA.filter(t=>t.team===fTeam):TEAM_DATA;
  var maxP=Math.max(...teams.map(t=>t.hot_pipeline||0))||1;
  document.getElementById('pl-team-bars').innerHTML=teams.map(t=>
    '<div class="bar-wrap"><div class="bar-label"><span>'+t.team+'</span><span>'+sar(t.hot_pipeline)+'</span></div>'
    +'<div class="bar-track"><div class="bar-fill fill-blue" style="width:'+((t.hot_pipeline||0)/maxP*100)+'%"></div></div></div>'
  ).join('');

  // Alerts
  var alerts='';
  var noLowPipe=ACCOUNTS.filter(a=>a.size_tier&&['A+','A'].includes(a.size_tier)&&(!a.hot_pipeline||a.hot_pipeline===0));
  if(noLowPipe.length)alerts+='<div class="insight danger">'+noLowPipe.length+' A+/A accounts with zero hot pipeline — 2026 coverage risk</div>';
  seFiltered.forEach(function(s){
    var rat=s.sales['2025']>0?s.hot_pipeline/s.sales['2025']:0;
    if(rat<1.5&&rat>0)alerts+='<div class="insight warn">'+s.se_name+' — pipeline coverage '+rat.toFixed(1)+'× 2025 sales (below 1.5× threshold)</div>';
  });
  document.getElementById('pl-alerts').innerHTML=alerts||'<div class="insight good">No critical coverage gaps detected in current selection</div>';

  // SE table
  document.getElementById('pl-tbody').innerHTML=seFiltered.map(function(s){
    var cov=s.sales['2025']>0?((s.hot_pipeline||0)/s.sales['2025']).toFixed(1)+'×':'—';
    return'<tr><td>'+s.se_name+'</td><td style="font-size:10px;color:var(--text2)">'+s.team+'</td>'
      +'<td class="r">'+sar(s.sales['2025'])+'</td>'
      +'<td class="r">'+sar(s.hot_pipeline)+'</td>'
      +'<td class="r">'+sar(s.award)+'</td>'
      +'<td class="r">'+sar(s.backlog)+'</td>'
      +'<td class="r '+(parseFloat(cov)>=2?'pos':parseFloat(cov)<1.5?'neg':'')+'">'+cov+'</td>'
      +'</tr>';
  }).join('');
}

function clearPipelineFilters(){
  ['pl-f-team','pl-f-se','pl-f-group'].forEach(id=>document.getElementById(id).value='');
  applyPipelineFilters();
}

// ── Product Analysis ──────────────────────────────────────────────────────
function renderProductPage(){applyProductFilters();}

function applyProductFilters(){
  var fTeam=document.getElementById('prod-f-team').value;
  var fSE=document.getElementById('prod-f-se').value;

  var accs=ACCOUNTS.filter(function(a){
    if(fTeam&&a.curr_team!==fTeam)return false;
    if(fSE&&a.curr_se!==fSE)return false;
    return true;
  });

  // KPIs
  var g25=PROD_DATA.by_year&&PROD_DATA.by_year['2025']||{};
  var g24=PROD_DATA.by_year&&PROD_DATA.by_year['2024']||{};
  var top25=Object.entries(g25).sort((a,b)=>b[1]-a[1])[0];
  var fastest=Object.keys(g25).map(function(g){
    return{g:g,delta:g24[g]>0?(g25[g]-g24[g])/g24[g]*100:null};
  }).filter(x=>x.delta!=null).sort((a,b)=>b.delta-a.delta)[0];
  var slowest=Object.keys(g25).map(function(g){
    return{g:g,delta:g24[g]>0?(g25[g]-g24[g])/g24[g]*100:null};
  }).filter(x=>x.delta!=null).sort((a,b)=>a.delta-b.delta)[0];
  var shifted=accs.filter(a=>a.group_shifted).length;

  document.getElementById('prod-kpis').innerHTML=[
    {l:'Top group 2025',v:top25?top25[0]:'—',s:top25?sar(top25[1]):'—',sc:'kpi-sub'},
    {l:'Fastest growing',v:fastest?fastest.g:'—',s:fastest?pct(fastest.delta):'—',sc:'kpi-pos'},
    {l:'Fastest declining',v:slowest?slowest.g:'—',s:slowest?pct(slowest.delta):'—',sc:'kpi-neg'},
    {l:'Accounts shifted group',v:shifted,s:'2024→2025',sc:'kpi-sub'},
  ].map(k=>'<div class="kpi"><div class="kpi-label">'+k.l+'</div><div class="kpi-val">'+k.v+'</div><div class="'+k.sc+'">'+k.s+'</div></div>').join('');

  // Group bars
  var groups=[...new Set(Object.keys(g24).concat(Object.keys(g25)))];
  var g23=PROD_DATA.by_year&&PROD_DATA.by_year['2023']||{};
  var maxGV=Math.max(...groups.map(g=>Math.max(g23[g]||0,g24[g]||0,g25[g]||0)))||1;
  document.getElementById('prod-group-bars').innerHTML=groups.map(function(g){
    var v23=g23[g]||0,v24=g24[g]||0,v25=g25[g]||0;
    var delta=v24>0?(v25-v24)/v24*100:null;
    var fill25=delta==null?'fill-blue':delta>=0?'fill-green':'fill-red';
    return'<div class="bar-wrap"><div class="bar-label"><span>'+g+'</span>'
      +'<span style="font-size:10px">'+sar(v23)+' → '+sar(v24)+' → <b>'+sar(v25)+'</b>'
      +(delta!=null?' <span style="color:var('+(delta>=0?'--success':'--danger')+')">'+pct(delta)+'</span>':'')
      +'</span></div>'
      +'<div class="bar-track">'
      +'<div class="bar-fill fill-gray" style="width:'+(v23/maxGV*100)+'%"></div>'
      +'<div class="bar-fill fill-blue-light" style="width:'+(v24/maxGV*100)+'%"></div>'
      +'<div class="bar-fill '+fill25+'" style="width:'+(v25/maxGV*100)+'%"></div>'
      +'</div></div>';
  }).join('');

  // Shifts
  var shifts=PROD_DATA.shifts||{};
  document.getElementById('prod-shifts').innerHTML=Object.entries(shifts).slice(0,8).map(([k,v])=>{
    var parts=k.split(' → ');
    return'<div style="display:flex;align-items:center;gap:8px;font-size:11px;padding:6px 0;border-bottom:1px solid var(--border)">'
      +'<span style="color:var(--text2);min-width:100px">'+parts[0]+'</span>'
      +'<span style="color:var(--text3)">→</span>'
      +'<span style="font-weight:500;min-width:100px;color:var(--warn)">'+parts[1]+'</span>'
      +'<span style="margin-left:auto;color:var(--text3)">'+v+' accts</span>'
      +'</div>';
  }).join('');

  // Table
  document.getElementById('prod-tbody').innerHTML=accs.filter(a=>a.group_2024||a.group_2025).slice(0,200).map(function(a){
    return'<tr>'
      +'<td style="font-weight:500">'+a.name+'</td>'
      +'<td style="font-size:10px;color:var(--text2)">'+a.description+'</td>'
      +'<td style="font-size:10px;color:var(--text2)">'+a.curr_se+(a.curr_team?' · '+a.curr_team:'')+'</td>'
      +'<td style="font-size:11px;color:var(--text2)">'+(a.group_2024||'—')+'</td>'
      +'<td style="font-size:11px">'+(a.group_2024||'—')+'</td>'
      +'<td style="font-size:11px">'+(a.group_2025||'—')+'</td>'
      +'<td class="c">'+(a.group_shifted?'<span class="badge b-warn">Shifted</span>':'<span class="dim">—</span>')+'</td>'
      +'<td class="r">'+sar(a.sales&&a.sales['2025'])+'</td>'
      +'</tr>';
  }).join('');
}

function clearProductFilters(){
  ['prod-f-team','prod-f-se'].forEach(id=>document.getElementById(id).value='');
  applyProductFilters();
}

// ── Excel Exports ─────────────────────────────────────────────────────────
function tableToXLSX(tableId,filename){
  var tbl=document.getElementById(tableId);
  if(!tbl)return;
  var wb=XLSX.utils.book_new();
  var ws=XLSX.utils.table_to_sheet(tbl);
  XLSX.utils.book_append_sheet(wb,ws,'Data');
  XLSX.writeFile(wb,filename+'_'+new Date().toISOString().slice(0,10)+'.xlsx');
}

function exportAccTable(){
  // Export filtered data as array of objects
  var rows=ACC_FILTERED.map(function(a){
    return{
      'Customer No':a.customer,'Customer Name':a.name,'Description':a.description,
      'Current SE':a.curr_se,'Current Team':a.curr_team,'Old SE':a.old_se,'Old Team':a.old_team,
      'Transfer Type':a.transfer_type,'Rep Changed':a.rep_changed?'Yes':'No',
      'Size':a.size_tier+(a.size_trend==='up'?' ↑':a.size_trend==='down'?' ↓':''),
      'Status':a.cust_status,'Created On':a.created_on,
      '2022 Sales':a.sales&&a.sales['2022'],'2023 Sales':a.sales&&a.sales['2023'],
      '2024 Sales':a.sales&&a.sales['2024'],'2025 Sales':a.sales&&a.sales['2025'],'2026 YTD':a.sales&&a.sales['2026'],
      'YoY 25v24%':a.yoy_25v24,'Hot Pipeline':a.hot_pipeline,'Award':a.award,'Backlog':a.backlog,
      'Group 2024':a.group_2024,'Group 2025':a.group_2025,'Group Shifted':a.group_shifted?'Yes':'No',
    };
  });
  var wb=XLSX.utils.book_new();
  var ws=XLSX.utils.json_to_sheet(rows);
  XLSX.utils.book_append_sheet(wb,ws,'Accounts');
  XLSX.writeFile(wb,'asap_accounts_'+new Date().toISOString().slice(0,10)+'.xlsx');
}

function exportTeamTable(){tableToXLSX('se-team-table','asap_teams');}
function exportSETable(){tableToXLSX('se-se-table','asap_se');}
function exportSEAccTable(){tableToXLSX('se-acc-table','asap_se_accounts');}
function exportTransferTable(){tableToXLSX('tr-table','asap_transfers');}
function exportPipelineTable(){tableToXLSX('pl-table','asap_pipeline');}
function exportProductTable(){tableToXLSX('prod-table','asap_products');}

// ── Data loading ─────────────────────────────────────────────────────────
function loadAll(){
  var base='data/';
  Promise.all([
    fetch(base+'asap_accounts.json').then(r=>r.json()),
    fetch(base+'asap_se.json').then(r=>r.json()),
    fetch(base+'asap_teams.json').then(r=>r.json()),
    fetch(base+'asap_products.json').then(r=>r.json()),
  ]).then(function(results){
    ACCOUNTS=results[0];
    SE_DATA=results[1];
    TEAM_DATA=results[2];
    PROD_DATA=results[3];
    ACCOUNTS.forEach(function(a,i){ACC_INDEX[a.customer]=i;});
    initFilters();
    ACC_FILTERED=ACCOUNTS.slice();
    document.getElementById('acc-count').textContent=ACCOUNTS.length+' accounts shown';
    renderAccTable();
    renderHome();
    renderSEPage();
    renderTransferPage();
    renderPipelinePage();
    renderProductPage();
  }).catch(function(err){
    console.error('Data load error:',err);
    document.querySelector('.nav-updated').textContent='ERROR: '+err.message;
  });
}

loadAll();
</script>
</body>
</html>
"""
    # Inject embedded JSON - use JSON.parse with escaped string
    # This avoids backticks/special chars in names breaking inline JS
    def to_js_safe(data):
        raw = json.dumps(data, ensure_ascii=True, default=safe_val)
        # Escape backslashes and single quotes for JS string wrapping
        raw = raw.replace(chr(92), chr(92)+chr(92))  # \ 
        raw = raw.replace("'", chr(92)+"'")
        return "JSON.parse('" + raw + "')"

    os.makedirs('docs', exist_ok=True)
    path = os.path.join('docs', 'asap.html')
    with open(path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  Written: {path} ({len(html)//1024}KB)")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("ASAP Sales Dashboard — generate.py")
    print("=" * 60)

    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(DOCS_DIR, exist_ok=True)

    print("\n[1/7] Loading SE lookup...")
    se_lookup = load_se_lookup()
    print(f"  SE lookup: {len(se_lookup)} entries")

    print("\n[2/7] Building customer number bridge...")
    bridge = load_cust_bridge()

    print("\n[3/7] Loading vCust...")
    vc = load_vcust()

    print("\n[4/7] Loading sales data...")
    sales = load_sales(bridge)

    print("\n[5/7] Loading pipeline...")
    pipe = load_pipeline(bridge)

    print("\n[6/7] Assembling data...")
    accounts  = assemble_accounts(vc, sales, pipe)
    se_data   = assemble_se_data(accounts, sales)
    team_data = assemble_team_data(accounts, sales)
    prod_data = assemble_product_data(sales, accounts)

    print("\n[7/7] Writing outputs...")
    write_json(accounts,  'asap_accounts.json')
    write_json(se_data,   'asap_se.json')
    write_json(team_data, 'asap_teams.json')
    write_json(prod_data, 'asap_products.json')
    build_html()

    print("\nDeploying to GitHub Pages...")
    try:
        subprocess.run(['git', 'add', '-A', 'docs/'], check=True)
        subprocess.run(['git', 'commit', '-m', 'ASAP dashboard update'], check=True)
        subprocess.run(['git', 'push'], check=True)
        print("  Deployed successfully")
    except subprocess.CalledProcessError as e:
        print(f"  Git deploy skipped or failed: {e}")

    print("\nDone.")

if __name__ == '__main__':
    main()
