#!/usr/bin/env python3
"""
ExxonMobil Physics Carryover Simulator - SCADA Baseline.
Outputs physics carryover % per vessel/time for ML bias training.
Exact match to your standard config. Pure physics → SCADA comparison ready.
"""

import argparse
import json
import logging
import sys
import time
import numpy as np
import pandas as pd
from pathlib import Path
from typing import List, Dict
from pydantic import BaseModel, Field
from numba import njit, prange, float64
import json
import requests
from datetime import datetime
import time
import scadaglobals as sg
from typing import Dict, Any, List
from statistics import median

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

simglobal = None

import numpy as np

def compute_carryover_scaling_factor(
    vessel_ids: np.ndarray,
    carryover_raw: np.ndarray,        # shape (n_time, n_vessels)
    carryover_physics: np.ndarray,    # shape (n_time, n_vessels)
    F_liq: np.ndarray,                # shape (n_time, n_vessels), liquid flow
    design_F_liq: np.ndarray,         # shape (n_vessels,), design liquid flow
    tol: float = 0.20                 # ±20% of design flow for calibration
):
    """
    Auto‑compute scaling factor per vessel:
        carryover_scada_norm = carryover_raw * scaling_factor
    so that SCADA and physics live in the same unit at design‑like conditions.
    """
    n_time, n_vessels = carryover_raw.shape
    scaling_factor = np.ones(n_vessels)  # init to 1; will overwrite for each vessel

    design_flow_low  = design_F_liq * (1.0 - tol)
    design_flow_high = design_F_liq * (1.0 + tol)

    for vid in range(n_vessels):
        # Find time indices where this vessel is near design liquid flow
        in_tolerance = (F_liq[:, vid] >= design_flow_low[vid]) & \
                       (F_liq[:, vid] <= design_flow_high[vid])

        raw_vals  = carryover_raw[in_tolerance, vid]
        phys_vals = carryover_physics[in_tolerance, vid]

        # Skip if no data
        if len(raw_vals) == 0 or np.any(raw_vals == 0):
            scaling_factor[vid] = 1.0
            continue

        # Use median to avoid outliers
        raw_med  = np.median(raw_vals)
        phys_med = np.median(phys_vals)

        # Avoid division by zero
        if abs(raw_med) < 1e-12:
            scaling_factor[vid] = 1.0
        else:
            scaling_factor[vid] = phys_med / raw_med

    return scaling_factor
  
def evolve_to_snapshot(evolve_json: Dict[str, Any]) -> Dict[str, Any]:
    """Convert an 'evolve'‑style JSON to a 'snapshot'‑style JSON."""
    data = evolve_json["data"]

    # Compute average for each vessel
    averages: Dict[str, float] = {}
    for vessel, values in data.items():
        #avg = sum(values) / len(values)
        median_val = median(values)  # Handles sorting/odd-even automatically      
        averages[vessel] = median_val

    # Build vessels list in order of appearance (sorted by key, or just sorted names)
    vessel_names: List[str] = sorted(
        data.keys()
    )  # or list(data.keys()) if you want insertion order

    snapshot: Dict[str, Any] = {
        "mode": "snapshot",
        "vessels": [],
        "max_carryover": 0.0,
        "max_vessel": "",
    }

    max_carryover = 0.0
    max_vessel_name = ""

    for idx, name in enumerate(vessel_names, start=1):
        avg = averages.get(name, 0.0)
        snapshot["vessels"].append({
            "vesselIndex": idx,
            "name": name,
            "carryover_sim": avg
        })
        if avg > max_carryover:
            max_carryover = avg
            max_vessel_name = name

#    snapshot["max_carryover"] = max_carryover
 #   snapshot["max_vessel"] = max_vessel_name

    return snapshot


def enrich_scada_with_carryover(snapshot, scada_json):
    """Add carryover_sim + bias to each vessel's RawFields,
       and return a NEW scada_json containing only the updated TopicReads."""

    if not isinstance(snapshot, (dict, list)):
      return None

    # Build vessel index → carryover_sim map
    sim_map = {v['vesselIndex']: v['carryover_sim'] for v in snapshot['vessels']}
    
    # Find matching vessel data in SCADA
    stream = scada_json['Messages']['StreamTopicDetails']['TopicReads']
    
    # Filter and enrich only those messages that can be updated
    updated_messages = []
    
    for msg in stream:
        try:
            raw_fields = msg['data']['SeparatorMetrics']['RawFields']
            vessel_id = int(raw_fields['vesselIndex'])
            
            if vessel_id in sim_map:
                carryover_sim = sim_map[vessel_id]
                carryover_raw = raw_fields['carryover']

                # Add new fields
                raw_fields['carryover_scada'] = carryover_raw              
                raw_fields['carryover_sim'] = carryover_sim
                raw_fields['carryover_bias'] = carryover_raw - carryover_sim  # raw - sim
              
                
              #  print(f"V{vessel_id}: raw={carryover_raw:.2f}, sim={carryover_sim:.2e}, bias={raw_fields['carryover_bias']:.2e}")
                
                # Keep this message because it was updated
                updated_messages.append(msg)
                
        except (KeyError, ValueError, TypeError):
            # Skip messages that cannot be enriched
            continue
    
    # Return a new scada_json with only the enriched TopicReads
    result = dict(scada_json)  # shallow copy
    result['Messages']['StreamTopicDetails']['TopicReads'] = updated_messages
    return result
  
  
def build_vessel_physics_from_tags(vessel_tags):
    """Extract PRE-COMPUTED physics - NO MATH NEEDED"""
    try:
        raw_fields = vessel_tags['data']['SeparatorMetrics']['RawFields']
    except:
        return None
    
    norm_raw = {str(k).lower(): v for k, v in raw_fields.items()}
    vessel_index = int(norm_raw.get('vesselindex') or 0)
    if not vessel_index:
        return None
    
    return {
        "from_id": vessel_index,
        "to_id": None,
        "F_in": float(norm_raw.get('f_total', 0)),
        "f_gas": float(norm_raw.get('f_gas', 0)),
        "rho_l": float(norm_raw.get('rho_l', 900)),
        "rho_g": float(norm_raw.get('rhog', 1.0)),
        "mu_g": float(norm_raw.get('mug', 1e-5)),
        "raw_scada": raw_fields,
        # Bonus analytics already computed!
        "carryover": float(norm_raw.get('carryover', 0)),
        "stokes_number": float(norm_raw.get('stokes_number', 0)),
        "inversion_risk": float(norm_raw.get('inversion_risk', 0))
    }

def build_complete_topology(scada_json, base_topology):
    """
    Hybrid: live SCADA physics + base_topology defaults for missing vessels
    """
    messages = scada_json['Messages']['StreamTopicDetails']['TopicReads']
    vessel_physics = {}
    
    # Live data from SCADA
    for msg in messages:
        physics = build_vessel_physics_from_tags(msg)
        if physics:
            vessel_physics[physics['from_id']] = physics
    
    required_vessels = {vessel['id'] for vessel in base_topology['vessels']}
    missing = required_vessels - set(vessel_physics.keys())
    
    print(f"Live: {len(vessel_physics)} vessels, Missing: {missing}")
    
    # Create complete physics dict (live + base defaults)
    complete_physics = vessel_physics.copy()
    
    # Fill missing vessels from base_topology
    for vessel in base_topology['vessels']:
        vid = vessel['id']
        if vid not in complete_physics:
            # Extract physics from base_topology vessel/flow data
            complete_physics[vid] = {
                "rho_l": vessel.get("rho_l", 1000.0),  # Default water
                "rho_g": vessel.get("rho_g", 1.2),     # Default gas  
                "mu_g": vessel.get("mu_g", 1.8e-5),    # Default air
                "F_in": vessel.get("F_in", 0.0),
                "f_gas": vessel.get("f_gas", 0.5)
            }
    
    # Build updated topology with complete physics
    updated_vessels = []
    for vessel in base_topology['vessels']:
        vid = vessel['id']
        physics = complete_physics[vid]
        updated_vessel = vessel.copy()
        updated_vessel.update({
            "rho_l": physics["rho_l"],
            "rho_g": physics["rho_g"], 
            "mu_g": physics["mu_g"]
        })
        updated_vessels.append(updated_vessel)
    
    updated_flows = []
    for flow in base_topology['flows']:
        from_id = flow['from']
        physics = complete_physics[from_id]
        updated_flows.append({
            "from": from_id,
            "to": flow['to'],
            "F_in": physics["F_in"],
            "f_gas": physics["f_gas"]
        })
    
    return {
        **base_topology,
        "vessels": updated_vessels,
        "flows": updated_flows
    }
  

# Usage - works with ANY case:
#scada_tags = {
 #   "GasFlowRate": 392, "hclflowrate": 1, "Water_Flow_Rate": 8,
  #  "gasdensity": 1.0, "HCL_Density": 1002, "WATERDENSITY": 1006,
   # "Gas_Viscosity": 65.535
#}

#config = build_vessel_physics_from_tags(scada_tags, vessel_id=32)
#print(config)


@njit(fastmath=True)
def _warmup_physics_carryover_config(
    nv,      # number of vessels
    t_steps, # number of time steps
    dt,      # time step size
    g=9.81,
):
    # Dummy but realistic shapes for typical use
    y0 = np.ones(nv)                    # holdups
    f_gas_mat = np.zeros((nv, nv))      # gas‑flow matrix
    f_liq_mat = np.zeros((nv, nv))      # liquid‑flow matrix
    areas = np.ones(nv) * 10.0
    hts = np.ones(nv) * 10.0
    rhol = np.ones(nv) * 900.0
    rhog = np.ones(nv) * 80.0
    mug = np.ones(nv) * 1.8e-5
    ein = np.ones(nv) * 0.55            # inlet efficiency
    em = np.ones(nv) * 0.98             # mist‑pad efficiency
    gh = np.ones(nv) * 2.5              # gravity‑height
    vs_souders = np.ones(nv)            # vs_souders
    dmean = np.ones(nv) * 250.0
    dsig = np.ones(nv) * 1.5
    carryover_scale = np.ones(nv)

    # Trigger Numba JIT once for this shape
    physics_carryover(
        t_steps, dt, y0,
        f_gas_mat, f_liq_mat,
        areas, hts, rhol, rhog, mug,
        ein, em, gh, vs_souders,
        dmean, dsig, carryover_scale,
        g,
    )

class VesselConfig(BaseModel):
    id: int
    name: str
    type: str = Field("separator", description="auto-physics")
    V0: float
    
    # Auto-filled from type (override OK)
    diameter: float = Field(3.5)
    height: float = Field(12.0)
    rho_l: float = Field(900.0)
    rho_g: float = Field(80.0)
    mu_g: float = Field(1.8e-5)
    inlet_eff: float = Field(0.55)
    mist_eff: float = Field(0.98)
    souders_k: float = Field(0.35)
    gravity_h: float = Field(2.5)
    d_mean_um: float = Field(250.0)
    d_sigma_um: float = Field(1.5)
    carryover_scale: float = 0.1  # Add this line with a safe default

class FlowConfig(BaseModel):
    from_id: int = Field(alias="from")
    to_id: int = Field(alias="to")
    F_in: float
    f_gas: float = Field(0.7, description="Gas fraction")
    

class SolverConfig(BaseModel):
    t0: float = 0.0
    t_end: float = 24.0
    dt: float = 0.001


class Config(BaseModel):
    vessels: List[VesselConfig]
    flows: List[FlowConfig]
    solver: SolverConfig          # ← add this line
    physics: dict = {"g": 9.81}


def run_to_csv(df, f_gas_mat, f_liq_mat, config_container, filename='run_carryover.csv'):
    """
    FIX: Specifically extracts the .vessels list from the config container 
    to prevent 'Unknown' metadata rows and alignment errors.
    """
    
    # 1. Access the actual vessel list (handling different container types)
    vessels = getattr(config_container, 'vessels', config_container)
    if not isinstance(vessels, list):
        vessels = [vessels] # Fallback for single-vessel runs

    settings_rows = []
    for v in vessels:
        # Check for dictionary or object attributes
        def fetch(attr, default):
            if isinstance(v, dict): return v.get(attr, default)
            return getattr(v, attr, default)

        settings_rows.append({
            "Vessel_Name": str(fetch('name', 'N/A')),
            "Type": str(fetch('type', 'N/A')),
            "Diameter": fetch('diameter', 0.0),
            "Souders_K": fetch('souders_k', 0.0),
            "Mist_Eff": fetch('mist_eff', 0.0),
            "Scale": fetch('carryover_scale', 0.1)
        })
    
    settings_df = pd.DataFrame(settings_rows)

    # 2. Topology (Flow Rates)
    v_cols = df.columns.tolist()
    gas_routes = []
    # Safeguard against matrix/column size mismatch
    rows, cols = f_gas_mat.shape
    for i in range(min(len(v_cols), rows)):
        for j in range(min(len(v_cols), cols)):
            if f_gas_mat[i, j] > 0:
                gas_routes.append(f"{v_cols[i]}->{v_cols[j]} ({f_gas_mat[i,j]:.1f})")

    # 3. Write CSV with clean separators
    with open(filename, 'w') as f:
        f.write("# SECTION 1: PHYSICAL DESIGN PARAMETERS\n")
        settings_df.to_csv(f, index=False)
        
        f.write("\n# SECTION 2: PLANT TOPOLOGY\n")
        f.write(f"# Paths: {', '.join(gas_routes)}\n")
        
        f.write("\n# SECTION 3: SIMULATION TIME-SERIES\n")
        # Format for readability: 6 decimal places for carryover
        df.to_csv(f, index_label='Timestamp', float_format='%.6f')

    print(f"✅ Audit Trail Created: {filename}")
    return df
  
def snapshot_to_csv(snapshot, f_gas_mat, f_liq_mat, vessel_config_list, filename='vessel_audit_snapshot.csv'):
    try:
        vessel_names = snapshot.index

        print("vessel_config_list==",vessel_config_list)
      
        # 1. Topology Mapping
        downstream_gas, downstream_liq = [], []
        for i in range(len(vessel_names)):
            g_idx = np.where(f_gas_mat[i, :] > 0)[0]
            l_idx = np.where(f_liq_mat[i, :] > 0)[0]
            downstream_gas.append(vessel_names[g_idx[0]] if len(g_idx) > 0 else "FLARE/EXPORT")
            downstream_liq.append(vessel_names[l_idx[0]] if len(l_idx) > 0 else "STORAGE/DUMP")

        # 2. EXTRACT DATA - Ensuring unique values per vessel
        processed_configs = []
        
        # Determine if we have a list or the TML Config object
        vessels = vessel_config_list.vessels if hasattr(vessel_config_list, 'vessels') else vessel_config_list
            
        for v in vessels:
            # Convert object to dict to ensure we see all attributes
            if hasattr(v, '_asdict'): 
                d = v._asdict()
            elif hasattr(v, '__dict__'): 
                d = v.__dict__.copy()
            else: 
                d = v.copy() if isinstance(v, dict) else {}
            
            # CRITICAL: Pull the specific carryover_scale from the object
            # This avoids the "0.1" default if the attribute exists on the object
            if hasattr(v, 'carryover_scale'):
                d['carryover_scale'] = v.carryover_scale
                
            processed_configs.append(d)

        config_df = pd.DataFrame(processed_configs)

        # 3. Build Results
        results_df = pd.DataFrame({
            'vesselName': vessel_names,
            'carryover_sim': snapshot.values,
            'feeds_gas_to': downstream_gas,
            'feeds_liquid_to': downstream_liq
        })

        # 4. Perform the Merge
        # This ensures 'Separator 1A' gets the specific scale defined in your JSON
        right_key = 'name' if 'name' in config_df.columns else 'vesselName'
        final_df = pd.merge(results_df, config_df, left_on='vesselName', right_on=right_key, how='left')

        # 5. Clean up and Reorder
        if right_key != 'vesselName' and right_key in final_df.columns:
            final_df.drop(columns=[right_key], inplace=True)

        primary_cols = ['id', 'vesselName', 'type', 'carryover_sim', 'carryover_scale', 'feeds_gas_to', 'feeds_liquid_to']
        existing_primary = [c for c in primary_cols if c in final_df.columns]
        other_cols = [c for c in final_df.columns if c not in existing_primary]
        
        final_df = final_df[existing_primary + other_cols]

        # 6. Save
        final_df.to_csv(filename, index=False)
        return final_df

    except Exception as e:
        print(f"Error: {e}")
        return None      
      
      #def run_to_csv(df, fg, fl, filename='run_carryover.csv'):
 #   """DataFrame → Excel-ready CSV (time × vessels)"""
  #  df.to_csv(filename)
   # print(f"Saved: {filename}")
    #return df

#def snapshot_to_csv(snapshot, fg, fl, filename='snapshot_carryover.csv'):
 #   """Series → Excel-ready CSV"""
  #  snapshot_df = pd.DataFrame({
   #     'vesselIndex': range(1, len(snapshot)+1),
    #    'vesselName': snapshot.index,
     #   'carryover_sim': snapshot.values
    #})
    #snapshot_df.to_csv(filename, index=False)
    #print(f"Saved: {filename}")
    #return snapshot_df


# Type-based physics lookup
VESSEL_PHYSICS = {
    "separator": {"diameter": 3.5, "height": 12.0, "gravity_h": 2.5, "mist_eff": 0.98, "souders_k": 0.35, "d_mean_um": 220.0, "carryover_scale": 0.1},
    "fwko": {"diameter": 2.8, "height": 8.0, "gravity_h": 2.0, "mist_eff": 0.95, "souders_k": 0.32, "d_mean_um": 300.0, "carryover_scale": 0.1},
    "tank": {"diameter": 5.0, "height": 15.0, "gravity_h": 4.0, "mist_eff": 0.85, "souders_k": 0.25, "d_mean_um": 500.0, "carryover_scale": 0.1},   
    "scrubber": {"diameter": 2.2, "height": 7.0, "gravity_h": 1.8, "mist_eff": 0.99, "souders_k": 0.35, "d_mean_um": 150.0, "carryover_scale": 0.05},
    "ko_drum": {"diameter": 2.0, "height": 6.0, "gravity_h": 1.6, "mist_eff": 0.80, "souders_k": 0.28, "d_mean_um": 400.0, "carryover_scale": 0.15},
    "dehydrator": {"diameter": 3.0, "height": 10.0, "gravity_h": 2.2, "mist_eff": 0.99, "souders_k": 0.38, "d_mean_um": 100.0, "carryover_scale": 0.02}
}
#VESSEL_PHYSICS = {
 #   "separator": {"diameter":3.5, "height":12.0, "gravity_h":2.5, "mist_eff":0.98, "souders_k":0.35, "d_mean_um":220.0,"carryover_scale":0.1},
  #  "fwko": {"diameter":2.8, "height":8.0, "gravity_h":2.0, "mist_eff":0.95, "souders_k":0.32, "d_mean_um":300.0,"carryover_scale":0.1},
   # "tank": {"diameter":5.0, "height":15.0, "gravity_h":4.0, "mist_eff":0.85, "souders_k":0.25, "d_mean_um":500.0,"carryover_scale":0.1}
#}


def load_config_str(jsonstr: str) -> Config:
    """
    Production-Grade Loader: Zero hard-coding. 
    Dynamically maps vessel instances to the Global Physics Library.
    """
    data = json.loads(jsonstr)
    
    # 1. Access the vessel list from the JSON data
    vessels_data = data.get("vessels", [])
    
    for v in vessels_data:
        # 2. Dynamic Type Discovery: Handles 'type', 'Type', or 'vessel_type'
        # No hard-coded "separator" default here to force explicit config
        vtype_raw = v.get("type") or v.get("Type") or v.get("vessel_type")
        
        if not vtype_raw:
            print(f"CRITICAL: Vessel '{v.get('name')}' has no defined type. Physics may be incomplete.")
            continue
            
        vtype = vtype_raw.lower().strip()

        # 3. Dynamic Attribute Injection from VESSEL_PHYSICS
        if vtype in VESSEL_PHYSICS:
            v_profile = VESSEL_PHYSICS[vtype]
            for key, default_val in v_profile.items():
                # SETDEFAULT ensures we keep JSON overrides but fill missing holes
                # This is how 'HP Sep Train B' keeps its unique 0.64 mist_eff
                if key not in v:
                    v[key] = default_val
        else:
            print(f"WARNING: Type '{vtype}' not found in VESSEL_PHYSICS. Using raw JSON values only.")

    # 4. Dynamic Solver Calibration
    solver = data.get("solver", {})
    t_start = solver.get("t0", 0.0)
    t_end = solver.get("t_end", 1.0)
    n_steps = solver.get("n_steps", 1000)
    
    if n_steps > 0:
        solver["dt"] = (t_end - t_start) / n_steps

    return Config(**data)
  

def load_config(path: str) -> Config:
    """Standard config → physics-ready."""
    with open(path) as f:
        data = json.load(f)
    
    # Type physics injection
    for v in data["vessels"]:
        vtype = v.get("type", "separator")
        if vtype in VESSEL_PHYSICS:
            for key, val in VESSEL_PHYSICS[vtype].items():
                if key not in v:
                    v[key] = val
    
    # Solver dt calc
    solver = data["solver"]
    if "n_steps" in solver:
        if solver["n_steps"] !=0:
          solver["dt"] = (solver["t_end"] - solver["t0"]) / solver["n_steps"]
    
    return Config(**data)

# Hard‑coded 3‑point quadrature
QUAD_PTS = 3
ZVALS = (-1.2, 0.0, 1.2)
WEIGHTS = (0.25, 0.5, 0.25)


@njit(fastmath=True)
def physics_carryover(
    t_steps, dt, y0, 
    f_gas_mat, f_liq_mat,
    areas, hts, rhol, rhog, mug,
    ein, em_input, gh, vs_souders, dmean, dsig,
    carryover_scale, g
):
    """
    Tier-1 Industrial Physics Engine:
    Implements Dynamic Souders-Brown with Vessel-Specific Hardware Overrides.
    """
    nv = len(y0)
    carryover = np.zeros((t_steps, nv))
    v_state = np.zeros((t_steps, nv))
    v_state[0, :] = y0
    
    # Control Logic: Emulates standard LCV (Level Control Valve) PID response
    Kp, Ki = 0.45, 0.01
    integral_err = np.zeros(nv)
    setpoints = y0 * 1.02 
    
    # 3-Point Gaussian Quadrature for Droplet Size Distribution (Log-Normal)
    ZVALS = np.array([-1.2, 0.0, 1.2])
    WEIGHTS = np.array([0.25, 0.5, 0.25])

    for t in range(1, t_steps):
        # 1. Stochastic Process Noise (Industrial Stream Emulation)
        # Mimics the 0.5% high-frequency oscillation in gas headers
        turb = 1.0 + (np.random.standard_normal() * 0.005)

        fg_in = np.zeros(nv)
        fl_in = np.zeros(nv)
        for col in range(nv):
            for row in range(nv):
                fg_in[col] += f_gas_mat[row, col] * turb
                fl_in[col] += f_liq_mat[row, col]

        for i in range(nv):
            # --- NUMERICAL FORTRESS: Division/NaN Protection ---
            s_area = max(areas[i], 1e-6)
            s_ht = max(hts[i], 1e-3)
            s_rhog = max(rhog[i], 1e-6)
            s_rhol = max(rhol[i], 1.0)
            s_mug = max(mug[i], 1e-9)
            
            # --- DYNAMIC HEADSPACE DYNAMICS ---
            v_curr = v_state[t-1, i]
            level_f = min(max((v_curr / s_area) / s_ht, 0.0), 0.99)
            
            # Gas area contraction: As level rises, gas velocity (v_g) accelerates
            gas_mod = max((1.0 - level_f)**3.0, 1e-7)
            q_g_m3s = fg_in[i] / 3600.0
            v_g = q_g_m3s / (s_area * gas_mod)

            # --- SOUDERS-BROWN CRITICAL VELOCITY ---
            # Dynamically uses vessel-specific K-factors (vs_souders)
            v_s_base = max(abs(s_rhol - s_rhog) / s_rhog, 1e-9)
            v_s = vs_souders[i] * np.sqrt(v_s_base)
            
            # Loading Ratio: Key metric for Exxon/Chevron 'What-If' analysis
            # Ratio of actual gas velocity to design terminal velocity
            loading = max(min(v_g / max(v_s, 1e-6), 4.0), 0.01)
            
            # --- MULTI-STAGE DROPLET INTEGRATION ---
            # Uses specific dmean/dsig for each vessel type (e.g., Tank vs. Scrubber)
            lmu = np.log(max(dmean[i], 1e-1))
            lst = np.log(max(dsig[i], 1.01))
            
            cf_class_sum = 0.0
            for j in range(3):
                dm = np.exp(lmu + ZVALS[j] * lst) * 1e-6
                # Stokes Law for gravitational settling
                v_stokes = (s_rhol - s_rhog) * g * (dm**2) / (18.0 * s_mug)
                
                # Gravity Separation Efficiency
                stn = (v_stokes * gh[i]) / (v_g * s_ht + 1e-10)
                e_s = 1.0 - np.exp(-max(min(stn, 20.0), 0.0))
                
                # Mist Extractor Efficiency (Degrades at high loadings)
                # Correctly handles individual 'mist_eff' values (em_input)
                et = 1.0 - (1.0 - ein[i]) * (1.0 - e_s) * (em_input[i] / max(loading, 0.5))
                cf_class_sum += (1.0 - et) * WEIGHTS[j]

            # --- FINAL OUTPUT STABILIZATION ---
            # Applies the unique 'carryover_scale' to simulate specific maintenance bias
            cf_inst = max(cf_class_sum * carryover_scale[i], 1e-9)
            
            # Sensor Jitter: Emulates 1.5% field instrument noise
            flicker = 1.0 + (np.random.standard_normal() * 0.015)
            carryover[t, i] = cf_inst * flicker

            # --- DYNAMIC VALVE RESPONSE ---
            err = v_curr - setpoints[i]
            integral_err[i] = (integral_err[i] * 0.98) + (err * dt)
            
            q_in = fl_in[i] / s_rhol
            q_out = max(Kp * err + Ki * integral_err[i] + q_in, 0.0)
            
            # Liquid Volume Integration (Euler Method)
            v_state[t, i] = max(v_curr + (q_in - q_out - (cf_inst * q_in)) * dt, 0.0)

    return carryover
  
class PhysicsCarryover:
    def __init__(self, cfg, filename,topologyname,warm_up=True):
        self.filename = filename
        self.topologyname = topologyname
        self.cfg = cfg
        self.vmap = {v.id: i for i, v in enumerate(cfg.vessels)}
        nv = len(cfg.vessels)

        # reasonable defaults for warm‑up:
        t_steps = 2
        dt = 0.1

        if warm_up:
            import time
            logger.info(f"Warming up Numba physics_carryover for nv={nv}...")
            t0 = time.time()
            _warmup_physics_carryover_config(nv, t_steps, dt)
            logger.info(f"Numba warm‑up done in {time.time() - t0:.3f}s")
            
        fg = np.zeros((nv, nv))
        fl = np.zeros((nv, nv))
        for f in cfg.flows:
            from_idx = self.vmap[f.from_id]
            to_idx   = self.vmap[f.to_id]
            fg[from_idx, to_idx] = f.F_in * f.f_gas
            fl[from_idx, to_idx] = f.F_in * (1 - f.f_gas)

        self.fg_mat = fg
        self.fl_mat = fl

        print("cfg.vessels==",cfg.vessels)
      
        self.diams = np.array([v.diameter for v in cfg.vessels])
        self.hts   = np.array([v.height for v in cfg.vessels])
        self.rhol  = np.array([v.rho_l for v in cfg.vessels])
        self.rhog  = np.array([v.rho_g for v in cfg.vessels])
        self.mug   = np.array([v.mu_g for v in cfg.vessels])
        self.ein   = np.array([v.inlet_eff for v in cfg.vessels])
        self.em    = np.array([v.mist_eff for v in cfg.vessels])
        self.gh    = np.array([v.gravity_h for v in cfg.vessels])
        self.ks    = np.array([v.souders_k for v in cfg.vessels])
        self.dmean = np.array([v.d_mean_um for v in cfg.vessels])
        self.dsig  = np.array([v.d_sigma_um for v in cfg.vessels])
        self.y0    = np.array([v.V0 for v in cfg.vessels])
        self.carryover_scale = np.array([getattr(v, "carryover_scale", 1.0)
                                         for v in cfg.vessels])
        self.areas = np.pi * (self.diams / 2.0)**2
        self.vs_souders = self.ks * np.sqrt((self.rhol - self.rhog) / (self.rhog + 1e-10))

    def update_flows(self, cfg):  # Called every timestep!
        """Update fg/fl matrices with real SCADA flows"""
        self.vmap = {v.id: i for i, v in enumerate(cfg.vessels)}
        self.nv = len(cfg.vessels)

        fg = np.zeros((self.nv, self.nv))
        fl = np.zeros((self.nv, self.nv))
        for f in cfg.flows:
            from_idx = self.vmap[f.from_id]
            to_idx   = self.vmap[f.to_id]
            fg[from_idx, to_idx] = f.F_in * f.f_gas
            fl[from_idx, to_idx] = f.F_in * (1 - f.f_gas)

        self.fg_mat = fg
        self.fl_mat = fl
  
    def update_fluids(self, cfg):  # Called every timestep!
        self.vmap = {v.id: i for i, v in enumerate(cfg.vessels)}
        self.nv = len(cfg.vessels)
      
        self.rhol = np.zeros(self.nv)
        self.rhog = np.zeros(self.nv)
        self.mug  = np.zeros(self.nv)      

        self.rhol  = np.array([v.rho_l for v in cfg.vessels])
        self.rhog  = np.array([v.rho_g for v in cfg.vessels])
        self.mug   = np.array([v.mu_g for v in cfg.vessels])
        self.carryover_scale = np.array([getattr(v, "carryover_scale", 1.0)
                                         for v in cfg.vessels])

    def run(self, dt_coarse=0.1) -> pd.DataFrame:  # 0.1 s step by default
        """
        Run full transient for t0 → t_end at dt_coarse.
        Use for offline / design‑style physics, not SCADA‑cycle.
        """        
        solver = self.cfg.solver
        t_end = solver.t_end
        t0 = solver.t0
        dt = solver.dt
        steps = int((t_end - t0) / dt) + 1

        t_start = time.time()
        p1buf = f"**********  Starting Carryover TIMESERIES FOR {len(self.cfg.vessels)} vessels for {self.topologyname} Steps={steps} **********************"
        print(p1buf)      
        p2buf = f"Time Start: {t_start}"
        print(p2buf)
            
        co_matrix = physics_carryover(
            steps, dt, self.y0,
            self.fg_mat, self.fl_mat,
            self.areas, self.hts, self.rhol, self.rhog, self.mug,
            self.ein, self.em, self.gh, self.vs_souders,
            self.dmean, self.dsig, self.carryover_scale,
            self.cfg.physics["g"]
        )
        t_end = time.time()
        t_elapsed = t_end - t_start
        t_end = time.time()

        p3buf = f"Time Ended: {t_end}"
        print(p3buf)
        p4buf = f"ELAPSED TIME: {t_elapsed} seconds"
        print(p4buf)
        p5buf="**********  END Carryover TIMESERIES **********************" 
        print(p5buf)

        if self.filename != "":
          with open(f"{self.filename}_ExecutionTime.txt", "w") as f:
           f.write(p1buf + "\n")
           f.write(p2buf + "\n")
           f.write(p3buf + "\n")
           f.write(p4buf + "\n")
           f.write(p5buf + "\n")


        times = np.linspace(t0, t_end, steps)
        df = pd.DataFrame(
            co_matrix,
            index=times,
            columns=[v.name for v in self.cfg.vessels]
        )
        logger.info(f"Fast physics (t_end={t_end}, dt={dt:.3f}): {t_elapsed:.5f}s ({steps} steps)")
        return df

    def run_snapshot(self, dt=0.1) -> pd.Series:
        """
        Run 2‑step simulation at dt; return carryover % at last step as a Series.
        Meant for SCADA‑cycle calls (sub‑~10 ms after warm‑up).
        """
        print('In snapshot')
      
        steps = 2   # take at least one step

        t_start = time.time()
        p1buf = f"**********  Starting Carryover SNAPSHOT FOR {len(self.cfg.vessels)} vessels for {self.topologyname} Steps=1 **********************"
        print(p1buf)      
        p2buf = f"Time Start: {t_start}"
        print(p2buf)

        co_matrix = physics_carryover(
            steps, dt, self.y0,
            self.fg_mat, self.fl_mat,
            self.areas, self.hts, self.rhol, self.rhog, self.mug,
            self.ein, self.em, self.gh, self.vs_souders,
            self.dmean, self.dsig, self.carryover_scale,
            self.cfg.physics["g"]
        )
        t_end = time.time()
        t_elapsed = t_end - t_start
        p3buf = f"Time Ended: {t_end}"
        print(p3buf)
        p4buf = f"ELAPSED TIME: {t_elapsed} seconds"
        print(p4buf)
        p5buf="**********  END Carryover SNAPSHOT **********************" 
        print(p5buf)

        if self.filename != "":
          with open(f"{self.filename}_ExecutionTime.txt", "w") as f:
           f.write(p1buf + "\n")
           f.write(p2buf + "\n")
           f.write(p3buf + "\n")
           f.write(p4buf + "\n")
           f.write(p5buf + "\n")
  
        logger.info(f"Fast physics {t_elapsed:.5f}s ({steps} steps)")
        return pd.Series(
            co_matrix[-1],                      # last step
            index=[v.name for v in self.cfg.vessels]
        )

    def save_snapshot_csv(self, snapshot, filename='snapshot_carryover.csv'):
        if filename != "":
          snapshot_to_csv(snapshot,self.fg_mat, self.fl_mat, self.cfg, filename)
    
    def save_run_csv(self, snapshot, filename='run_carryover.csv'):
        if filename != "":      
          run_to_csv(snapshot, self.fg_mat, self.fl_mat, self.cfg, filename)        

def to_json(data):
    """Generic: Series → snapshot JSON, DataFrame → run JSON"""
    if isinstance(data, pd.Series):
        # run_snapshot output
        vessels = [{
            'vesselIndex': i+1,
            'name': idx,
            'carryover_sim': float(val)
        } for i, (idx, val) in enumerate(data.items())]
        return {
            'mode': 'snapshot',
            'vessels': vessels,
            'max_carryover': float(data.max()),
            'max_vessel': str(data.idxmax())
        }
    elif isinstance(data, pd.DataFrame):
        # run output  
        return {
            'mode': 'evolve',
            'times': data.index.astype(float).tolist(),
            'data': data.astype(float).to_dict('list')  # vessel → [t0,t1,t2,...]
        }
    else:
        raise ValueError("Input must be pd.Series (run_snapshot) or pd.DataFrame (run)")

def main():
    parser = argparse.ArgumentParser(description="Physics Carryover for SCADA Comparison")
    parser.add_argument("config", help="config.json")
    parser.add_argument("--output", "-o", default="physics_carryover.csv")
    parser.add_argument("--dt", type=float, default=0.1, help="time step for fast mode")
    args = parser.parse_args()

    cfg = load_config(args.config)

    sim = PhysicsCarryover(cfg)
    # Warm‑up (compilation)
 #   _ = sim.run_snapshot(dt=0.1)
  #  logger.info("Warm‑up done.")

    
    snapshot = sim.run_snapshot(0.1)
    print(snapshot)
    json_data = to_json(snapshot)
    #print(json.dumps(json_data, indent=2))

    snapshot = sim.run(0.1)
    print(snapshot)
    json_data = to_json(snapshot)
    #print(json.dumps(json_data, indent=2))

    sim.save_snapshot_csv('my_snapshot.csv')
    sim.save_run_csv('my_study.csv')

################### Run server
  #  run_server()

def run_server():
    # 1. Warm‑up once (already done at import)

    # 2. Keep a sim pool alive
    sim = None

    with open("config.json") as f:
         jsonstr=f.readlines()
         
    while True:
        try:
            cfg = load_config_str(jsonstr)
            if sim is None or id(sim.cfg) != id(cfg):
                sim = PhysicsCarryover(cfg)

            snapshot = sim.run_snapshot(dt=0.1)
            print(f"{time.time()}: {snapshot.tolist()}")
            time.sleep(0)
        except Exception as e:
            logger.error(e)
            time.sleep(5)

def runsim(jsonstr,filename,topologyname,dt=0.1,modestr="carryover_snapshot"):
       global simglobal

       print("jsonstr==",jsonstr)
  
       #print("modestr==",modestr)
       #print("dt==",dt)
       #print("filename==",filename)
  
       # pass the tml_physics_simulator section of payload
  #     try:
       cfg = load_config_str(jsonstr)
       print('MAIN CFG======',cfg)
     
       if simglobal is None or id(simglobal.cfg) != id(cfg):
            simglobal = PhysicsCarryover(cfg,filename,topologyname)
       else:
          simglobal.update_flows(cfg)
          simglobal.update_fluids(cfg)
         
       if modestr=="carryover_snapshot":
          snapshot = simglobal.run_snapshot(dt)
          simglobal.save_snapshot_csv(snapshot, filename)
       elif modestr=="carryover_timeseries":
          snapshot = simglobal.run(dt)
          simglobal.save_run_csv(snapshot,filename)
          
       json_data = to_json(snapshot)
  
       return json_data
            
#            print(f"{time.time()}: {snapshot.tolist()}")
#            time.sleep(0)
       #except Exception as e:
        #    print(f"Error: {e}")

#def modbus_read_loop(scada_cfg, interval_s, callback_url, max_reads, fields, scaling, start_address, sendtotopic, job_id, VIPERTOKEN, VIPERHOST, VIPERPORT,args,vessel_names,preprocessing,machinelearning, predictions, agenticai, ai,createvariables=""): 
def simloop(scadarawdatatopic,
            sendtotopic,
            physicspreprocessedtopic,
            rollbackoffsets,
            solver,
            topology, 
            localfoldername,
            topologyname,
            modestr,
            baseurl,
            callbackurl,                    
            timeinterval,
            savetodiskfrequency,
            VIPERTOKEN,
            VIPERHOST,
            VIPERPORT,
            stopbuf):

        global simglobal

        #modestr = sg.mainmode

        while True:          
          try:
            print("here===stop==", sg.topologystop,timeinterval)
            
            if stopbuf in sg.topologystop and stopbuf in sg.topologyrunning:
                print(f"{stopbuf} STOPPED ===========")
                sg.topologystop = [v for v in sg.topologystop if v != stopbuf]
                sg.topologyrunning = [v for v in sg.topologyrunning if v != stopbuf]              

                print("here===stop=inside=", sg.topologystop,timeinterval)

                break
            
            #print("MAIN MODEDS MODESTR ================", sg.mainmodes)
            #print("CURRENT MODE ================", sg.mainmode)
            
            endpoint = "/api/v1/consume"
            payload = f'{{"topic": "{scadarawdatatopic}", "rollbackoffsets": "{rollbackoffsets}"}}'
          
            response = requests.post(f"{baseurl}{endpoint}", json=json.loads(payload), timeout=5.0)

           # print('scadarawdata=', json.dumps(response.text))

            # step 2 get the preprocessed Scada data 
            # check snapshot or time series
    
            if response.text != "":
              updated_topology = build_complete_topology(json.loads(response.text), topology)
              #print("Updated topology=",json.dumps(updated_topology))
           
              if updated_topology:
                  mdt=solver.get("dt",0.1)
                  filename=""
                  if localfoldername != "":                      
                     #timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
                     if savetodiskfrequency == 0:
                       timestamp = datetime.now().strftime('%Y-%m-%d_%H')
                       filename=f"/rawdata/carryover/{localfoldername}/{topologyname}_{modestr}_{timestamp}.csv"
                     elif savetodiskfrequency == 1:  
                       timestamp = datetime.now().strftime('%Y-%m-%d_%H_%M_%S')
                       filename=f"/rawdata/carryover/{localfoldername}/{topologyname}_{modestr}_{timestamp}.csv"
                     else:
                       filename = ""
                    
                  jsonsim = runsim(json.dumps(updated_topology),filename,topologyname,mdt,modestr)
                
                  #print("json = sim=====", json.dumps(jsonsim))
                

                  if modestr== "carryover_snapshot":
                    scadajson=enrich_scada_with_carryover(jsonsim, json.loads(response.text))
                  else:
                    jsonsim=evolve_to_snapshot(jsonsim)
                    scadajson=enrich_scada_with_carryover(jsonsim, json.loads(response.text))

                  if not isinstance(scadajson, (dict, list)):
                      # time.sleep(timeinterval) 
                       continue

                    # write to preprocess topic 
                  if physicspreprocessedtopic != "": # This is the simulation data topic
                     endpoint = f"{baseurl}/api/v1/jsondataline"
                     messages = scadajson['Messages']['StreamTopicDetails']['TopicReads']
                     for msg in messages:                    
                        msg['sendtotopic'] = physicspreprocessedtopic
                        print("ENRICHED DATA msg=======",json.dumps(msg))
                        requests.post(endpoint.strip(), json=msg, timeout=5.0)
           
                  if sendtotopic != "": # This is the simulation data topic
                     endpoint = f"{baseurl}/api/v1/jsondataline"
                     jsonsim['sendtotopic'] = sendtotopic
                     requests.post(endpoint.strip(), json=jsonsim, timeout=5.0)
                    
                  if callbackurl != "":
                    carr = callbackurl.split(",")
                    for c in carr:
                      requests.post(c.strip(), json=jsonsim, timeout=5.0)
                  #print("✅ COMPLETE TOPOLOGY READY")
            time.sleep(timeinterval) 
          except Exception as e:
             print(f"Error: {e}")
             continue
        #sg.sim_thread.join(timeout=4.0)
        simglobal = None
        #modestr = ""
#        if sg.stop_sim.is_set():
 #          sg.stop_sim.clear()


#if __name__ == "__main__":
 #   main()
   # run_server()
