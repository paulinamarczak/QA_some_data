@echo off

set python_exe=C:\Python37\python.exe

%python_exe% .\qaqc_batch_fix.py "C:\Users\pmarczak\Documents\scripting_paulina\qa_dir" bc_fire_uncertainty_harvest_base_fire_ high_  1990 2070 "abs_AG_Biomass_C abs_All_to_Products abs_Merch_to_StemSnags abs_StemSnags_to_Products Age ha_Disturbance_Emissions_CH4 ha_Disturbance_Emissions_CO ha_Disturbance_Emissions_CO2 ha_NPP ha_Rh" "_base _miti" --draw_min 1 --draw_max 1 --n_cores 20

@rem below contains full list of arguments possible to implement
@rem %python_exe% .\qaqc_batch_fix.py "C:\Users\pmarczak\Documents\scripting_paulina\qa_dir" bc_fire_uncertainty_harvest_base_fire_ high_ 1990 2070 
@rem "abs_AG_Biomass_C abs_All_to_Products abs_Merch_to_StemSnags abs_StemSnags_to_Products Age ha_Disturbance_Emissions_CH4 ha_Disturbance_Emissions_CO ha_Disturbance_Emissions_CO2 ha_NPP ha_Rh"  "_base _miti" 
@rem --draw_min 1 --draw_max 4 --n_cores 19


pause
