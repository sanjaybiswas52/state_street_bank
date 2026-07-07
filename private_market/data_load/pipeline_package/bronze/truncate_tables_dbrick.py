import sys

sys.path.append('/Workspace/Shared/PrivateMarket_V3/Data_Loads/util/')
sys.path.append('/Workspace/Shared/PrivateMarket_V3/Data_Loads/Logging/')
sys.path.append('/Workspace/Shared/PrivateMarket_V3/Data_Loads/Factory/')

import logging
from CommonUtilityFunctionsFactory import CommonUtilityFunctionsFactory
from EnvironmentConfigLoader import EnvironmentConfigLoader
from MetadataConstants import MetadataConstants
from SilverConstants import SilverConstants

env = "dev"

# Build const_dict following the same pattern used across all Gold-PMO processes
bronze_defaults = MetadataConstants.as_dict()
silver_defaults = SilverConstants.as_dict()
tbl_defaults = {**bronze_defaults, **silver_defaults}
const_dict = EnvironmentConfigLoader.get_config(env, tbl_defaults)

# Fact tables to truncate.

TRUNC_TABLES = [
    f"{const_dict['pm_medallion_catalog_var']}.{const_dict['PM_BRONZE_SCHEMA_VAR']}.{const_dict['BRONZE_IOS_GL_ACTIVITY_INVESTMENT_TBL']}",
    f"{const_dict['pm_medallion_catalog_var']}.{const_dict['PM_BRONZE_SCHEMA_VAR']}.{const_dict['BRONZE_IOS_GL_ACTIVITY_INVESTOR_TBL']}",
    f"{const_dict['pm_medallion_catalog_var']}.{const_dict['PM_BRONZE_SCHEMA_VAR']}.{const_dict['BRONZE_IOS_GL_ACTIVITY_TBL']}",
    f"{const_dict['pm_medallion_catalog_var']}.{const_dict['PM_SILVER_SCHEMA_VAR']}.{const_dict['SILVER_IOS_GL_ACTIVITY_INVESTMENT_TBL']}",
    f"{const_dict['pm_medallion_catalog_var']}.{const_dict['PM_SILVER_SCHEMA_VAR']}.{const_dict['SILVER_IOS_GL_ACTIVITY_INVESTOR_TBL']}",
    f"{const_dict['pm_medallion_catalog_var']}.{const_dict['PM_SILVER_SCHEMA_VAR']}.{const_dict['SILVER_IOS_GL_ACTIVITY_TBL']}",
    f"{const_dict['pm_medallion_catalog_var']}.{const_dict['PM_SILVER_SCHEMA_VAR']}.{const_dict['REJECTED_AUDIT_TABLE']}"
]
