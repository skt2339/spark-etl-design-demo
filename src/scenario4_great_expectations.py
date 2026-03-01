# Simplified GE-like expectations for clarity

def expectation_suite():
    suite = {
        "party_key_not_null": True,
        "party_key_unique": True,
        "country_allowed_set": ["SG", "IN", "US", "UK"],
        "valid_source_updated_at": True,
        "dob_before_today": True,
        "name_null_threshold": 0.1
    }

    return suite


# In pipeline:
# 1. Run expectations
# 2. If critical expectation fails -> stop job
# 3. Log validation result for audit
