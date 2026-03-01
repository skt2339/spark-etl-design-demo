-- Duplicate party_key
SELECT party_key, COUNT(*) 
FROM raw_customer
GROUP BY party_key
HAVING COUNT(*) > 1;

-- Raw vs Curated count comparison
SELECT 
  (SELECT COUNT(*) FROM raw_customer WHERE run_date = '2026-03-01') AS raw_count,
  (SELECT COUNT(*) FROM curated_customer WHERE run_date = '2026-03-01') AS curated_count;

-- Rejected records
SELECT * 
FROM rejected_customer
WHERE run_date = '2026-03-01';
