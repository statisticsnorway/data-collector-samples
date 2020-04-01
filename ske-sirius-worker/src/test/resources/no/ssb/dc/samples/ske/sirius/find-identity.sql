SELECT i.fid, i.identity, i.income_year, i.reg_date, i.event_type
FROM IDENTITY_HISTORY as i
WHERE i.identity = ? AND i.income_year = ?
ORDER BY i.reg_date DESC LIMIT 1
