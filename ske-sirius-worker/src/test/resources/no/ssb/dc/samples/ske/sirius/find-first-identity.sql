SELECT i.fid, i.identity, i.income_year, i.reg_date, i.event_type
FROM IDENTITY_MAP as i
WHERE i.fid IN (SELECT g.fid FROM IDENTITY_MAP g WHERE g.identity = ? AND g.income_year = ?)
ORDER BY i.reg_date ASC LIMIT 1
