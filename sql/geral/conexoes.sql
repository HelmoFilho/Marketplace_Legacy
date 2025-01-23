SELECT 
    DB_NAME(dbid) as DBName, 
    COUNT(dbid) as NumberOfConnections,
    loginame as LoginName,
	sys.sysprocesses.hostname,
	sys.sysprocesses.program_name
FROM
    sys.sysprocesses
WHERE 
    dbid > 0
	AND hostname = 'DAGCLD125'
	AND program_name = 'Python'
GROUP BY 
    dbid, loginame, hostname, program_name