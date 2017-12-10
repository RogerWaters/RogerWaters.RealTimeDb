IF NOT EXISTS (SELECT * FROM sys.service_contracts WHERE name = '{contractName}')
BEGIN
	CREATE CONTRACT [{contractName}]
	(
		[{messageTypeName}] SENT BY ANY
	)
END