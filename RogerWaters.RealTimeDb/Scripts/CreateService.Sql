IF NOT EXISTS (SELECT * FROM sys.services WHERE name = '{serviceName.Name}')
BEGIN
	CREATE SERVICE {serviceName}
	ON QUEUE {queueName} ({contractName})
END