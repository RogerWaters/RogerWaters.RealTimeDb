IF NOT EXISTS (SELECT * FROM sys.services WHERE name = '{serviceName}')
BEGIN
	CREATE SERVICE [{serviceName}]
	ON QUEUE [{queueName}] ([{contractName}])
END