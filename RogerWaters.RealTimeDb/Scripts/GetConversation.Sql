DECLARE @dialog_handle UNIQUEIDENTIFIER

SELECT TOP(1) @dialog_handle = ce.conversation_handle
FROM sys.conversation_endpoints ce 
JOIN
	sys.services s
ON
	ce.service_id = s.service_id
WHERE 
	ce.far_service = '{recieverServiceName.Name}' 
	AND 
	s.name = '{senderServiceName.Name}'

IF @dialog_handle IS NULL
BEGIN
	BEGIN DIALOG CONVERSATION @dialog_handle  
	FROM SERVICE {senderServiceName}
	TO SERVICE '{recieverServiceName.Name}' ON CONTRACT {contractName}
    WITH ENCRYPTION = OFF
END

SELECT @dialog_handle