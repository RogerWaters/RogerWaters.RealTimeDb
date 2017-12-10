WAITFOR
(
	RECEIVE CAST(message_body AS NVARCHAR(MAX))
	FROM [{queueName}]
), TIMEOUT {timeoutMs}