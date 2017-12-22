WAITFOR
(
	RECEIVE TOP (1000) CAST(message_body AS NVARCHAR(MAX))
	FROM {queueName}
), TIMEOUT {timeoutMs}