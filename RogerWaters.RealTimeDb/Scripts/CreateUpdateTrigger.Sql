IF EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID('{triggerName}'))
BEGIN
	DROP TRIGGER {triggerName}
END
EXEC dbo.sp_executesql @statement = N'
	CREATE TRIGGER {triggerName}
	ON {tableName} {with}
	FOR UPDATE
	AS {as}
	BEGIN
		DECLARE @dialogHandle UNIQUEIDENTIFIER = ''{dialogHandle}''
		DECLARE @message NVARCHAR(MAX) = CAST
		(
			(
				SELECT
				(
					SELECT *
					FROM inserted AS row
					FOR XML AUTO, ELEMENTS XSINIL, ROOT(''UPDATED''), TYPE
				)
				FOR XML PATH(''{tableName.Schema}.{tableName.Name}'')
			) 
			AS NVARCHAR(MAX)
		);

		IF @message IS NULL
		BEGIN
			RETURN
		END;

		SEND ON CONVERSATION (@dialogHandle)
		MESSAGE TYPE {messageType}
		(@message)
	END'