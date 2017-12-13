IF EXISTS (SELECT * FROM sys.triggers WHERE name = '{triggerName}')
BEGIN
	DROP TRIGGER [{triggerName}]
END
EXEC dbo.sp_executesql @statement = N'
	CREATE TRIGGER [{triggerName}]
	ON [{tableName}]
	FOR INSERT
	AS
	BEGIN
		DECLARE @dialogHandle UNIQUEIDENTIFIER = ''{dialogHandle}''
		DECLARE @message NVARCHAR(MAX) = CAST
		(
			(
				SELECT
				(
					SELECT *
					FROM inserted AS row
					FOR XML AUTO, ELEMENTS XSINIL, ROOT(''INSERTED''), TYPE
				)
				FOR XML PATH(''{tableName}'')
			) 
			AS NVARCHAR(MAX)
		);

		IF @message IS NULL
		BEGIN
			RETURN
		END;

		SEND ON CONVERSATION (@dialogHandle)
		MESSAGE TYPE [{messageType}]
		(@message)
	END'