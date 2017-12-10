IF EXISTS (SELECT * FROM sys.triggers WHERE name = '{triggerName}')
BEGIN
	DROP TRIGGER [{triggerName}]
END
EXEC dbo.sp_executesql @statement = N'
	CREATE TRIGGER [{triggerName}]
	ON [{tableName}]
	FOR DELETE
	AS
	BEGIN
		DECLARE @dialogHandle UNIQUEIDENTIFIER = ''{dialogHandle}''
		DECLARE @message NVARCHAR(MAX) = CAST
		(
			(
				SELECT *
				FROM deleted AS row
				FOR XML AUTO, ELEMENTS XSINIL, ROOT(''DELETED'')
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
