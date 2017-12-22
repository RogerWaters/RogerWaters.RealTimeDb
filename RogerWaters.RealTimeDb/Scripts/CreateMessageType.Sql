IF NOT EXISTS (SELECT * FROM sys.service_message_types WHERE name = '{messageTypeName.Name}')
BEGIN
	CREATE MESSAGE TYPE {messageTypeName}
	VALIDATION = NONE
END