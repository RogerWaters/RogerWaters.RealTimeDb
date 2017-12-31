IF OBJECT_ID('{cacheName}') IS NOT NULL
BEGIN
	DROP TABLE {cacheName}
END

DECLARE @columnNames NVARCHAR(MAX) = (
SELECT 
	'[' + name + '] ' + system_type_name + ' ' + (CASE WHEN is_nullable = 1 THEN 'NULL' ELSE 'NOT NULL' END) + ', '
FROM sys.dm_exec_describe_first_result_set
(N'SELECT * FROM {viewName}', null, 0)
FOR XML PATH(''))

SET @columnNames = 'CREATE TABLE {cacheName} (   
   '+@columnNames+'
   CONSTRAINT PK_{cacheName.Schema}_{cacheName.Name} PRIMARY KEY NONCLUSTERED ({primaryColumnList})  
 )   
 WITH (MEMORY_OPTIMIZED=ON, DURABILITY=SCHEMA_ONLY)'

 EXEC(@columnNames)
 
 INSERT INTO {cacheName}
 SELECT * FROM {viewName}