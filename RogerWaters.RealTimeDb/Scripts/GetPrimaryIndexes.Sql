SELECT
	ix.name AS IndexName,
	CASE
		WHEN ix.is_unique_constraint = 1 OR ix.is_unique = 1 THEN 'UX' 
		WHEN ix.is_primary_key = 1 THEN 'PK'
		ELSE '' 
	END AS IndexType,
	c.name AS ColumnName
FROM 
	sys.tables t 
JOIN 
	sys.indexes ix 
ON 
	t.object_id = ix.object_id
JOIN
	sys.index_columns ic
ON
	ic.index_id = ix.index_id
	AND
	ic.object_id = ix.object_id
JOIN
	sys.columns c
ON
	ic.column_id = c.column_id
	AND
	c.object_id = ix.object_id
 WHERE
	ix.type > 0 
	AND
	(
		ix.is_primary_key = 1 
		OR
		ix.is_unique_constraint = 1 
		OR 
		IX.is_unique = 1
	)
	AND
	t.is_ms_shipped = 0 
	AND 
	t.name <> 'sysdiagrams'
	AND
	t.object_id = OBJECT_ID('{tableName}')