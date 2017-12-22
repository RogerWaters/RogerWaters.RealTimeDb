IF OBJECT_ID('{cacheName}') IS NOT NULL
BEGIN
	DROP TABLE {cacheName}
END

SELECT *
INTO {cacheName}
FROM {viewName}