INSERT INTO {cacheName} ({allTargetCollumns})
OUTPUT inserted.*
SELECT [{sourceAlias}].*
FROM {viewName} [{sourceAlias}]
LEFT JOIN
	{cacheName} [{targetAlias}]
ON
	{keyColumnsMatchCondition}
WHERE
	[{targetAlias}].{firstNotNullPrimaryKeyColumn} IS NULL

UPDATE [{targetAlias}]
	SET {valuesUpdate}
OUTPUT inserted.*
FROM
	{cacheName} [{targetAlias}]
JOIN
	{viewName} [{sourceAlias}]
ON
	{keyColumnsMatchCondition}
WHERE
	{notMatchedValuesWithNullCheck}
	
DELETE [{targetAlias}]
OUTPUT deleted.*
FROM 
	{cacheName} [{targetAlias}]
LEFT JOIN
	{viewName} [{sourceAlias}]
ON
	{keyColumnsMatchCondition}
WHERE
	[{targetAlias}].{firstNotNullPrimaryKeyColumn} IS NULL