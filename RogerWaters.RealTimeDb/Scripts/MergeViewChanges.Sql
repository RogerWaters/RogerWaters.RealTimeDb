SET ANSI_NULLS OFF;
MERGE   
    INTO [{cacheName}] AS [{targetAlias}]
    USING [{viewName}] AS [{sourceAlias}]
    ON 
		{keyColumnsMatchCondition}
{whenMatchedClause}
    WHEN NOT MATCHED BY TARGET
        THEN 
			INSERT ({allTargetCollumns})
			VALUES ({allSourceColumns})
    WHEN NOT MATCHED BY SOURCE
        THEN DELETE
;  