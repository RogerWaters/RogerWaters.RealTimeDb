# RogerWaters.RealTimeDb
Experimental prototype to test realtime capabilities of Sql-Server

Todos:
- allow query sync in memory without sql cache
- documentation
- support other mappers (propably use https://github.com/jacentino/TypesafeSQL)
- support scalar query
- detect and optimize for single-table query
-- where
-- sum
-- count
-- first
- refactor objects to prepare for better extensibility
-- simplify UserQuery
-- support lazy and prepared setup
-- allow reconnect to query
- performance tests and optimizations
- UnitTests