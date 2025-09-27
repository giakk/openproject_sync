#!/usr/bin/env python3

vvv = [1,2,3,4,5,6,7,8,9]

for i in vvv:
    print(i)
    if i == 4:
        vvv.append(33)
    else:
        if i == 33:
            vvv.append(77)


# from config.configManager import ConfigManager
# from config.databaseConnector import DatabaseConnector


# config = ConfigManager()

# db = DatabaseConnector(config.cacheDB_config, config.gestionale_config)

# results = db.test_all_connections()

# print(results)

# print(db.initialize_cache_database())
    
    