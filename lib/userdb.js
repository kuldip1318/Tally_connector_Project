
const { Pool } = require('pg');


function getDbConfig(userId) {
  const dbName = `user_${userId}_db`;  
  return {
    host: "98.85.154.231",  
    port: 5432,             
    database: dbName,       
    user: "postgres",      
    password: "admin", 
  };
}

// Function to get the pool for a specific user
function getPoolForUser(userId) {
  const dbConfig = getDbConfig(userId);
  return new Pool(dbConfig); // Return a pool with the user-specific configuration
}

module.exports = { getPoolForUser };
