using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace Castanion.DataAccess
{
    public class SqlBulkData
    {
        /// <summary>
        /// Method to execute a Bulking data 
        /// </summary>
        /// <param name="table">Data table Structure</param>
        /// <param name="sqlConnection"> SqlConnection </param>
        /// <param name="tableName">Name of tha table to alter</param>
        /// <returns></returns>
        public static bool BulkCopy(DataTable table, SqlConnection sqlConnection, String tableName)
        {
            bool WasCompleted = false;
            try
            {
                using (SqlBulkCopy bulk = new SqlBulkCopy(sqlConnection))
                {
                    bulk.DestinationTableName = tableName;
                    bulk.WriteToServer(table);
                }
                WasCompleted = true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return WasCompleted;
        }
    }
}
